// Copyright 2023 RisingWave Labs
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::sync::atomic::AtomicU64;
use std::sync::Arc;
use std::time::Duration;

use risingwave_batch::task::BatchManager;
use risingwave_common::util::epoch::Epoch;
use risingwave_stream::executor::monitor::StreamingMetrics;
use risingwave_stream::task::LocalStreamManager;

use super::MemoryControlPolicy;
use crate::memory_management::MemoryControlStats;

/// The minimal memory requirement of computing tasks in megabytes.
pub const MIN_COMPUTE_MEMORY_MB: usize = 512;
/// The memory reserved for system usage (stack and code segment of processes, allocation overhead,
/// network buffer, etc.) in megabytes.
pub const SYSTEM_RESERVED_MEMORY_MB: usize = 512;

/// When `enable_managed_cache` is set, compute node will launch a [`GlobalMemoryManager`] to limit
/// the memory usage.
pub struct GlobalMemoryManager {
    /// All cached data before the watermark should be evicted.
    watermark_epoch: Arc<AtomicU64>,
    /// Total memory that can be allocated by the compute node for computing tasks (stream & batch)
    /// in bytes.
    total_compute_memory_bytes: usize,
    /// Barrier interval.
    barrier_interval_ms: u32,
    metrics: Arc<StreamingMetrics>,
    /// The memory control policy for computing tasks.
    memory_control_policy: MemoryControlPolicy,
    wkx_max_memory_manager_step: usize,
}

pub type GlobalMemoryManagerRef = Arc<GlobalMemoryManager>;

impl GlobalMemoryManager {
    pub fn new(
        total_compute_memory_bytes: usize,
        barrier_interval_ms: u32,
        metrics: Arc<StreamingMetrics>,
        memory_control_policy: MemoryControlPolicy,
        wkx_operator_cache_capacity_mb: usize,
        wkx_max_memory_manager_step: usize,
    ) -> Arc<Self> {
        // Arbitrarily set a minimal barrier interval in case it is too small,
        // especially when it's 0.
        let barrier_interval_ms = std::cmp::max(barrier_interval_ms, 10);

        tracing::debug!(
            "memory control policy: {}",
            memory_control_policy.describe(total_compute_memory_bytes)
        );

        let hack_total_compute_memory_bytes = wkx_operator_cache_capacity_mb << 20;
        tracing::info!(
            "WKXLOG hack_total_compute_memory_bytes/wkx_operator_cache_capacity_mb: {}, total_compute_memory_bytes: {}",
            memory_control_policy.describe(hack_total_compute_memory_bytes),
            memory_control_policy.describe(total_compute_memory_bytes)
        );

        Arc::new(Self {
            watermark_epoch: Arc::new(0.into()),
            total_compute_memory_bytes: hack_total_compute_memory_bytes,
            barrier_interval_ms,
            metrics,
            memory_control_policy,
            wkx_max_memory_manager_step,
        })
    }

    pub fn get_watermark_epoch(&self) -> Arc<AtomicU64> {
        self.watermark_epoch.clone()
    }

    /// Memory manager will get memory usage statistics from batch and streaming and perform memory
    /// control accordingly.
    pub async fn run(
        self: Arc<Self>,
        batch_manager: Arc<BatchManager>,
        stream_manager: Arc<LocalStreamManager>,
    ) {
        let mut tick_interval =
            tokio::time::interval(Duration::from_millis(self.barrier_interval_ms as u64));
        let mut memory_control_stats = MemoryControlStats {
            batch_memory_usage: 0,
            streaming_memory_usage: 0,
            jemalloc_allocated_mib: 0,
            lru_watermark_step: 0,
            lru_watermark_time_ms: Epoch::physical_now(),
            lru_physical_now_ms: Epoch::physical_now(),
        };

        loop {
            // Wait for a while to check if need eviction.
            tick_interval.tick().await;

            memory_control_stats = self.memory_control_policy.apply(
                self.total_compute_memory_bytes,
                self.barrier_interval_ms,
                memory_control_stats,
                batch_manager.clone(),
                stream_manager.clone(),
                self.watermark_epoch.clone(),
                self.wkx_max_memory_manager_step,
            );

            // tracing::info!(
            //     "WKXLOG jemalloc_allocated_mib: {}, streaming_memory_usage: {},
            // lru_watermark_step: {}",     memory_control_stats.jemalloc_allocated_mib,
            //     memory_control_stats.streaming_memory_usage,
            //     memory_control_stats.lru_watermark_step
            // );

            self.metrics
                .lru_current_watermark_time_ms
                .set(memory_control_stats.lru_watermark_time_ms as i64);
            self.metrics
                .lru_physical_now_ms
                .set(memory_control_stats.lru_physical_now_ms as i64);
            self.metrics
                .lru_watermark_step
                .set(memory_control_stats.lru_watermark_step as i64);
            self.metrics.lru_runtime_loop_count.inc();
            self.metrics
                .jemalloc_allocated_bytes
                .set(memory_control_stats.jemalloc_allocated_mib as i64);
            self.metrics
                .stream_total_mem_usage
                .set(memory_control_stats.streaming_memory_usage as i64);
            self.metrics
                .batch_total_mem_usage
                .set(memory_control_stats.batch_memory_usage as i64);
        }
    }
}
