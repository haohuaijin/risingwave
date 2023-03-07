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

use std::ops::Deref;
use std::sync::Arc;

use risingwave_pb::meta::SystemParams;
use risingwave_rpc_client::{StreamClientPool, StreamClientPoolRef};

use super::{SystemParamsManager, SystemParamsManagerRef};
use crate::manager::{
    IdGeneratorManager, IdGeneratorManagerRef, IdleManager, IdleManagerRef, NotificationManager,
    NotificationManagerRef,
};
#[cfg(any(test, feature = "test"))]
use crate::storage::MemStore;
use crate::storage::MetaStore;
use crate::MetaResult;

/// [`MetaSrvEnv`] is the global environment in Meta service. The instance will be shared by all
/// kind of managers inside Meta.
#[derive(Clone)]
pub struct MetaSrvEnv<S>
where
    S: MetaStore,
{
    /// id generator manager.
    id_gen_manager: IdGeneratorManagerRef<S>,

    /// meta store.
    meta_store: Arc<S>,

    /// notification manager.
    notification_manager: NotificationManagerRef<S>,

    /// stream client pool memorization.
    stream_client_pool: StreamClientPoolRef,

    /// idle status manager.
    idle_manager: IdleManagerRef,

    /// system param manager.
    system_params_manager: SystemParamsManagerRef<S>,

    /// options read by all services
    pub opts: Arc<MetaOpts>,
}

/// Options shared by all meta service instances
#[derive(Clone)]
pub struct MetaOpts {
    /// Whether to enable the recovery of the cluster. If disabled, the meta service will exit on
    /// abnormal cases.
    pub enable_recovery: bool,
    /// The maximum number of barriers in-flight in the compute nodes.
    pub in_flight_barrier_nums: usize,
    /// After specified seconds of idle (no mview or flush), the process will be exited.
    /// 0 for infinite, process will never be exited due to long idle time.
    pub max_idle_ms: u64,
    /// Whether run in compaction detection test mode
    pub compaction_deterministic_test: bool,

    /// Interval of GC metadata in meta store and stale SSTs in object store.
    pub vacuum_interval_sec: u64,
    /// Threshold used by worker node to filter out new SSTs when scanning object store.
    pub min_sst_retention_time_sec: u64,
    /// The spin interval when collecting global GC watermark in hummock
    pub collect_gc_watermark_spin_interval_sec: u64,
    /// Enable sanity check when SSTs are committed
    pub enable_committed_sst_sanity_check: bool,
    /// Schedule compaction for all compaction groups with this interval.
    pub periodic_compaction_interval_sec: u64,
    /// Interval of reporting the number of nodes in the cluster.
    pub node_num_monitor_interval_sec: u64,

    /// The prometheus endpoint for dashboard service.
    pub prometheus_endpoint: Option<String>,

    /// The VPC id of the cluster.
    pub vpc_id: Option<String>,

    /// A usable security group id to assign to a vpc endpoint
    pub security_group_id: Option<String>,

    /// Endpoint of the connector node, there will be a sidecar connector node
    /// colocated with Meta node in the cloud environment
    pub connector_rpc_endpoint: Option<String>,

    /// Schedule space_reclaim_compaction for all compaction groups with this interval.
    pub periodic_space_reclaim_compaction_interval_sec: u64,

    /// Schedule ttl_reclaim_compaction for all compaction groups with this interval.
    pub periodic_ttl_reclaim_compaction_interval_sec: u64,
}

impl MetaOpts {
    /// Default opts for testing. Some tests need `enable_recovery=true`
    pub fn test(enable_recovery: bool) -> Self {
        Self {
            enable_recovery,
            in_flight_barrier_nums: 40,
            max_idle_ms: 0,
            compaction_deterministic_test: false,
            vacuum_interval_sec: 30,
            min_sst_retention_time_sec: 3600 * 24 * 7,
            collect_gc_watermark_spin_interval_sec: 5,
            enable_committed_sst_sanity_check: false,
            periodic_compaction_interval_sec: 60,
            node_num_monitor_interval_sec: 10,
            prometheus_endpoint: None,
            vpc_id: None,
            security_group_id: None,
            connector_rpc_endpoint: None,
            periodic_space_reclaim_compaction_interval_sec: 60,
            periodic_ttl_reclaim_compaction_interval_sec: 60,
        }
    }
}

impl<S> MetaSrvEnv<S>
where
    S: MetaStore,
{
    pub async fn new(
        opts: MetaOpts,
        init_system_params: SystemParams,
        meta_store: Arc<S>,
    ) -> MetaResult<Self> {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let stream_client_pool = Arc::new(StreamClientPool::default());
        let notification_manager = Arc::new(NotificationManager::new(meta_store.clone()).await);
        let idle_manager = Arc::new(IdleManager::new(opts.max_idle_ms));
        let system_params_manager = Arc::new(
            SystemParamsManager::new(
                meta_store.clone(),
                notification_manager.clone(),
                init_system_params,
            )
            .await?,
        );

        Ok(Self {
            id_gen_manager,
            meta_store,
            notification_manager,
            stream_client_pool,
            idle_manager,
            system_params_manager,
            opts: opts.into(),
        })
    }

    pub fn meta_store_ref(&self) -> Arc<S> {
        self.meta_store.clone()
    }

    pub fn meta_store(&self) -> &S {
        self.meta_store.deref()
    }

    pub fn id_gen_manager_ref(&self) -> IdGeneratorManagerRef<S> {
        self.id_gen_manager.clone()
    }

    pub fn id_gen_manager(&self) -> &IdGeneratorManager<S> {
        self.id_gen_manager.deref()
    }

    pub fn notification_manager_ref(&self) -> NotificationManagerRef<S> {
        self.notification_manager.clone()
    }

    pub fn notification_manager(&self) -> &NotificationManager<S> {
        self.notification_manager.deref()
    }

    pub fn idle_manager_ref(&self) -> IdleManagerRef {
        self.idle_manager.clone()
    }

    pub fn idle_manager(&self) -> &IdleManager {
        self.idle_manager.deref()
    }

    pub fn system_params_manager_ref(&self) -> SystemParamsManagerRef<S> {
        self.system_params_manager.clone()
    }

    pub fn system_params_manager(&self) -> &SystemParamsManager<S> {
        self.system_params_manager.deref()
    }

    pub fn stream_client_pool_ref(&self) -> StreamClientPoolRef {
        self.stream_client_pool.clone()
    }

    pub fn stream_client_pool(&self) -> &StreamClientPool {
        self.stream_client_pool.deref()
    }
}

#[cfg(any(test, feature = "test"))]
impl MetaSrvEnv<MemStore> {
    // Instance for test.
    pub async fn for_test() -> Self {
        Self::for_test_opts(MetaOpts::test(false).into()).await
    }

    pub async fn for_test_opts(opts: Arc<MetaOpts>) -> Self {
        // change to sync after refactor `IdGeneratorManager::new` sync.
        let meta_store = Arc::new(MemStore::default());
        let id_gen_manager = Arc::new(IdGeneratorManager::new(meta_store.clone()).await);
        let notification_manager = Arc::new(NotificationManager::new(meta_store.clone()).await);
        let stream_client_pool = Arc::new(StreamClientPool::default());
        let idle_manager = Arc::new(IdleManager::disabled());
        let system_params_manager = Arc::new(
            SystemParamsManager::new(
                meta_store.clone(),
                notification_manager.clone(),
                risingwave_common::system_param::default_system_params(),
            )
            .await
            .unwrap(),
        );

        Self {
            id_gen_manager,
            meta_store,
            notification_manager,
            stream_client_pool,
            idle_manager,
            system_params_manager,
            opts,
        }
    }
}
