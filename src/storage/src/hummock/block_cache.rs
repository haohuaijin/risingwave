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

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::ops::Deref;
use std::sync::Arc;

use futures::Future;
use risingwave_common::cache::{CacheableEntry, LookupResponse, LruCache, LruCacheEventListener};
use risingwave_hummock_sdk::HummockSstableObjectId;
use tokio::sync::oneshot::Receiver;
use tokio::task::JoinHandle;

use super::{Block, HummockResult, TieredCacheEntry};
use crate::hummock::HummockError;

const MIN_BUFFER_SIZE_PER_SHARD: usize = 32 * 1024 * 1024;

type CachedBlockEntry = CacheableEntry<(HummockSstableObjectId, u64), Box<Block>>;

enum BlockEntry {
    Cache(CachedBlockEntry),
    Owned(Box<Block>),
    RefEntry(Arc<Block>),
}

pub struct BlockHolder {
    _handle: BlockEntry,
    pub block: *const Block,
}

impl BlockHolder {
    pub fn from_ref_block(block: Arc<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::RefEntry(block),
            block: ptr,
        }
    }

    pub fn from_owned_block(block: Box<Block>) -> Self {
        let ptr = block.as_ref() as *const _;
        Self {
            _handle: BlockEntry::Owned(block),
            block: ptr,
        }
    }

    pub fn from_cached_block(entry: CachedBlockEntry) -> Self {
        let ptr = entry.value().as_ref() as *const _;
        Self {
            _handle: BlockEntry::Cache(entry),
            block: ptr,
        }
    }

    pub fn from_tiered_cache(
        entry: TieredCacheEntry<(HummockSstableObjectId, u64), Box<Block>>,
    ) -> Self {
        match entry {
            TieredCacheEntry::Cache(entry) => Self::from_cached_block(entry),
            TieredCacheEntry::Owned(block) => Self::from_owned_block(*block),
        }
    }
}

impl Deref for BlockHolder {
    type Target = Block;

    fn deref(&self) -> &Self::Target {
        unsafe { &(*self.block) }
    }
}

unsafe impl Send for BlockHolder {}
unsafe impl Sync for BlockHolder {}

type BlockCacheEventListener =
    Arc<dyn LruCacheEventListener<K = (HummockSstableObjectId, u64), T = Box<Block>>>;

#[derive(Clone)]
pub struct BlockCache {
    inner: Arc<LruCache<(HummockSstableObjectId, u64), Box<Block>>>,
}

pub enum BlockResponse {
    Block(BlockHolder),
    WaitPendingRequest(Receiver<CachedBlockEntry>),
    Miss(JoinHandle<Result<CachedBlockEntry, HummockError>>),
}

impl BlockResponse {
    pub async fn wait(self) -> HummockResult<BlockHolder> {
        match self {
            BlockResponse::Block(block_holder) => Ok(block_holder),
            BlockResponse::WaitPendingRequest(receiver) => receiver
                .await
                .map_err(|recv_error| recv_error.into())
                .map(BlockHolder::from_cached_block),
            BlockResponse::Miss(join_handle) => join_handle
                .await
                .unwrap()
                .map(BlockHolder::from_cached_block),
        }
    }
}

impl BlockCache {
    pub fn new(capacity: usize, max_shard_bits: usize) -> Self {
        Self::new_inner(capacity, max_shard_bits, None)
    }

    pub fn with_event_listener(
        capacity: usize,
        max_shard_bits: usize,
        listener: BlockCacheEventListener,
    ) -> Self {
        Self::new_inner(capacity, max_shard_bits, Some(listener))
    }

    fn new_inner(
        capacity: usize,
        mut max_shard_bits: usize,
        listener: Option<BlockCacheEventListener>,
    ) -> Self {
        if capacity == 0 {
            panic!("block cache capacity == 0");
        }
        while (capacity >> max_shard_bits) < MIN_BUFFER_SIZE_PER_SHARD && max_shard_bits > 0 {
            max_shard_bits -= 1;
        }

        let cache = match listener {
            Some(listener) => LruCache::with_event_listener(max_shard_bits, capacity, listener),
            None => LruCache::new(max_shard_bits, capacity),
        };

        Self {
            inner: Arc::new(cache),
        }
    }

    pub fn get(&self, object_id: HummockSstableObjectId, block_idx: u64) -> Option<BlockHolder> {
        self.inner
            .lookup(Self::hash(object_id, block_idx), &(object_id, block_idx))
            .map(BlockHolder::from_cached_block)
    }

    pub fn insert(
        &self,
        object_id: HummockSstableObjectId,
        block_idx: u64,
        block: Box<Block>,
    ) -> BlockHolder {
        BlockHolder::from_cached_block(self.inner.insert(
            (object_id, block_idx),
            Self::hash(object_id, block_idx),
            block.capacity(),
            block,
        ))
    }

    pub fn get_or_insert_with<F, Fut>(
        &self,
        object_id: HummockSstableObjectId,
        block_idx: u64,
        mut fetch_block: F,
    ) -> BlockResponse
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = HummockResult<Box<Block>>> + Send + 'static,
    {
        let h = Self::hash(object_id, block_idx);
        let key = (object_id, block_idx);
        match self
            .inner
            .lookup_with_request_dedup::<_, HummockError, _>(h, key, || {
                let f = fetch_block();
                async move {
                    let block = f.await?;
                    let len = block.capacity();
                    Ok((block, len))
                }
            }) {
            LookupResponse::Invalid => unreachable!(),
            LookupResponse::Cached(entry) => {
                BlockResponse::Block(BlockHolder::from_cached_block(entry))
            }
            LookupResponse::WaitPendingRequest(receiver) => {
                BlockResponse::WaitPendingRequest(receiver)
            }
            LookupResponse::Miss(join_handle) => BlockResponse::Miss(join_handle),
        }
    }

    fn hash(object_id: HummockSstableObjectId, block_idx: u64) -> u64 {
        let mut hasher = DefaultHasher::default();
        object_id.hash(&mut hasher);
        block_idx.hash(&mut hasher);
        hasher.finish()
    }

    pub fn size(&self) -> usize {
        self.inner.get_memory_usage()
    }

    #[cfg(any(test, feature = "test"))]
    pub fn clear(&self) {
        // This is only a method for test. Therefore it should be safe to call the unsafe method.
        self.inner.clear();
    }
}
