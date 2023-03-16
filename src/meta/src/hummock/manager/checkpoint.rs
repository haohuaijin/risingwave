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

use std::sync::Arc;

use risingwave_common::system_param::reader::SystemParamsReader;
use risingwave_object_store::object::object_metrics::ObjectStoreMetrics;
use risingwave_object_store::object::{parse_remote_object_store, ObjectStoreImpl, ObjectStoreRef};
use risingwave_pb::hummock::HummockVersionCheckpoint;

use crate::hummock::error::{Error, Result};
use crate::hummock::HummockManager;
use crate::storage::MetaStore;

const CHECKPOINT_FILE_NAME: &str = "checkpoint";

impl<S> HummockManager<S>
where
    S: MetaStore,
{
    async fn read_checkpoint(&self) -> Result<HummockVersionCheckpoint> {
        use prost::Message;
        let data = self.object_store.read(&self.checkpoint_path, None).await?;
        let ckpt = HummockVersionCheckpoint::decode(data).map_err(|e| anyhow::anyhow!(e))?;
        Ok(ckpt)
    }

    async fn write_checkpoint(&self, checkpoint: HummockVersionCheckpoint) -> Result<()> {
        use prost::Message;
        let buf = checkpoint.encode_to_vec();
        self.object_store
            .upload(&self.checkpoint_path, buf.into())
            .await?;
        Ok(())
    }
}

pub(super) async fn object_store_client(
    system_params_reader: SystemParamsReader,
) -> ObjectStoreImpl {
    let url = match system_params_reader.state_store("".to_string()) {
        hummock if hummock.starts_with("hummock+") => {
            hummock.strip_prefix("hummock+").unwrap().to_string()
        }
        _ => "memory".to_string(),
    };
    parse_remote_object_store(
        &url,
        Arc::new(ObjectStoreMetrics::unused()),
        "Version Checkpoint",
    )
    .await
}

pub(super) fn checkpoint_path(system_params_reader: &SystemParamsReader) -> String {
    let dir = system_params_reader.data_directory().to_string();
    format!("{}/{}", dir, CHECKPOINT_FILE_NAME)
}
