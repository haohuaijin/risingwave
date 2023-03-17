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

use std::collections::HashMap;

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_pb::common::PbColumnOrder;
use risingwave_pb::plan_common::StorageTableDesc;

use super::{ColumnDesc, ColumnId, TableId};
use crate::util::sort_util::ColumnOrder;

/// Includes necessary information for compute node to access data of the table.
///
/// It's a subset of `TableCatalog` in frontend. Refer to `TableCatalog` for more details.
#[derive(Debug, Clone, Default, PartialEq, Eq, Hash)]
pub struct TableDesc {
    /// Id of the table, to find in storage.
    pub table_id: TableId,
    /// The key used to sort in storage.
    pub pk: Vec<ColumnOrder>,
    /// All columns in the table, noticed it is NOT sorted by columnId in the vec.
    pub columns: Vec<ColumnDesc>,

    pub dist_key_in_pk_indices: Vec<usize>,
    /// Column indices for primary keys.
    pub stream_key: Vec<usize>,

    /// Whether the table source is append-only
    pub append_only: bool,

    pub retention_seconds: u32,

    pub value_indices: Vec<usize>,

    /// The prefix len of pk, used in bloom filter.
    pub read_prefix_len_hint: usize,

    /// the column indices which could receive watermarks.
    pub watermark_columns: FixedBitSet,

    /// Whether the table is versioned. If `true`, column-aware row encoding will be used
    /// to be compatible with schema changes.
    ///
    /// See `version` field in `TableCatalog` for more details.
    pub versioned: bool,
}

impl TableDesc {
    pub fn arrange_key_orders_protobuf(&self) -> Vec<PbColumnOrder> {
        // Set materialize key as arrange key + pk
        self.pk.iter().map(|x| x.to_protobuf()).collect()
    }

    pub fn order_column_indices(&self) -> Vec<usize> {
        self.pk.iter().map(|col| (col.column_index)).collect()
    }

    pub fn order_column_ids(&self) -> Vec<ColumnId> {
        self.pk
            .iter()
            .map(|col| self.columns[col.column_index].column_id)
            .collect()
    }

    pub fn to_protobuf(&self) -> StorageTableDesc {
        let dist_key_in_pk_indices: Vec<u32> = self.dist_key_in_pk_indices.iter().map(|&k| k as u32).collect();
        // let pk_indices: Vec<u32> = self
        //     .pk
        //     .iter()
        //     .map(|v| v.to_protobuf().column_index)
        //     .collect();
        // let dist_key_in_pk_indices = dist_key_indices
        //     .iter()
        //     .map(|&di| {
        //         pk_indices
        //             .iter()
        //             .position(|&pi| di == pi)
        //             .unwrap_or_else(|| {
        //                 panic!(
        //                     "distribution key {:?} must be a subset of primary key {:?}",
        //                     dist_key_indices, pk_indices
        //                 )
        //             })
        //     })
        //     .map(|d| d as u32)
        //     .collect_vec();
        StorageTableDesc {
            table_id: self.table_id.into(),
            columns: self.columns.iter().map(Into::into).collect(),
            pk: self.pk.iter().map(|v| v.to_protobuf()).collect(),
            dist_key_in_pk_indices,
            retention_seconds: self.retention_seconds,
            value_indices: self.value_indices.iter().map(|&v| v as u32).collect(),
            read_prefix_len_hint: self.read_prefix_len_hint as u32,
            versioned: self.versioned,
        }
    }

    /// Helper function to create a mapping from `column id` to `column index`
    pub fn get_id_to_op_idx_mapping(&self) -> HashMap<ColumnId, usize> {
        let mut id_to_idx = HashMap::new();
        self.columns.iter().enumerate().for_each(|(idx, c)| {
            id_to_idx.insert(c.column_id, idx);
        });
        id_to_idx
    }
}
