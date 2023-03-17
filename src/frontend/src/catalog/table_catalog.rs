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

use std::collections::{HashMap, HashSet};

use fixedbitset::FixedBitSet;
use itertools::Itertools;
use risingwave_common::catalog::{ColumnCatalog, TableDesc, TableId, TableVersionId};
use risingwave_common::constants::hummock::TABLE_OPTION_DUMMY_RETENTION_SECOND;
use risingwave_common::error::{ErrorCode, RwError};
use risingwave_common::util::sort_util::ColumnOrder;
use risingwave_pb::catalog::table::{
    OptionalAssociatedSourceId, TableType as ProstTableType, TableVersion as ProstTableVersion,
};
use risingwave_pb::catalog::Table as ProstTable;

use super::{ColumnId, ConflictBehaviorType, DatabaseId, FragmentId, RelationCatalog, SchemaId};
use crate::user::UserId;
use crate::WithOptions;

/// Includes full information about a table.
///
/// Currently, it can be either:
/// - a table or a source
/// - a materialized view
/// - an index
///
/// Use `self.table_type()` to determine the type of the table.
///
/// # Column ID & Column Index
///
/// [`ColumnId`](risingwave_common::catalog::ColumnId) (with type `i32`) is the unique identifier of
/// a column in a table. It is used to access storage.
///
/// Column index, or idx, (with type `usize`) is the relative position inside the `Vec` of columns.
///
/// A tip to avoid making mistakes is never do casting - i32 as usize or vice versa.
///
/// # Keys
///
/// All the keys are represented as column indices.
///
/// - **Primary Key** (pk): unique identifier of a row.
///
/// - **Order Key**: the primary key for storage, used to sort and access data.
///
///   For an MV, the columns in `ORDER BY` clause will be put at the beginning of the order key. And
/// the remaining columns in pk will follow behind.
///
///   If there's no `ORDER BY` clause, the order key will be the same as pk.
///
/// - **Distribution Key**: the columns used to partition the data. It must be a subset of the order
///   key.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
#[cfg_attr(test, derive(Default))]
pub struct TableCatalog {
    pub id: TableId,

    pub associated_source_id: Option<TableId>, // TODO: use SourceId

    pub name: String,

    /// All columns in this table.
    pub columns: Vec<ColumnCatalog>,

    /// Key used as materialize's storage key prefix, including MV order columns and stream_key.
    pub pk: Vec<ColumnOrder>,

    /// pk_indices of the corresponding materialize operator's output.
    pub stream_key: Vec<usize>,

    /// Type of the table. Used to distinguish user-created tables, materialized views, index
    /// tables, and internal tables.
    pub table_type: TableType,

    /// The append-only attribute is derived from `StreamMaterialize` and `StreamTableScan` relies
    /// on this to derive an append-only stream plan.
    pub append_only: bool,

    /// Owner of the table.
    pub owner: UserId,

    /// Properties of the table. For example, `appendonly` or `retention_seconds`.
    pub properties: WithOptions,

    /// The fragment id of the `Materialize` operator for this table.
    pub fragment_id: FragmentId,

    /// An optional column index which is the vnode of each row computed by the table's consistent
    /// hash distribution.
    pub vnode_col_index: Option<usize>,

    /// An optional column index of row id. If the primary key is specified by users, this will be
    /// `None`.
    pub row_id_index: Option<usize>,

    /// The column indices which are stored in the state store's value with row-encoding. Currently
    /// is not supported yet and expected to be `[0..columns.len()]`.
    pub value_indices: Vec<usize>,

    /// The full `CREATE TABLE` or `CREATE MATERIALIZED VIEW` definition of the table.
    pub definition: String,

    pub conflict_behavior_type: ConflictBehaviorType,

    pub read_prefix_len_hint: usize,

    /// Per-table catalog version, used by schema change. `None` for internal tables and tests.
    pub version: Option<TableVersion>,

    /// The column indices which could receive watermarks.
    pub watermark_columns: FixedBitSet,

    /// Optional field specifies the distribution key indices in pk.
    /// See https://github.com/risingwavelabs/risingwave/issues/8377 for more information.
    pub dist_key_in_pk: Vec<usize>,
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Hash)]
pub enum TableType {
    /// Tables created by `CREATE TABLE`.
    Table,
    /// Tables created by `CREATE MATERIALIZED VIEW`.
    MaterializedView,
    /// Tables serving as index for `TableType::Table` or `TableType::MaterializedView`.
    Index,
    /// Internal tables for executors.
    Internal,
}

#[cfg(test)]
impl Default for TableType {
    fn default() -> Self {
        Self::Table
    }
}

impl TableType {
    fn from_prost(prost: ProstTableType) -> Self {
        match prost {
            ProstTableType::Table => Self::Table,
            ProstTableType::MaterializedView => Self::MaterializedView,
            ProstTableType::Index => Self::Index,
            ProstTableType::Internal => Self::Internal,
            ProstTableType::Unspecified => unreachable!(),
        }
    }

    fn to_prost(self) -> ProstTableType {
        match self {
            Self::Table => ProstTableType::Table,
            Self::MaterializedView => ProstTableType::MaterializedView,
            Self::Index => ProstTableType::Index,
            Self::Internal => ProstTableType::Internal,
        }
    }
}

/// The version of a table, used by schema change. See [`ProstTableVersion`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct TableVersion {
    pub version_id: TableVersionId,
    pub next_column_id: ColumnId,
}

impl TableVersion {
    /// Create an initial version for a table, with the given max column id.
    #[cfg(test)]
    pub fn new_initial_for_test(max_column_id: ColumnId) -> Self {
        use risingwave_common::catalog::INITIAL_TABLE_VERSION_ID;

        Self {
            version_id: INITIAL_TABLE_VERSION_ID,
            next_column_id: max_column_id.next(),
        }
    }

    pub fn from_prost(prost: ProstTableVersion) -> Self {
        Self {
            version_id: prost.version,
            next_column_id: ColumnId::from(prost.next_column_id),
        }
    }

    pub fn to_prost(&self) -> ProstTableVersion {
        ProstTableVersion {
            version: self.version_id,
            next_column_id: self.next_column_id.into(),
        }
    }
}

impl TableCatalog {
    /// Get a reference to the table catalog's table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    pub fn with_id(mut self, id: TableId) -> Self {
        self.id = id;
        self
    }

    pub fn conflict_behavior_type(&self) -> ConflictBehaviorType {
        self.conflict_behavior_type
    }

    pub fn table_type(&self) -> TableType {
        self.table_type
    }

    pub fn is_table(&self) -> bool {
        self.table_type == TableType::Table
    }

    pub fn is_internal_table(&self) -> bool {
        self.table_type == TableType::Internal
    }

    pub fn is_mview(&self) -> bool {
        self.table_type == TableType::MaterializedView
    }

    pub fn is_index(&self) -> bool {
        self.table_type == TableType::Index
    }

    /// Returns an error if `DROP` statements are used on the wrong type of table.
    #[must_use]
    pub fn bad_drop_error(&self) -> RwError {
        let msg = match self.table_type {
            TableType::MaterializedView => {
                "Use `DROP MATERIALIZED VIEW` to drop a materialized view."
            }
            TableType::Index => "Use `DROP INDEX` to drop an index.",
            TableType::Table => "Use `DROP TABLE` to drop a table.",
            TableType::Internal => "Internal tables cannot be dropped.",
        };

        ErrorCode::InvalidInputSyntax(msg.to_owned()).into()
    }

    /// Get the table catalog's associated source id.
    #[must_use]
    pub fn associated_source_id(&self) -> Option<TableId> {
        self.associated_source_id
    }

    pub fn has_associated_source(&self) -> bool {
        self.associated_source_id.is_some()
    }

    /// Get a reference to the table catalog's columns.
    pub fn columns(&self) -> &[ColumnCatalog] {
        &self.columns
    }

    /// Get a reference to the table catalog's pk desc.
    pub fn pk(&self) -> &[ColumnOrder] {
        self.pk.as_ref()
    }

    /// Get the column IDs of the primary key.
    pub fn pk_column_ids(&self) -> Vec<ColumnId> {
        self.pk
            .iter()
            .map(|x| self.columns[x.column_index].column_id())
            .collect()
    }

    /// Get a [`TableDesc`] of the table.
    pub fn table_desc(&self) -> TableDesc {
        use risingwave_common::catalog::TableOption;

        let table_options =
            TableOption::build_table_option(&self.properties.inner().clone().into_iter().collect());

        TableDesc {
            table_id: self.id,
            pk: self.pk.clone(),
            stream_key: self.stream_key.clone(),
            columns: self.columns.iter().map(|c| c.column_desc.clone()).collect(),
            dist_key_in_pk_indices: self.dist_key_in_pk.clone(),
            append_only: self.append_only,
            retention_seconds: table_options
                .retention_seconds
                .unwrap_or(TABLE_OPTION_DUMMY_RETENTION_SECOND),
            value_indices: self.value_indices.clone(),
            read_prefix_len_hint: self.read_prefix_len_hint,
            watermark_columns: self.watermark_columns.clone(),
            versioned: self.version.is_some(),
        }
    }

    /// Get a reference to the table catalog's name.
    pub fn name(&self) -> &str {
        self.name.as_ref()
    }

    pub fn dist_key_in_pk_indices(&self) -> &[usize] {
        &self.dist_key_in_pk.as_ref()
    }

    pub fn to_internal_table_prost(&self) -> ProstTable {
        use risingwave_common::catalog::{DatabaseId, SchemaId};
        self.to_prost(
            SchemaId::placeholder().schema_id,
            DatabaseId::placeholder().database_id,
        )
    }

    /// Returns the SQL statement that can be used to create this table.
    pub fn create_sql(&self) -> String {
        self.definition.clone()
    }

    /// Get a reference to the table catalog's version.
    pub fn version(&self) -> Option<&TableVersion> {
        self.version.as_ref()
    }

    /// Get the table's version id. Returns `None` if the table has no version field.
    pub fn version_id(&self) -> Option<TableVersionId> {
        self.version().map(|v| v.version_id)
    }

    pub fn to_prost(&self, schema_id: SchemaId, database_id: DatabaseId) -> ProstTable {
        ProstTable {
            id: self.id.table_id,
            schema_id,
            database_id,
            name: self.name.clone(),
            columns: self.columns().iter().map(|c| c.to_protobuf()).collect(),
            pk: self.pk.iter().map(|o| o.to_protobuf()).collect(),
            stream_key: self.stream_key.iter().map(|x| *x as _).collect(),
            dependent_relations: vec![],
            optional_associated_source_id: self
                .associated_source_id
                .map(|source_id| OptionalAssociatedSourceId::AssociatedSourceId(source_id.into())),
            table_type: self.table_type.to_prost() as i32,
            append_only: self.append_only,
            owner: self.owner,
            properties: self.properties.inner().clone().into_iter().collect(),
            fragment_id: self.fragment_id,
            vnode_col_index: self.vnode_col_index.map(|i| i as _),
            row_id_index: self.row_id_index.map(|i| i as _),
            value_indices: self.value_indices.iter().map(|x| *x as _).collect(),
            definition: self.definition.clone(),
            read_prefix_len_hint: self.read_prefix_len_hint as u32,
            version: self.version.as_ref().map(TableVersion::to_prost),
            watermark_indices: self.watermark_columns.ones().map(|x| x as _).collect_vec(),
            dist_key_in_pk: self.dist_key_in_pk.iter().map(|x| *x as _).collect(),
            handle_pk_conflict_behavior: self.conflict_behavior_type,
        }
    }
}

impl From<ProstTable> for TableCatalog {
    fn from(tb: ProstTable) -> Self {
        let id = tb.id;
        let table_type = tb.get_table_type().unwrap();
        let associated_source_id = tb.optional_associated_source_id.map(|id| match id {
            OptionalAssociatedSourceId::AssociatedSourceId(id) => id,
        });
        let name = tb.name.clone();
        let mut col_names = HashSet::new();
        let mut col_index: HashMap<i32, usize> = HashMap::new();
        let columns: Vec<ColumnCatalog> = tb.columns.into_iter().map(ColumnCatalog::from).collect();
        for (idx, catalog) in columns.clone().into_iter().enumerate() {
            let col_name = catalog.name();
            if !col_names.insert(col_name.to_string()) {
                panic!("duplicated column name {} in table {} ", col_name, tb.name)
            }

            let col_id = catalog.column_desc.column_id.get_id();
            col_index.insert(col_id, idx);
        }

        let pk = tb.pk.iter().map(ColumnOrder::from_protobuf).collect();
        let mut watermark_columns = FixedBitSet::with_capacity(columns.len());
        for idx in tb.watermark_indices {
            watermark_columns.insert(idx as _);
        }

        let conflict_behavior_type = tb.handle_pk_conflict_behavior;

        Self {
            id: id.into(),
            associated_source_id: associated_source_id.map(Into::into),
            name,
            pk,
            columns,
            table_type: TableType::from_prost(table_type),
            stream_key: tb.stream_key.iter().map(|x| *x as _).collect(),
            append_only: tb.append_only,
            owner: tb.owner,
            properties: WithOptions::new(tb.properties),
            fragment_id: tb.fragment_id,
            vnode_col_index: tb.vnode_col_index.map(|x| x as usize),
            row_id_index: tb.row_id_index.map(|x| x as usize),
            value_indices: tb.value_indices.iter().map(|x| *x as _).collect(),
            definition: tb.definition,
            conflict_behavior_type,
            read_prefix_len_hint: tb.read_prefix_len_hint as usize,
            version: tb.version.map(TableVersion::from_prost),
            watermark_columns,
            dist_key_in_pk: tb.dist_key_in_pk.iter().map(|x| *x as _).collect(),
        }
    }
}

impl From<&ProstTable> for TableCatalog {
    fn from(tb: &ProstTable) -> Self {
        tb.clone().into()
    }
}

impl RelationCatalog for TableCatalog {
    fn owner(&self) -> UserId {
        self.owner
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use risingwave_common::catalog::{
        row_id_column_desc, ColumnCatalog, ColumnDesc, ColumnId, TableId,
    };
    use risingwave_common::constants::hummock::PROPERTIES_RETENTION_SECOND_KEY;
    use risingwave_common::test_prelude::*;
    use risingwave_common::types::*;
    use risingwave_common::util::sort_util::OrderType;
    use risingwave_pb::catalog::Table as ProstTable;
    use risingwave_pb::plan_common::{
        ColumnCatalog as ProstColumnCatalog, ColumnDesc as ProstColumnDesc,
    };

    use super::*;
    use crate::catalog::table_catalog::{TableCatalog, TableType};
    use crate::WithOptions;

    #[test]
    fn test_into_table_catalog() {
        let table: TableCatalog = ProstTable {
            id: 0,
            schema_id: 0,
            database_id: 0,
            name: "test".to_string(),
            table_type: ProstTableType::Table as i32,
            columns: vec![
                ProstColumnCatalog {
                    column_desc: Some((&row_id_column_desc()).into()),
                    is_hidden: true,
                },
                ProstColumnCatalog {
                    column_desc: Some(ProstColumnDesc::new_struct(
                        "country",
                        1,
                        ".test.Country",
                        vec![
                            ProstColumnDesc::new_atomic(
                                DataType::Varchar.to_protobuf(),
                                "address",
                                2,
                            ),
                            ProstColumnDesc::new_atomic(
                                DataType::Varchar.to_protobuf(),
                                "zipcode",
                                3,
                            ),
                        ],
                    )),
                    is_hidden: false,
                },
            ],
            pk: vec![ColumnOrder::new(0, OrderType::ascending()).to_protobuf()],
            stream_key: vec![0],
            dependent_relations: vec![],
            optional_associated_source_id: OptionalAssociatedSourceId::AssociatedSourceId(233)
                .into(),
            append_only: false,
            owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
            properties: HashMap::from([(
                String::from(PROPERTIES_RETENTION_SECOND_KEY),
                String::from("300"),
            )]),
            fragment_id: 0,
            value_indices: vec![0],
            definition: "".into(),
            read_prefix_len_hint: 0,
            vnode_col_index: None,
            row_id_index: None,
            version: Some(ProstTableVersion {
                version: 0,
                next_column_id: 2,
            }),
            watermark_indices: vec![],
            handle_pk_conflict_behavior: 0,
            dist_key_in_pk: vec![],
        }
        .into();

        assert_eq!(
            table,
            TableCatalog {
                id: TableId::new(0),
                associated_source_id: Some(TableId::new(233)),
                name: "test".to_string(),
                table_type: TableType::Table,
                columns: vec![
                    ColumnCatalog::row_id_column(),
                    ColumnCatalog {
                        column_desc: ColumnDesc {
                            data_type: DataType::new_struct(
                                vec![DataType::Varchar, DataType::Varchar],
                                vec!["address".to_string(), "zipcode".to_string()]
                            ),
                            column_id: ColumnId::new(1),
                            name: "country".to_string(),
                            field_descs: vec![
                                ColumnDesc {
                                    data_type: DataType::Varchar,
                                    column_id: ColumnId::new(2),
                                    name: "address".to_string(),
                                    field_descs: vec![],
                                    type_name: String::new(),
                                },
                                ColumnDesc {
                                    data_type: DataType::Varchar,
                                    column_id: ColumnId::new(3),
                                    name: "zipcode".to_string(),
                                    field_descs: vec![],
                                    type_name: String::new(),
                                }
                            ],
                            type_name: ".test.Country".to_string()
                        },
                        is_hidden: false
                    }
                ],
                stream_key: vec![0],
                pk: vec![ColumnOrder::new(0, OrderType::ascending())],
                append_only: false,
                owner: risingwave_common::catalog::DEFAULT_SUPER_USER_ID,
                properties: WithOptions::new(HashMap::from([(
                    String::from(PROPERTIES_RETENTION_SECOND_KEY),
                    String::from("300")
                )])),
                fragment_id: 0,
                vnode_col_index: None,
                row_id_index: None,
                value_indices: vec![0],
                definition: "".into(),
                conflict_behavior_type: 0,
                read_prefix_len_hint: 0,
                version: Some(TableVersion::new_initial_for_test(ColumnId::new(1))),
                watermark_columns: FixedBitSet::with_capacity(2),
                dist_key_in_pk: vec![],
            }
        );
        assert_eq!(table, TableCatalog::from(table.to_prost(0, 0)));
    }
}
