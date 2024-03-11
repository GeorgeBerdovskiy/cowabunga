use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::bufferpool::PhysicalPageID;
use crate::table::{RID, Address, Indexer};

/// Contains buffer pool metadata for writing to disk.
#[derive(Serialize, Deserialize, Debug)]
pub struct BufferPoolPersistable {

    /// Map from table names to identifiers (table identifiers exist only
    /// for ownership / efficiency reasons).
    pub table_identifiers: HashMap<String, usize>,

    /// Next available table identifier.
    pub next_table_id: usize,
}

/// Contains column metadata for writing to disk.
#[derive(Serialize, Deserialize, Debug)]
pub struct ColumnPeristable {
    /// Index of the next available page (for writing) in the corresponding column file.
    pub next_page_index: usize
}

/// Contains logical page metadata for writing to disk.
#[derive(Serialize, Deserialize)]
pub struct LogicalPagePersistable {
    /// Physical page IDs corresponding to each column.
    pub columns: Vec<PhysicalPageID>,
}

/// Contains page range metadata for writing to disk.
#[derive(Serialize, Deserialize)]
pub struct PageRangePersistable {
    /// Logical base pages.
    pub base_pages: Vec<LogicalPagePersistable>,

    /// Logical tail pages.
    pub tail_pages: Vec<LogicalPagePersistable>,

    /// Next base page index.
    pub next_base_page: usize,

    /// Tail Page Sequence (TPS)
    pub tps: usize,
}

/// Contains table metadata for writing to disk.
#[derive(Serialize, Deserialize)]
pub struct TableMetadata {
    /// Name of table.
    pub name: String,

    /// Table identifier (granted by buffer pool).
    pub table_identifier: usize,

    /// Number of columns.
    pub num_columns: usize,

    /// Index of key column.
    pub key_column: usize,

    /// Next avaliable RID.
    pub next_rid: usize,

    /// Vector of _persistable_ page ranges
    pub page_ranges: Vec<PageRangePersistable>,

    /// Map from RIDs to their addresses.
    pub page_directory: HashMap<RID, Address>,

    /// Next available page range
    pub next_page_range: usize,

    /// Indexer (... hopefully self explanatory)
    pub indexer: Indexer,
}
