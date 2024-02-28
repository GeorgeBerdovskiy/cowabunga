use std::collections::HashMap;
use serde::{Serialize, Deserialize};
use crate::bufferpool::PhysicalPageID;

/// Contains buffer pool metadata for writing to disk.
#[derive(Serialize, Deserialize, Debug)]
pub struct BufferPoolPersistable {
    /// Map from physical pages IDs to the frame indexes they inhabit.
    pub page_map: HashMap<PhysicalPageID, usize>,

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
