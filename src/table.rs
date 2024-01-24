use std::collections::HashMap;
use std::io;

use pyo3::prelude::*;

/// Represents record IDs (RIDs), which are _physical_.
struct RID {
    /// Index of the page that contains this record
    page: usize,

    /// Index of the slot that contains this record
    slot: usize
}

/// Represents a table. According to the L-Store architecture, a table is really just an index,
/// a collection of base pages, and a collection of tail pages.
#[pyclass]
struct Table {
    /// Name of the table
    name: String,

    /// Map from logical IDs to record IDs
    lid_to_rid: HashMap<i64, RID>,

    /// Number of columns in the table
    number_of_columns: i32, // NOTE - This could be smaller?

    /// Set of table's base pages
    base_pages: Vec<Page>,

    /// Set of table's tail pages
    tail_pages: Vec<Page>
}

impl Table {
    /// Create a new empty table
    pub fn new(name: String, number_of_columns: i32) -> Self {
        Table {
            name, number_of_columns,
            lid_to_rid: HashMap::new(),
            base_pages: Vec::new(),
            tail_pages: Vec::new()
        }
    }

    /// Insert a new record. If the record is successfully inserted, return `Ok()`. Otherwise,
    /// return `Err(io::Error)`.
    pub fn insert() -> Result<(), io::Error> {
        unimplemented!()
    }
}

/// Represents either a base or tail page. Note that the `Base` and `Tail` variants both contain
/// `Vec<Column>` because they are _logical_ constructs. Physically, there is no difference between them.
enum Page {
    Base([ColumnPage; 1024]),
    Tail([ColumnPage; 1024])
}

/// Represents a column. You can think of a column as a "column page"
struct ColumnPage {
    records: Vec<i64>
}
