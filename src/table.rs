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
pub struct Table {
    /// Name of the table
    name: String,

    /// Map from logical IDs to record IDs
    lid_to_rid: HashMap<i64, RID>,

    /// Number of columns in the table
    number_of_columns: usize,

    /// Set of table's base pages
    base_pages: Vec<Page>,

    /// Set of table's tail pages
    tail_pages: Vec<Page>
}

#[pymethods]
impl Table {
    /// Create a new empty table
    #[new]
    pub fn new(name: String, number_of_columns: usize) -> Self {
        Table {
            name, number_of_columns,
            lid_to_rid: HashMap::new(),
            base_pages: vec![Page::Base(Vec::new())],
            tail_pages: vec![Page::Tail(Vec::new())]
        }
    }

    /// Insert a new record. If the record is successfully inserted, return `Ok()`. Otherwise,
    /// return `Err(String)`.
    pub fn insert(&mut self, columns: Vec<i64>) -> PyResult<()> {
        if columns.len() != self.number_of_columns {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Wrong number of columns",
            ));
        }

        // The first key is our LID - make sure it doesn't exist yet
        if self.lid_to_rid.contains_key(&columns[0]) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Record with this ID already exists",
            ));
        }

        match self.base_pages.last() {
            Some(Page::Base(base_page)) => {
                println!("[DEBUG] Adding to base page...")
            },

            Some(Page::Tail(tail_page)) => {
                panic!("[ERROR] Base pages vector contains tail page.")
            }

            None => {
                panic!("[ERROR] Table has no base pages.")
            }
        }

        Ok(())
    }
}

/// Represents either a base or tail page. Note that the `Base` and `Tail` variants both contain
/// `Vec<Column>` because they are _logical_ constructs. Physically, there is no difference between them.
pub enum Page {
    Base(Vec<ColumnPage>),
    Tail(Vec<ColumnPage>)
}

/// Represents a column. You can think of a column as a "column page"
pub struct ColumnPage {
    records: Vec<i64>
}
