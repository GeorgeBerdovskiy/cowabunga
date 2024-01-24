use std::collections::HashMap;
use std::marker::PhantomData;

use pyo3::prelude::*;

/// Represents record IDs (RIDs), which are _physical_.
#[derive(Debug)]
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
    base_pages: Vec<Page<Base>>,

    /// Set of table's tail pages
    tail_pages: Vec<Page<Tail>>
}

#[pymethods]
impl Table {
    /// Create a new empty table.
    #[new]
    pub fn new(name: String, number_of_columns: usize) -> Self {
        Table {
            name, number_of_columns,
            lid_to_rid: HashMap::new(),
            base_pages: vec![Page {
                columns: vec![ColumnPage::new(); number_of_columns],
                phantom: PhantomData::<Base>
            }],
            tail_pages: vec![Page {
                columns: vec![ColumnPage::new(); number_of_columns],
                phantom: PhantomData::<Tail>
            }]
        }
    }

    /// Format as a string for printing in Python.
    pub fn __str__(&self) -> PyResult<String> {
        let string = format!("{}\n---------\n", self.name);
        let string = string + &format!("{:?}", self.lid_to_rid) + "\n------\n";
        let string = string + &format!("{:?}", self.base_pages);

        Ok(string)
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

        let base_page_length = self.base_pages.len() - 1;
        let page = &mut self.base_pages[base_page_length];

        let next_col = columns.iter();
        let mut next_col_index: usize = 0;

        for col in next_col {
            page.columns[next_col_index].insert_record(*col);
            next_col_index += 1;
        }

        let page_index = base_page_length;
        let slot_index = page.columns[0].records.len() - 1;
        self.lid_to_rid.insert(columns[0], RID { page: page_index, slot: slot_index });

        Ok(())
    }
}

/// Represents either a base or tail page. Note that the `Base` and `Tail` variants both contain
/// `Vec<Column>` because they are _logical_ constructs. Physically, there is no difference between them.
#[derive(Debug)]
pub struct Base();

#[derive(Debug)]
pub struct Tail();

#[derive(Debug)]
pub struct Page<T> {
    columns: Vec<ColumnPage>,
    phantom: PhantomData<T>
}

/// Represents a column. You can think of a column as a "column page"
#[derive(Debug)]
#[derive(Clone)]
pub struct ColumnPage {
    records: Vec<i64>
}

impl ColumnPage {
    pub fn new() -> Self {
        ColumnPage {
            records: Vec::new()
        }
    }

    pub fn insert_record(&mut self, value: i64) {
        self.records.push(value)
    }
}
