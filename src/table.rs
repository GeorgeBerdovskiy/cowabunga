use std::cell;
use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::pyclass;

/// Number of cells that can be stored in a page.
const CELLS_PER_PAGE: usize = 512;

/// Contains one record field. Because all fields are 64 bit integers, we use `i64`.
/// If a field has been written, it contains `Some(i64)`. Otherwise, it holds `None`.
#[derive(Copy, Clone, Debug)]
struct Cell(Option<i64>);

impl Cell {
    /// Create a new cell.
    pub fn new(value: Option<i64>) -> Self {
        Cell(value)
    }

    /// Create a new **empty** cell.
    pub fn empty() -> Self {
        Cell(None)
    }
}

/// Represents a physical page. In our design, every physical page has 512 cells. Therefore,
/// each has a size of **4096 bytes.** In order to calculate the physical location of a record,
/// we divide the RID by 512 for the page index and calculate the remainder for the offset (or
/// cell index).
#[derive(Clone, Copy, Debug)]
struct Page {
    /// Fixed size array of cells.
    cells: [Cell; CELLS_PER_PAGE],

    /// The number of cells currently written. Also represents the next available index.
    cell_count: usize
}

impl Page {
    /// Create a new empty physical page.
    pub fn new() -> Self {
        Page {
            cells: [Cell::empty(); CELLS_PER_PAGE],
            cell_count: 0
        }
    }

    /// Write a new record to a physical page. Guaranteed to succeed.
    pub fn write(&mut self, offset: usize, value: Option<i64>) {
        self.cells[offset] = Cell::new(value);
        self.cell_count += 1;
    }

    pub fn read(&self, offset: usize) -> Option<i64> {
        self.cells[offset].0
    }
}

/// Represents the RID (record identifier) of a record. The struct isn't required, but benefits
/// code readability and helps us catch errors using the type checker.
/// 
/// The RID is used to calculate the physical location of a page by dividing it by 512 for the
/// **page index** and calculating `<RID> % 512` for the offset. We increment the RID by one for
/// every new record so it's impossible to accidentally overwrite previous records.
#[derive(Debug)]
struct RID {
    /// Index of the page that contains this record
    raw_rid: usize,

    /// True if this RID refers to a base page, false otherwise
    base: bool
}

/// Represents a table, including all of its indexes, pages, and RID mappings.
#[pyclass]
pub struct Table {
    /// The name of the table.
    name: String,

    /// The number of columns.
    num_columns: usize,

    /// The LID (or _logical_ identifier) is a unique identifier that doesn't change as the table
    /// is updated, while the RID (or _record_ identifier) represents the physical location of a record.
    /// 
    /// Every base and tail record is associated with an RID, and they share the same RID space.
    /// 
    /// When working with a logical record, our first step is to obtain the RID of its most recent update
    /// using its logical identifier.
    lid_to_rid: HashMap<i64, RID>,

    /// The next available **base** RID. We increment this field by one for every new base record.
    next_base_rid: usize,

    /// The next available **tail** RID. We increment this field by one for every new base record.
    next_tail_rid: usize,

    /// List of logical columns. See the definition of `Column` for more information
    columns: Vec<Column>
}

#[derive(Clone, Debug)]
struct Column {
    base_pages: Vec<Page>,
    tail_pages: Vec<Page>
}

impl Column {
    pub fn new() -> Self {
        Column {
            base_pages: Vec::new(),
            tail_pages: Vec::new()
        }
    }

    pub fn insert_base(&mut self, page_index: usize, cell_index: usize, value: Option<i64>) {
        if self.base_pages.len() <= page_index {
            self.base_pages.push(Page::new())
        }

        self.base_pages[page_index].write(cell_index, value);
    }

    pub fn insert_tail(&mut self, page_index: usize, cell_index: usize, value: Option<i64>) {
        if self.tail_pages.len() <= page_index {
            self.tail_pages.push(Page::new())
        }

        self.tail_pages[page_index].write(cell_index, value);
    }

    pub fn read_base(&self, page_index: usize, cell_index: usize) -> Option<i64> {
        self.base_pages[page_index].read(cell_index)
    }

    pub fn read_tail(&self, page_index: usize, cell_index: usize) -> Option<i64> {
        self.tail_pages[page_index].read(cell_index)
    }
}

#[pymethods]
impl Table {
    #[new]
    pub fn new(name: String, number_of_columns: usize) -> Self {
        Table {
            name: name,
            num_columns: number_of_columns,
            lid_to_rid: HashMap::new(),
            next_base_rid: 0,
            next_tail_rid: 0,
            columns: vec![Column::new(); number_of_columns]
        }
    }

    /// Insert a new record with the columns in `columns`. Note that values may be `None`.
    /// Returns the ID of your inserted record if successful and an error otherwise.
    // TODO - Move to `Query`
    pub fn insert(&mut self, columns: Vec<i64>) -> PyResult<i64> {
        if self.lid_to_rid.contains_key(&columns[0]) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Record with this ID already exists.",
            ));
        }

        let page_index = self.next_base_rid / CELLS_PER_PAGE;
        let cell_index = self.next_base_rid % CELLS_PER_PAGE;

        for column in self.columns.iter_mut().zip(columns.iter()) {
            column.0.insert_base(page_index, cell_index, Some(*column.1));
        }

        self.lid_to_rid.insert(columns[0], RID {
            raw_rid: self.next_base_rid,
            base: true
        });

        self.next_base_rid += 1;
        println!("[INFO] Inserted base record {:?} with RID {}.", columns, self.next_base_rid);

        Ok(columns[0])
    }

    pub fn update(&mut self, columns: Vec<Option<i64>>) -> PyResult<i64> {
        if columns[0].is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Cannot have 'None' key.",
            ));
        }

        if !self.lid_to_rid.contains_key(&columns[0].unwrap()) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No record with this ID exists.",
            ));
        }

        let page_index = self.next_tail_rid / CELLS_PER_PAGE;
        let cell_index = self.next_tail_rid % CELLS_PER_PAGE;

        for column in self.columns.iter_mut().zip(columns.iter()) {
            column.0.insert_tail(page_index, cell_index, *column.1);
        }

        let prev_rid = self.lid_to_rid.insert(columns[0].unwrap(), RID {
            raw_rid: self.next_tail_rid,
            base: false
        });

        println!("[INFO] Updated record w/ID {:?} with RID {:?} -> {}.", columns[0], prev_rid, self.next_tail_rid);
        self.next_tail_rid += 1;

        Ok(columns[0].unwrap())
    }

    /// Select a logical record given its LID. Assume for now that `key` is the LID and is
    /// always the first column.
    // TODO - Move to `Query`
    pub fn select(&mut self, key: i64) -> PyResult<Record> {
        match self.lid_to_rid.get(&key) {
            Some(rid) => {
                let page_index = rid.raw_rid / CELLS_PER_PAGE;
                let cell_index = rid.raw_rid % CELLS_PER_PAGE;

                let mut result: Vec<Option<i64>> = Vec::new();

                println!("[DEBUG] Preparing to grab columns...");

                for column in self.columns.iter() {
                    if rid.base {
                        println!("[DEBUG] Adding base record column.");
                        result.push(column.read_base(page_index, cell_index));
                    } else {
                        println!("[DEBUG] Adding tail record column.");
                        result.push(column.read_tail(page_index, cell_index));
                    }
                }

                Ok(Record {
                    columns: result
                })
            },

            None => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No record with this ID exists.",
            ))
        }
    }
}

/// Represents the query interface.
#[pyclass]
struct Query {
    /// The table we will interact with.
    table: Table
}

/// Represents a **logical** record returned to the user in response to selection and
/// range queries.
#[pyclass]
pub struct Record {
    columns: Vec<Option<i64>>
}

#[pymethods]
impl Record {
    pub fn __str__(&self) -> PyResult<String> {
        let mut result = String::from("[");

        for column in self.columns.iter() {
            result = result + &format!("{:?}, ", column.or_else(|| None));
        }

        result += "]";

        Ok(result)
    }
}
