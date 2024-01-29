use std::collections::HashMap;
use pyo3::prelude::*;
use pyo3::pyclass;

/// Number of cells that can be stored in a page.
const CELLS_PER_PAGE: usize = 512;

/// Contains one record field. Because all fields are 64 bit integers, we use `i64`.
/// If a field has been written, it contains `Some(i64)`. Otherwise, it holds `None`.
#[derive(Copy, Clone)]
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
    _inner: usize
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
        self.base_pages[page_index].write(page_index, value);
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
            columns: Vec::new()
        }
    }

    /// Insert a new record with the columns in `columns`. Note that values may be `None`.
    // TODO - Move to `Query`
    pub fn insert(&mut self, columns: Vec<Option<i64>>) {
        let page_index = self.next_base_rid / CELLS_PER_PAGE;
        let cell_index = self.next_base_rid % CELLS_PER_PAGE;

        for column in self.columns.iter_mut().zip(columns.iter()) {
            column.0.insert_base(page_index, cell_index, *column.1);
        }

        self.next_base_rid += 1;
        println!("[INFO] Inserted base record {:?} with RID {}.", columns, self.next_base_rid);
    }

    /// Select a logical record given its LID. Assume for now that `key` is the LID and is
    /// always the first column.
    // TODO - Move to `Query`
    pub fn select(&mut self, key: i64) -> Record {
        unimplemented!()
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
