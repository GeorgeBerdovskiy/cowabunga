use pyo3::{buffer, prelude::*};
use core::num;
use std::{collections::HashMap, hash::Hash, marker::PhantomData};

const BASE_PAGES_PER_RANGE: usize = 16;

use crate::bufferpool::*;

#[derive(Clone, Copy)]
struct Base;

#[derive(Clone, Copy)]
struct Tail;

#[derive(Clone)]
struct LogicalPage<T> {
    /// Vector of **page identifiers**.
    columns: Vec<PageIdentifier>,
    buffer_pool_manager: &mut BufferPool,
    phantom: PhantomData<T>
}

impl<T> LogicalPage<T> {
    pub fn new(num_columns: usize, buffer_pool_manager: &'static mut BufferPool) -> LogicalPage<T> {
        LogicalPage {
            columns: buffer_pool_manager.allocate_pages(num_columns),
            buffer_pool_manager: buffer_pool_manager,
            phantom: PhantomData::<T>
        }
    }
}

impl<Base> LogicalPage<Base> {
    pub fn insert(&mut self, columns: Vec<Option<i64>>) -> Result<usize, ()> {
        let mut offset = 0;

        for pair in self.columns.iter().zip(columns.iter()) {
            match self.buffer_pool_manager.write_next(*pair.0, *pair.1) {
                Ok(returned_offset) => offset = returned_offset,
                Err(error) => return Err(error)
            }
        }

        Ok(offset)
    }
}

struct PageRange {
    base_pages: Vec<LogicalPage<Base>>,
    tail_pages: Vec<LogicalPage<Tail>>,

    next_base_page: usize
}

impl PageRange {
    pub fn new(num_columns: usize, buffer_pool_manager: &'static mut BufferPool) -> Self {
        PageRange {
            base_pages: vec![LogicalPage::<Base>::new(num_columns, buffer_pool_manager); BASE_PAGES_PER_RANGE],
            tail_pages: vec![LogicalPage::<Tail>::new(num_columns, buffer_pool_manager)],
            
            next_base_page: 0
        }
    }

    pub fn insert_base(&mut self, columns: Vec<Option<i64>>) -> Result<(usize, usize), ()> {
        if self.next_base_page == BASE_PAGES_PER_RANGE {
            return Err(());
        }

        match self.base_pages[self.next_base_page].insert(columns) {
            Ok(offset) => {
                return Ok((self.next_base_page, offset))
            },

            Err(_) => {
                self.next_base_page += 1;
                return self.insert_base(columns);
            }
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
struct RID {
    raw: usize
}

#[derive(Eq, Hash, PartialEq)]
struct LID {
    raw: i64
}

struct BaseAddress {
    range: usize,

    /// **Logical** page index.
    page: usize,

    offset: usize
}

#[pyclass]
pub struct Table {
    name: String,
    num_columns: usize,

    /// Index of the primary key column.
    key_column: usize,

    page_ranges: Vec<PageRange>,
    page_directory: HashMap<RID, BaseAddress>,
    lid_to_rid: HashMap<LID, RID>,

    /// Index of the next available page range.
    next_page_range: usize,

    /// Reference to the buffer pool manager shared by all tables.
    buffer_pool_manager: &'static BufferPool
}

impl Table {
    pub fn new(name: String, num_columns: usize, key_column: usize, buffer_pool_manager: &'static mut BufferPool) -> Self {
        Table {
            name,
            num_columns,
            key_column,
            page_ranges: vec![PageRange::new(num_columns, buffer_pool_manager)],
            page_directory: HashMap::new(),
            lid_to_rid: HashMap::new(),
            next_page_range: 0,
            buffer_pool_manager
        }
    }

    /// Add a **base record** to the table.
    pub fn insert(&mut self, columns: Vec<Option<i64>>) -> Result<BaseAddress, ()> {
        if columns.len() < self.num_columns {
            return Err(());
        }

        // NOTE - This will crash if the value in columns[self.key_column] is `None`
        if self.lid_to_rid.get(&LID { raw: columns[self.key_column].unwrap() }).is_some() {
            return Err(());
        }

        match self.page_ranges[self.next_page_range].insert_base(columns) {
            Ok((page, offset)) => Ok(BaseAddress {
                range: self.next_page_range,
                page,
                offset
            }),

            Err(_) => {
                // This page range is full - add new range
                self.page_ranges.push(PageRange::new(self.num_columns, &mut self.buffer_pool_manager));
                self.next_page_range += 1;

                return self.insert(columns);
            }
        }
    }
}

/*/// Number of cells that can be stored in a page.
const CELLS_PER_PAGE: usize = 512;

/// Represents the RID (record identifier) of a record. The struct isn't strictly required, but
/// it benefits code readability and helps the type checker catch errors.
///
/// The RID is used to calculate the physical location of a page by dividing it by 512 for the
/// **page index** and calculating `<RID> % 512` for the offset. We increment the RID by one for
/// every new record so it's impossible to accidentally overwrite previous records.
#[derive(Debug)]
struct RID {
    /// Index of the page that contains this record.
    raw: usize,

    /// True if this RID refers to a base page, false otherwise.
    is_base: bool,
}

/// Represents a logical column. Because every column consists of a set of base and tail pages,
/// the struct contains two vectors of pages.

// TODO: Disgtinguish between base and tail pages using enums or generic type arguments. This
// may improve the implementation, which currently has separate methods for base and tail
// operations... can they be consolidated by this modification?
#[derive(Clone, Debug)]
struct Column {
    /// List of base page _identifiers_.
    base_pages: Vec<usize>,

    /// List of tail page _identifiers_.
    tail_pages: Vec<usize>,
}

impl Column {
    /// Create a new empty column.
    pub fn new() -> Self {
        Column {
            base_pages: Vec::new(),
            tail_pages: Vec::new(),
        }
    }

    /// Insert a base record to this column.
    pub fn insert_base(&mut self, page_index: usize, cell_index: usize, value: Option<i64>) {
        if self.base_pages.len() <= page_index {
            // Add a new page if the requested page index is out of bounds
            self.base_pages.push(Page::new())
        }

        // Write `value` to the cell at index `cell_index` on the page at index `page_index`
        self.base_pages[page_index].write(cell_index, value);
    }

    /// Insert a tail record to this column.
    pub fn insert_tail(&mut self, page_index: usize, cell_index: usize, value: Option<i64>) {
        if self.tail_pages.len() <= page_index {
            // Add a new page if the requested page index is out of bounds
            self.tail_pages.push(Page::new())
        }

        // Write `value` to the cell at index `cell_index` on the page at index `page_index`
        self.tail_pages[page_index].write(cell_index, value);
    }

    /// Read from a base record.
    pub fn read_base(&self, page_index: usize, cell_index: usize) -> Option<i64> {
        self.base_pages[page_index].read(cell_index)
    }

    /// Read from a tail record.
    pub fn read_tail(&self, page_index: usize, cell_index: usize) -> Option<i64> {
        self.tail_pages[page_index].read(cell_index)
    }
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

    /// List of table columns.
    columns: Vec<Column>,
}

#[pymethods]
impl Table {
    /// Create a new table given a name and the number of columns.
    #[new]
    pub fn new(name: String, num_columns: usize) -> Self {
        Table {
            name,
            num_columns,
            lid_to_rid: HashMap::new(),
            next_base_rid: 0,
            next_tail_rid: 0,

            // The first two columns are the _indirection_ and _schema encoding_ columns,
            // repsectively. The rest are defined by the user.
            columns: vec![Column::new(); num_columns + 2],
        }
    }

    /// Insert a new record with the columns in `columns`. Note that values may be `None`.
    /// Returns the ID of your inserted record if successful and an error otherwise.
    // TODO: Move to `Query`
    pub fn insert(&mut self, columns: Vec<i64>) -> PyResult<i64> {
        if self.lid_to_rid.contains_key(&columns[0]) {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "Record with this ID already exists.",
            ));
        }

        let page_index = self.next_base_rid / CELLS_PER_PAGE;
        let cell_index = self.next_base_rid % CELLS_PER_PAGE;

        self.columns[0].insert_base(page_index, cell_index, None);
        self.columns[1].insert_base(page_index, cell_index, Some(-1));

        for column in self.columns.iter_mut().skip(2).zip(columns.iter()) {
            column
                .0
                .insert_base(page_index, cell_index, Some(*column.1));
        }

        self.lid_to_rid.insert(
            columns[0],
            RID {
                raw: self.next_base_rid,
                is_base: true,
            },
        );

        self.next_base_rid += 1;

        println!(
            "[INFO] Inserted base record {:?} with RID {}.",
            columns, self.next_base_rid
        );

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

        for column in self.columns.iter_mut().skip(2).zip(columns.iter()) {
            column.0.insert_tail(page_index, cell_index, *column.1);
        }

        let prev_rid = self.lid_to_rid.insert(
            columns[0].unwrap(),
            RID {
                raw: self.next_tail_rid,
                is_base: false,
            },
        );

        self.columns[0].insert_tail(
            page_index,
            cell_index,
            Some(prev_rid.as_ref().unwrap().raw as i64),
        );

        self.columns[1].insert_tail(
            page_index,
            cell_index,
            Some(if prev_rid.as_ref().unwrap().is_base {
                1
            } else {
                0
            }),
        );

        println!(
            "[INFO] Updated record w/ID {:?} with RID {:?} -> {}.",
            columns[0], prev_rid, self.next_tail_rid
        );

        self.next_tail_rid += 1;

        Ok(columns[0].unwrap())
    }

    /// Select a logical record given its LID. Assume for now that `key` is the LID and
    /// is always the first column.
    // TODO: Move to `Query`
    pub fn select(&mut self, key: i64) -> PyResult<Record> {
        // This is horrible and rushed! Absolutely come back to this
        match self.lid_to_rid.get(&key) {
            Some(rid) => {
                let page_index = rid.raw / CELLS_PER_PAGE;
                let cell_index = rid.raw % CELLS_PER_PAGE;

                let mut result: Vec<Option<i64>> = Vec::new();

                println!("[DEBUG] Preparing to grab columns...");

                for column in self.columns.iter() {
                    if rid.is_base {
                        println!("[DEBUG] Adding base record column.");
                        result.push(column.read_base(page_index, cell_index));
                    } else {
                        println!("[DEBUG] Adding tail record column.");
                        let mut col_val = column.read_tail(page_index, cell_index);

                        // We may need to travel backwards to get the value of this column
                        let mut last_rid = RID {
                            raw: rid.raw,
                            is_base: rid.is_base,
                        };
                        let mut last_rid_page = rid.raw / 512;
                        let mut last_rid_cell = rid.raw % 512;

                        while col_val.is_none() {
                            // This may not work on 32 bit systems... rewrite soon
                            if last_rid.is_base {
                                last_rid = RID {
                                    raw: self.columns[0]
                                        .read_base(last_rid_page, last_rid_cell)
                                        .unwrap() as usize,
                                    is_base: self.columns[1]
                                        .read_base(last_rid_page, last_rid_cell)
                                        .unwrap()
                                        == 1,
                                };
                            } else {
                                last_rid = RID {
                                    raw: self.columns[0]
                                        .read_tail(last_rid_page, last_rid_cell)
                                        .unwrap() as usize,
                                    is_base: self.columns[1]
                                        .read_tail(last_rid_page, last_rid_cell)
                                        .unwrap()
                                        == 1,
                                };
                            }

                            last_rid_page = last_rid.raw / 512;
                            last_rid_cell = last_rid.raw % 512;

                            col_val = if last_rid.is_base {
                                column.read_base(last_rid_page, last_rid_cell)
                            } else {
                                column.read_tail(last_rid_page, last_rid_cell)
                            }
                        }

                        result.push(col_val);
                    }
                }

                Ok(Record { columns: result })
            }

            None => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                "No record with this ID exists.",
            )),
        }
    }
}

/// Represents the query interface.
#[pyclass]
struct Query {
    /// The table we will interact with.
    table: Table,
}

/// Represents a **logical** record returned to the user in response to selection and
/// range queries.
#[pyclass]
pub struct Record {
    columns: Vec<Option<i64>>,
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
*/