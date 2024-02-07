use pyo3::prelude::*;
use std::{collections::HashMap, marker::PhantomData};
use std::sync::{Arc, Mutex};

use crate::constants::*;
use crate::bufferpool::*;

/// Empty type representing **base** pages. Used as a generic type argument for the `LogicalPage` struct
/// to distinguish between base and tail pages without overhead.
#[derive(Clone, Copy)]
struct Base;

/// Empty type representing **tail** pages. Used as a generic type argument for the `LogicalPage` struct
/// to distinguish between base and tail pages without overhead.
#[derive(Clone, Copy)]
struct Tail;

/// Represents a **logical** base or tail page, depending on the provided generic type argument.
struct LogicalPage<T> {
    /// Vector of **page identifiers** used by the buffer pool manager.
    columns: Vec<PageIdentifier>,

    /// Buffer pool manager shared by all tables.
    buffer_pool_manager: Arc<Mutex<BufferPool>>,

    /// Phantom field for the generic type argument (required since none of the other fields
    /// actually use `T`).
    phantom: PhantomData<T>
}

impl<T> LogicalPage<T> {
    /// Create a new logical page with `num_columns` columns and a shared buffer pool manager.
    pub fn new(num_columns: usize, buffer_pool_manager: Arc<Mutex<BufferPool>>) -> LogicalPage<T> {
        LogicalPage {
            columns: buffer_pool_manager.clone().lock().unwrap().allocate_pages(num_columns),
            buffer_pool_manager,
            phantom: PhantomData::<T>
        }
    }

    pub fn read(&self, offset: usize) -> Result<Vec<Option<i64>> ,()>{
        let mut result = Vec::new();

        for column in &self.columns {
            match self.buffer_pool_manager.lock().unwrap().read(*column, offset) {
                Ok(value) => result.push(value),
                Err(_) => return Err(())
            }
        }

        Ok(result)
    }
}

impl LogicalPage<Base> {
    /// Insert a new **base** record given a vector of columns. Returns the offset of this record
    /// on a physical page if successful or `Err(...)` if the physical page has no more space.
    pub fn insert(&mut self, columns: &Vec<Option<i64>>) -> Result<usize, ()> {
        let mut offset = 0;

        // This adds the user's columns, but _not_ the metadata columns
        for pair in self.columns.iter().zip(columns.iter()) {
            match self.buffer_pool_manager.lock().unwrap().write_next(*pair.0, *pair.1) {
                Ok(returned_offset) => offset = returned_offset,
                Err(error) => return Err(error)
            }
        }

        // Now, we need to add the metadata columns. For now, we'll just use `None` for simplicity
        for column in self.columns.iter().skip(columns.len()) {
            self.buffer_pool_manager.lock().unwrap().write_next(*column, None).expect("[ERROR] Failed to write metadata columns.");
        }

        Ok(offset)
    }

    pub fn update_indirection(&mut self, offset: usize, new_rid: RID) -> Result<(), ()> {
        // The page identifier at index self.columns.len() - 2 is the indirection, while the
        // page identifier at index self.columns.len() - 1 is the schema encoding column
        let indirection_col = self.columns[self.columns.len() - 2];
        self.buffer_pool_manager.lock().unwrap().write(indirection_col, offset, Some(new_rid as i64))
    }
}

impl LogicalPage<Tail> {
    /// Insert a new **tail** record given a vector of columns.
    pub fn insert(&mut self, columns: &Vec<Option<i64>>, indirection: Option<i64>) -> Result<usize, ()> {
        let mut offset = 0;

        for pair in columns.iter().zip(self.columns.iter().take(self.columns.len() - 2)) {
            println!("[DEBUG] Writing {:?} to another column...", *pair.0);
            match self.buffer_pool_manager.lock().unwrap().write_next(*pair.1, *pair.0) {
                Ok(returned_offset) => offset = returned_offset,
                Err(error) => return Err(error)
            }
        }

        println!("[DEBUG] Offset thus far is {}", offset);

        // Write the indirection value
        println!("[DEBUG] Preparing to write indrection of {:?}", indirection);
        let res = self.buffer_pool_manager.lock().unwrap().write_next(self.columns[self.columns.len() - 2], indirection);

        println!("[DEBUG] Offset NOW is {:?}", res);

        res
    }
}

/// Represents a page range. Consists of a set of base pages (which should have a set maximum
/// size) and a set of tail pages (which is unbounded).
struct PageRange {
    /// The set of base pages associated with this page range. Whenever we write to this vector,
    /// we ensure that its length doesn't exceed `BASE_PAGES_PER_RANGE` (defined in `constants.rs`).
    base_pages: Vec<LogicalPage<Base>>,

    /// The set of tail pages associated with this page range. It's unbounded, so no checks
    /// on its length are necessary.
    tail_pages: Vec<LogicalPage<Tail>>,

    /// Index of the next base page to which we can write. If it ever becomes `BASE_PAGES_PER_RANGE`,
    /// this page range cannot accept any more base records.
    next_base_page: usize,

    /// Number of columns in the table.
    num_columns: usize,

    /// Shared buffer pool manager
    buffer_pool_manager: Arc<Mutex<BufferPool>>
}

impl PageRange {
    /// Create a new page range given the number of columns and a shared buffer pool manager.
    pub fn new(num_columns: usize, buffer_pool_manager: Arc<Mutex<BufferPool>>) -> Self {
        let mut base_page_vec: Vec<LogicalPage<Base>> = Vec::new();

        // Initialize the base page vector. Unfortunately, we can't use `vec![...]` because
        // logical pages cannot implement `Copy` - the vectors they store prevent this
        for _ in [0..BASE_PAGES_PER_RANGE] {
            base_page_vec.push(LogicalPage::new(num_columns, buffer_pool_manager.clone()));
        }

        PageRange {
            base_pages: base_page_vec,
            tail_pages: vec![LogicalPage::<Tail>::new(num_columns, buffer_pool_manager.clone())],
            next_base_page: 0,
            num_columns,
            buffer_pool_manager: buffer_pool_manager.clone()
        }
    }

    pub fn read_base_record(&mut self, page: usize, offset: usize) -> Result<Vec<Option<i64>>, ()> {
        self.base_pages[page].read(offset)
    }

    pub fn read_tail_record(&mut self, page: usize, offset: usize) -> Result<Vec<Option<i64>>, ()> {
        self.tail_pages[page].read(offset)
    }

    pub fn update_base_indirection(&mut self, base_addr: BaseAddress, new_rid: RID) -> Result<(), ()> {
        self.base_pages[base_addr.page].update_indirection(base_addr.offset, new_rid)
    }

    pub fn insert_tail(&mut self, columns: &Vec<Option<i64>>, indirection: Option<i64>) -> (usize, usize) {
        let next_tail_page = self.tail_pages.len() - 1;

        match self.tail_pages[next_tail_page].insert(&columns, indirection) {
            Ok(offset) => {
                // Record was inserted successfully
                return (next_tail_page, offset);
            },

            Err(_) => {
                // Failed to insert record because there is no more space in the last tail page
                // Add a new tail page and try to insert again
                self.tail_pages.push(LogicalPage::new(self.num_columns, self.buffer_pool_manager.clone()));

                // Note that although this call is recursive, it will have a depth of at most one
                return self.insert_tail(columns, indirection);
            }
        }
    }

    /// Insert a base record into this page range. Returns (page index, offset) if successful
    /// and `Err(...)` otherwise.
    pub fn insert_base(&mut self, columns: &Vec<Option<i64>>) -> Result<(usize, usize), ()> {
        if self.next_base_page >= BASE_PAGES_PER_RANGE {
            // We ran out of base pages - returning this error will trigger the creation
            // of a new page range
            return Err(());
        }

        // We still have at least one base page left to fill... try to insert record
        match self.base_pages[self.next_base_page].insert(&columns) {
            Ok(offset) => {
                // Record was inserted successfully
                return Ok((self.next_base_page, offset))
            },

            Err(_) => {
                // Failed to insert record because there is no more space in the physical pages
                // Increment the base page index and try to insert again
                self.next_base_page += 1;
                self.base_pages.push(LogicalPage::new(self.num_columns, self.buffer_pool_manager.clone()));

                // Note that although this call is recursive, it will have a depth of at most one
                return self.insert_base(columns);
            }
        }
    }
}

/// Represents the _record_ identifier.
type RID = usize;

/// Represents the _logical_ identifier.
type LID = i64;

/// Represents the address of a record. We obtain this address from the page directory,
/// which maps from RIDs to physical addresses.

// NOTE - We might want to rename this to `Address` since it should theoretically
// work for base _and_ tail pages 
#[derive(Clone, Copy)]
struct BaseAddress {
    /// Page range index.
    range: usize,

    /// Logical base page index.
    page: usize,

    /// Physical page offset.
    offset: usize
}

impl BaseAddress {
    // Create a new base address.
    pub fn new(range: usize, page: usize, offset: usize) -> Self {
        BaseAddress {range, page, offset }
    }
}

/// Represents a table and is exposed by PyO3.
#[pyclass]
pub struct Table {
    /// Name of the table.
    name: String,

    /// Number of columns.
    num_columns: usize,

    /// Index of the primary key column.
    key_column: usize,

    /// Next available RID.
    next_rid: usize,

    /// Page ranges associated with this table. Note that it's expanded _dynamically_.
    page_ranges: Vec<PageRange>,

    /// Page directory - maps from RIDs to base record addresses.
    page_directory: HashMap<RID, BaseAddress>,

    /// Maps LIDs to RIDs.
    lid_to_rid: HashMap<LID, RID>,

    /// Index of the next available page range.
    next_page_range: usize,

    /// Buffer pool manager shared by all tables
    buffer_pool_manager: Arc<Mutex<BufferPool>>
}

#[pymethods]
impl Table {
    /// Create a new table given its name, number of columns, primary key column index, and shared
    /// buffer pool manager.
    #[new]
    pub fn new(name: String, num_columns: usize, key_column: usize, buffer_pool_manager: &PyAny) -> Self {
        let buffer_pool_manager = buffer_pool_manager.extract::<PyRef<BufferPool>>().unwrap();
        let buffer_pool_manager = Arc::new(Mutex::new(buffer_pool_manager.clone()));

        Table {
            name,
            num_columns: num_columns,
            key_column: key_column,
            next_rid: 0,

            // The last `NUM_METADATA_COLS` columns are the _indirection_ and _schema encoding_ columns,
            // repsectively. The rest are defined by the user.
            page_ranges: vec![PageRange::new(num_columns + NUM_METADATA_COLS, buffer_pool_manager.clone())],
            page_directory: HashMap::new(),
            lid_to_rid: HashMap::new(),
            next_page_range: 0,
            buffer_pool_manager: buffer_pool_manager
        }
    }

    /// Update an existing record (in other words, insert a **tail record**).
    pub fn update(&mut self, columns: Vec<Option<i64>>) -> PyResult<()> {
        if columns.len() < self.num_columns {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Table has {} columns, but only {} were provided.", self.num_columns, columns.len()),
            ));
        }

        if columns[self.key_column].is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Key cannot be 'None'"),
            ));
        }

        let key_value = columns[self.key_column].unwrap();

        if self.lid_to_rid.get(&key_value).is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Record with identifier {} doesn't exist.", key_value),
            ));
        }

        let base_rid = self.lid_to_rid[&key_value];
        let base_addr = self.page_directory[&base_rid];

        // Grab the base page because we need to check the indirection column
        match self.page_ranges[base_addr.range].read_base_record(base_addr.page, base_addr.offset) {
            Ok(base_columns) => {
                let indirection = base_columns[base_columns.len() - 2];

                println!("[DEBUG] Indirection is {:?}", indirection);

                // We need to store this indirection when adding the tail page
                let (page, offset) = if indirection.is_some() {
                    self.page_ranges[base_addr.range].insert_tail(&columns, indirection)
                } else {
                    self.page_ranges[base_addr.range].insert_tail(&columns, Some(base_rid as i64))
                };

                // Add the new RID to physical address mapping
                // NOTE - It's called "BaseAddress" but I (George) believe it can be used
                // for _any_ record - not just base records
                self.page_directory.insert(self.next_rid, BaseAddress::new(base_addr.range, page, offset));
        
                // Update the base record indirection column
                match self.page_ranges[base_addr.range].update_base_indirection(base_addr, self.next_rid) {
                    Ok(_) => {},
                    Err(_) => panic!("[ERROR] Failed to update base record indirection column.")
                }
        
                // Increment the RID for the next record
                self.next_rid += 1;
        
                Ok(())
            },

            Err(_) => panic!("[ERROR] Failed to grab base record.")
        }
    }

    pub fn select(&mut self, primary_key: i64) -> PyResult<Vec<Option<i64>>> {
        match self.lid_to_rid.get(&primary_key) {
            Some(rid) => {
                let base_addr = self.page_directory[&rid];
                
                match self.page_ranges[base_addr.range].read_base_record(base_addr.page, base_addr.offset) {
                    Ok(columns) => {
                        println!("[DEBUG] Columns are {:?}", columns);

                        // Check if we should look for a tail record
                        match columns[columns.len() - 2] {
                            Some(tail_rid) => {
                                // Grab the tail record
                                // TODO - Make sure this doesn't crash on 32-bit systems, where usize may not be 64 bits
                                let tail_addr = self.page_directory[&(tail_rid as usize)];

                                match self.page_ranges[base_addr.range].read_tail_record(tail_addr.page, tail_addr.offset) {
                                    Ok(columns) => {
                                        println!("[DEBUG] Tail cols are {:?}", columns);
                                        Ok(columns)
                                    },
                                    Err(_) => panic!("[ERROR] Failed to read most recent tail record.")
                                }
                            },

                            None => Ok(columns) // No updates - return the base record as-is
                        }
                    },

                    Err(_) => panic!("[DEBUG] Failed to read base record.")
                }
            },

            None => Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Record with identifier {} doesn't exist.", primary_key),
            ))
        }
    }

    /// Create a new **base record**.
    pub fn insert(&mut self, columns: Vec<i64>) -> PyResult<()> {
        // Some functions take a vector of optionals rather than integers because updates use `None`
        // to signal that a value isn't updated. However, we want to require that all columns are
        // provided for _new_ records. For this reason, we wrap them inside `Some` here.
        let columns_wrapped: Vec<Option<i64>> = columns.iter().map(|val| Some(*val)).collect();

        if columns.len() < self.num_columns {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Table has {} columns, but only {} were provided.", self.num_columns, columns.len()),
            ));
        }

        if self.lid_to_rid.get(&columns[self.key_column]).is_some() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Record with identifier {} already exists.", columns[self.key_column]),
            ));
        }

        match self.page_ranges[self.next_page_range].insert_base(&columns_wrapped) {
            Ok((page, offset)) => {
                // Add the new LID to RID mapping
                self.lid_to_rid.insert(columns[self.key_column], self.next_rid);

                // Add the new RID to physical address mapping
                self.page_directory.insert(self.next_rid, BaseAddress::new(self.next_page_range, page, offset));
                
                // Increment the RID for the next record
                self.next_rid += 1;

                Ok(())
            },

            Err(_) => {
                // This page range is full - add new range
                self.page_ranges.push(PageRange::new(self.num_columns, self.buffer_pool_manager.clone()));
                self.next_page_range += 1;

                return self.insert(columns);
            }
        }
    }
}
