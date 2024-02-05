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
}

impl<Base> LogicalPage<Base> {
    /// Insert a new **base** record given a vector of columns. Returns the offset of this record
    /// on a physical page if successful or `Err(...)` if the physical page has no more space.
    pub fn insert(&mut self, columns: &Vec<Option<i64>>) -> Result<usize, ()> {
        let mut offset = 0;

        for pair in self.columns.iter().zip(columns.iter()) {
            match self.buffer_pool_manager.lock().unwrap().write_next(*pair.0, *pair.1) {
                Ok(returned_offset) => offset = returned_offset,
                Err(error) => return Err(error)
            }
        }

        Ok(offset)
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
            num_columns,
            key_column,
            next_rid: 0,
            page_ranges: vec![PageRange::new(num_columns, buffer_pool_manager.clone())],
            page_directory: HashMap::new(),
            lid_to_rid: HashMap::new(),
            next_page_range: 0,
            buffer_pool_manager: buffer_pool_manager
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

        // NOTE - This will crash if the value in columns[self.key_column] is `None`
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
