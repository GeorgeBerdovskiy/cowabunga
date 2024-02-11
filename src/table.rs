use pyo3::prelude::*;
use pyo3::types::PyTuple;
use std::{collections::HashMap, marker::PhantomData};
use std::sync::{Arc, Mutex};

use crate::constants::*;
use crate::bufferpool::*;
use crate::errors::DatabaseError;
use crate::helpers::*;

/// Empty type representing **base** pages.
#[derive(Clone, Copy)]
struct Base;

/// Empty type representing **tail** pages.
#[derive(Clone, Copy)]
struct Tail;

/// Represents a **logical** base or tail page, depending on the provided generic type argument.
struct LogicalPage<T> {
    /// Vector of **page identifiers** used by the buffer pool manager.
    columns: Vec<PageIdentifier>,

    /// Buffer pool manager shared by all tables.
    buffer_pool_manager: Arc<Mutex<BufferPool>>,

    /// Phantom field for the generic type argument (required since none of the other fields actually use `T`).
    phantom: PhantomData<T>
}

/// Methods for all logical pages.
impl<T> LogicalPage<T> {
    /// Create a new logical page with `num_columns` columns and a shared buffer pool manager.
    pub fn new(num_columns: usize, buffer_pool_manager: Arc<Mutex<BufferPool>>) -> LogicalPage<T> {
        LogicalPage {
            columns: buffer_pool_manager.clone().lock().unwrap().allocate_pages(num_columns),
            buffer_pool_manager,
            phantom: PhantomData::<T>
        }
    }

    /// Read from every column in this logical page given an offset.
    pub fn read(&self, offset: Offset) -> Result<Vec<Option<i64>>, DatabaseError>{
        self.columns.iter()
            .map(|column| self.buffer_pool_manager.lock().unwrap().read(*column, offset))
            .collect()
    }
}

/// Methods for logical **base** pages.
impl LogicalPage<Base> {
    /// Insert a new **base** record given a vector of columns. Returns the offset of this record
    /// on a physical page if successful or `Err(...)` if the physical page has no more space.
    pub fn insert(&mut self, columns: &Vec<Option<i64>>) -> Result<Offset, DatabaseError> {
        let mut offset = 0;

        // This adds the user's columns, but _not_ the metadata columns
        for (user_column, column_value) in self.columns.iter().zip(columns.iter()) {
            offset = self.buffer_pool_manager.lock().unwrap().write_next(*user_column, *column_value)?;
        }

        // Write the indirection column, which is last
		self.buffer_pool_manager.lock().unwrap().write_next(self.columns[self.columns.len() - 1], None)?;

        Ok(offset)
    }

    /// Updates the indirection column of a base record.
    pub fn update_indirection(&mut self, offset: Offset, new_rid: RID) -> Result<(), DatabaseError> {
        // The columns are _ | _ | ... | INDIRECTION
        let indirection_column = self.columns[self.columns.len() - 1];
        self.buffer_pool_manager.lock().unwrap().write(indirection_column, offset, Some(new_rid as i64))
    }
}

/// Methods for logical **tail** pages
impl LogicalPage<Tail> {
    /// Insert a new **tail** record given a vector of columns.
    pub fn insert(&mut self, columns: &Vec<Option<i64>>, indirection: Option<i64>) -> Result<Offset, DatabaseError> {
        let mut offset = 0;

        // Iterate over columns and write their values, excluding the last one for special handling
        for (&user_column, &column_value) in columns.iter().zip(self.columns.iter().take(self.columns.len() - 1)) {
            offset = self.buffer_pool_manager.lock().unwrap().write_next(column_value, user_column)?;
        }

        self.buffer_pool_manager.lock().unwrap().write_next(self.columns[self.columns.len() - 1], indirection)?;

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

    /// Shared buffer pool manager.
    buffer_pool_manager: Arc<Mutex<BufferPool>>
}

impl PageRange {
    /// Create a new page range given the number of columns and a shared buffer pool manager.
    pub fn new(num_columns: usize, buffer_pool_manager: Arc<Mutex<BufferPool>>) -> Self {
        let base_page_vec = (0..BASE_PAGES_PER_RANGE)
            .map(|_| LogicalPage::new(num_columns, buffer_pool_manager.clone()))
            .collect();

        PageRange {
            base_pages: base_page_vec,
            tail_pages: vec![LogicalPage::<Tail>::new(num_columns, buffer_pool_manager.clone())],
            next_base_page: 0,
            num_columns,
            buffer_pool_manager: buffer_pool_manager.clone()
        }
    }

    /// Read an entire base record given the page index and physical offset.
    pub fn read_base_record(&mut self, page: usize, offset: Offset) -> Result<Vec<Option<i64>>, DatabaseError> {
        self.base_pages[page].read(offset)
    }

    /// Read an entire tail record given the page index and physical offset.
    pub fn read_tail_record(&mut self, page: usize, offset: Offset) -> Result<Vec<Option<i64>>, DatabaseError> {
        self.tail_pages[page].read(offset)
    }

    /// Update the indirection column of a base record given its address and the new RID.
    pub fn update_base_indirection(&mut self, address: Address, new_rid: RID) -> Result<(), DatabaseError> {
        self.base_pages[address.page].update_indirection(address.offset, new_rid)
    }

    /// Insert a tail record into this page range. Returns the logical page index and physical offset.
    pub fn insert_tail(&mut self, columns: &Vec<Option<i64>>, indirection: Option<i64>) -> (usize, Offset) {
        let next_tail_page = self.tail_pages.len() - 1;

        match self.tail_pages[next_tail_page].insert(&columns, indirection) {
            Ok(offset) => (next_tail_page, offset),
            Err(_) => {
                // Add a new tail page and try to insert again
                self.tail_pages.push(LogicalPage::new(self.num_columns, self.buffer_pool_manager.clone()));

                // Recursively insert which will have at most one level of recursion
                return self.insert_tail(columns, indirection);
            }
        }
    }

    /// Insert a base record into this page range. Returns the logical page index and physical offset if successful
    /// and an error otherwise.
    pub fn insert_base(&mut self, columns: &Vec<Option<i64>>) -> Result<(usize, Offset), DatabaseError> {
        if self.next_base_page >= BASE_PAGES_PER_RANGE {
            return Err(DatabaseError::PageRangeFilled);
        }

        // We still have at least one base page left to fill... try to insert record
        match self.base_pages[self.next_base_page].insert(&columns) {
            Ok(offset) => Ok((self.next_base_page, offset)),
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
#[derive(Clone, Copy)]
struct Address {
    /// Page range index.
    range: usize,

    /// Logical base page index.
    page: usize,

    /// Physical page offset.
    offset: Offset
}

impl Address {
    // Create a new base address.
    pub fn new(range: usize, page: usize, offset: usize) -> Self {
        Address {range, page, offset }
    }
}

#[pyclass]
pub struct PyRecord {
    #[pyo3(get)]
    pub rid: RID,
    #[pyo3(get)]
    pub key: LID,
    #[pyo3(get)]
    pub columns: Vec<Option<i64>>
}

#[pymethods]
impl PyRecord {
    #[new]
    pub fn new(rid: RID, key: LID, columns: Vec<Option<i64>>) -> Self {
        PyRecord { rid, key, columns }
    }
}

/// Represents a table and is exposed by PyO3.
#[pyclass]
pub struct Table {
    /// Name of the table.
    #[pyo3(get)]
    pub name: String,

    /// Number of columns.
    #[pyo3(get)]
    pub num_columns: usize,

    /// Index of the primary key column.
    key_column: usize,

    /// Next available RID.
    next_rid: usize,

    /// Page ranges associated with this table. Note that it's expanded _dynamically_.
    page_ranges: Vec<PageRange>,

    /// Page directory - maps from RIDs to base record addresses.
    page_directory: HashMap<RID, Address>,

    /// Maps LIDs to RIDs.
    lid_to_rid: HashMap<LID, RID>,

    /// Index of the next available page range.
    next_page_range: usize,

    /// Buffer pool manager shared by all tables.
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

            // The columns are _ | _ | ... | INDIRECTION
            page_ranges: vec![PageRange::new(num_columns + NUM_METADATA_COLS, buffer_pool_manager.clone())],
            page_directory: HashMap::new(),
            lid_to_rid: HashMap::new(),
            next_page_range: 0,
            buffer_pool_manager
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
                self.page_directory.insert(self.next_rid, Address::new(self.next_page_range, page, offset));
                
                // Increment the RID for the next record
                self.next_rid += 1;

                Ok(())
            },

            Err(_) => {
                // This page range is full - add new range
                self.page_ranges.push(PageRange::new(self.num_columns + NUM_METADATA_COLS, self.buffer_pool_manager.clone()));
                self.next_page_range += 1;

                return self.insert(columns);
            }
        }
    }

    /// Update an existing record (in other words, insert a **tail record**). Note that we are using the **cumulative** update scheme.
    pub fn update(&mut self, key: i64, columns: Vec<Option<i64>>) -> PyResult<()> {
        if columns.len() < self.num_columns {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Table has {} columns, but only {} were provided.", self.num_columns, columns.len()),
            ));
        }

        let key_value = key;

        if self.lid_to_rid.get(&key_value).is_none() {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("Record with identifier {} doesn't exist.", key_value),
            ));
        }

        let base_rid = self.lid_to_rid[&key_value];
        let base_address = self.page_directory[&base_rid];

		// Since we're using the cumulative update scheme, we need to grab the remaining values first
		let mut cumulative_columns: Vec<Option<i64>> = vec![None; self.num_columns + NUM_METADATA_COLS];

        // Grab the base page because we need to check the indirection column
        match self.page_ranges[base_address.range].read_base_record(base_address.page, base_address.offset) {
            Ok(base_columns) => {
                let indirection = base_columns[base_columns.len() - 1];

				if indirection.is_some() {
					// We need to grab the last tail record
					let tail_rid = indirection.unwrap();
					let tail_address = self.page_directory[&(tail_rid as usize)];

					match self.page_ranges[tail_address.range].read_tail_record(tail_address.page, tail_address.offset) {
						Ok(tail_columns) => {
							// We've got the tail columns - let's combine them with the requested updates
							for (update, (original, target)) in columns.iter().zip(tail_columns.iter().zip(cumulative_columns.iter_mut())) {
								if update.is_none() {
									// This column isn't being updated, so use the original value
									*target = *original;
								} else {
									// This column is being updated, so use the updated value
									*target = *update;
								}
							}
						},

						Err(error) => {
							// We couldn't access the tail record for some reason. For now, return an error. Later, default
							// to using the base columns and generate a warning or something like that.
							return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
								format!("Failed to retrieve tail record."),
							));
						}
					}
				} else {
					// This is our first update, so columns that aren't updated should come from `base_columns`
					for (update, (original, target)) in columns.iter().zip(base_columns.iter().zip(cumulative_columns.iter_mut())) {
						if update.is_none() {
							// This column isn't being updated, so use the original value
							*target = *original;
						} else {
							// This column is being updated, so use the updated value
							*target = *update;
						}
					}
				}

				// At this point, `cumulative_columns` contains all of our changes and original data that wasn't updated
				let indirection_or_base_rid = indirection.unwrap_or(base_rid as i64);
                let (page, offset) = self.page_ranges[base_address.range].insert_tail(&cumulative_columns, Some(indirection_or_base_rid));

                // Add the new RID to physical address mapping
                self.page_directory.insert(self.next_rid, Address::new(base_address.range, page, offset));
        
                // Update the base record indirection column
                self.page_ranges[base_address.range].update_base_indirection(base_address, self.next_rid)
                    .map_err(|_| PyErr::new::<pyo3::exceptions::PyValueError, _>("Failed to update base record indirection column."))?;

                // Increment the RID for the next record
                self.next_rid += 1;
        
                Ok(())
            },

            Err(_) => panic!("[ERROR] Failed to grab base record.")
        }
    }

	pub fn print(&self) -> PyResult<()> {
		self.buffer_pool_manager.lock().unwrap().print_all();
        Ok(())
	}

    /// Select the most recent version of a record given its primary key.
    pub fn select(&mut self, search_key: i64, search_key_index: usize, projected_columns: Vec<i8>) -> PyResult<Vec<PyRecord>> {
		// TODO - Right now, this assumes we always use the primary key. That's wrong - fix it later
		let rid = self.lid_to_rid[&search_key];
		let base_address = self.page_directory[&rid];

		// First, get the base record
		match self.page_ranges[base_address.range].read_base_record(base_address.page, base_address.offset) {
			Ok(base_columns) => {
				// Check if we have a most recent tail record
				if base_columns[base_columns.len() - 1].is_none() {
					// There is no record more recent than this one! Return it

					// TODO - See if there is a more "efficient" way of doing this, because I'm pretty sure `into_iter` isn't cheap
                    // TODO: is this the correct rid to use?

                    let ret_rec = PyRecord::new(rid, search_key, base_columns.into_iter().take(self.num_columns).collect());
					return Ok(vec![ret_rec]);
				}

				// We DO have a most recent tail record - let's find it!
				let tail_rid = base_columns[base_columns.len() - 1].unwrap();
				let tail_address = self.page_directory[&(tail_rid as usize)];

				match self.page_ranges[tail_address.range].read_tail_record(tail_address.page, tail_address.offset) {
					Ok(tail_columns) => {
                        let ret_rec = PyRecord::new(rid, search_key, tail_columns.into_iter().take(self.num_columns).collect());
                        return Ok(vec![ret_rec]);
                    },
					Err(_) => {
						// Do nothing for now
					}
				}
			},

			Err(_) => {
				// Do nothing for now
			}
		}

        // if the above failed, just return empty Vec.
		Ok(vec![])
    }
    pub fn select_version(&mut self, search_key: i64, search_key_index: usize, projected_columns: Vec<i8>, relative_version:i64) -> PyResult<Vec<PyRecord>> {
        let rid = self.lid_to_rid[&search_key];
		let base_address = self.page_directory[&rid];
        let mut version = -1;

        match self.page_ranges[base_address.range].read_base_record(base_address.page, base_address.offset) {
			Ok(base_columns) => {
				// Check if we have a most recent tail record
				if base_columns[base_columns.len() - 1].is_none() {
					// There is no record more recent than this one! Return it

					// TODO - See if there is a more "efficient" way of doing this, because I'm pretty sure `into_iter` isn't cheap
                    // TODO: is this the correct rid to use?

                    let ret_rec = PyRecord::new(rid, search_key, base_columns.into_iter().take(self.num_columns).collect());
					return Ok(vec![ret_rec]);
				}
				// We DO have a most recent tail record - let's find it!
                let mut tail_rid = base_columns[base_columns.len() - 1].unwrap() as usize;
                let mut record = base_columns;
                let mut ret_rec = PyRecord::new(rid, search_key, record.into_iter().take(self.num_columns).collect());


                while version > relative_version {
                    if tail_rid == rid {
                        return Ok(vec![ret_rec])
                    }

                    let tail_address = self.page_directory[&tail_rid];
                    
                    match self.page_ranges[tail_address.range].read_tail_record(tail_address.page, tail_address.offset) {
                        Ok(tail_columns) => {
                            tail_rid = tail_columns[tail_columns.len() - 1].unwrap() as usize;
                            record = tail_columns;

                        },
                        Err(_) => {
                            panic!("Error looping through tail rids for select_version");
                        }
                    }

                    version -= 1;

				}
                ret_rec = PyRecord::new(rid, search_key, record.into_iter().take(self.num_columns).collect());
                Ok(vec![ret_rec])
			},
            Err(_) => {
                return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                    format!("Error in select_version"),
                ));
            }
            
        }
    }
}
