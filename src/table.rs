use pyo3::prelude::*;

use crate::bufferpool::*;
use crate::constants::*;
use crate::errors::DatabaseError;
use crate::persistables::*;
use serde::{Deserialize, Serialize};
use core::panic;
use std::collections::{BTreeMap, HashSet, HashMap};
use std::marker::PhantomData;
use std::fs::{OpenOptions, File};
use std::io::{Read, Write};
use std::ops::Bound::Included;
use std::sync::atomic::AtomicUsize;
use std::sync::mpsc::{self, Sender};
use std::sync::{Arc, Mutex};
use std::thread;

use once_cell::sync::Lazy;

static BPM: Lazy<BufferPool> = Lazy::new(|| {
    // This block is run only once to initialize the instance of BufferPool
    BufferPool::new()
});

/// Zero sized struct representing **base** pages.
#[derive(Clone, Copy, Debug)]
struct Base;

/// Zero sized struct representing **tail** pages.
#[derive(Clone, Copy, Debug)]
struct Tail;

/// Represents a **logical** base or tail page, depending on the provided generic type argument.
#[derive(Clone, Debug)]
struct LogicalPage<T> {
    /// Vector of **physical page identifiers** used by the buffer pool manager.
    columns: Vec<PhysicalPageID>,

    /// Buffer pool manager shared by all tables.
    buffer_pool_manager: &'static BufferPool,

    /// Phantom field for the generic type argument (required since none of the other fields actually use `T`).
    phantom: PhantomData<T>,
}

/// Methods for all logical pages.
impl<T> LogicalPage<T> {
    /// Create a new logical page with `num_columns` columns and a shared buffer pool manager.
    pub fn new(table_id: usize, num_columns: usize, buffer_pool_manager: &'static BufferPool) -> LogicalPage<T> {
        LogicalPage {
            columns: buffer_pool_manager.allocate_pages(table_id, num_columns),
            buffer_pool_manager,
            phantom: PhantomData::<T>,
        }
    }

    /// Read from every column in this logical page given an offset.
    pub fn read(&self, offset: Offset, projection: &Vec<usize>) -> Result<Vec<Option<i64>>, DatabaseError> {
        let mut result = Vec::new();

        for i in 0..projection.len() {
            if projection[i] == 0 {
                continue;
            }

            result.push(
                self.buffer_pool_manager.read(self.columns[i], offset)?,
            );
        }

        Ok(result)
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
            offset = self
                .buffer_pool_manager.write_next_value(*user_column, *column_value)?;
        }

        // Write the indirection column, which is last
        //self.buffer_pool_manager.lock().unwrap().write_next_value(self.columns[self.columns.len() - 1], None)?;

        Ok(offset)
    }

    /// Updates the indirection column of a base record.
    pub fn update_indirection(&mut self, offset: Offset, new_rid: RID) -> Result<(), DatabaseError> {
        // The columns are _ | _ | ... | INDIRECTION
        let indirection_column = self.columns[self.columns.len() - 1];
        self.buffer_pool_manager.write_value(indirection_column, offset, Some(new_rid as i64))
    }
}

/// Methods for logical **tail** pages
impl LogicalPage<Tail> {
    /// Insert a new **tail** record given a vector of columns.
    pub fn insert(&mut self, columns: &Vec<Option<i64>>, indirection: Option<i64>) -> Result<Offset, DatabaseError> {
        let mut offset = 0;

        // Iterate over columns and write their values, excluding the last one for special handling
        for (&user_column, &column_value) in columns
            .iter()
            .zip(self.columns.iter().take(self.columns.len() - 1))
        {
            offset = self.buffer_pool_manager
                .write_next_value(column_value, user_column)?;
        }

        self.buffer_pool_manager
            .write_next_value(self.columns[self.columns.len() - 1], indirection)?;

        Ok(offset)
    }
}

/// Represents a page range. Consists of a set of base pages (which should have a set maximum
/// size) and a set of tail pages (which is unbounded).
struct PageRange {
    /// The identifier of the table this page range belongs to.
    table_identifier: usize,

    /// The set of base pages associated with this page range. Whenever we write to this vector,
    /// we ensure that its length doesn't exceed `BASE_PAGES_PER_RANGE` (defined in `constants.rs`).
    pub base_pages: Vec<LogicalPage<Base>>,

    /// The set of tail pages associated with this page range. It's unbounded, so no checks
    /// on its length are necessary.
    tail_pages: Vec<LogicalPage<Tail>>,

    /// Index of the next base page to which we can write. If it ever becomes `BASE_PAGES_PER_RANGE`,
    /// this page range cannot accept any more base records.
    next_base_page: usize,

    /// Number of columns in the table.
    num_columns: usize,

    /// Shared buffer pool manager.
    buffer_pool_manager: &'static BufferPool,

    num_updates: usize,

    pub tps: Arc<AtomicUsize>,
}

impl PageRange {
    /// Create a new page range given the number of columns and a shared buffer pool manager.
    pub fn new(table_identifier: usize, num_columns: usize, buffer_pool_manager: &'static BufferPool) -> Self {
        let base_page_vec = (0..BASE_PAGES_PER_RANGE)
            .map(|_| LogicalPage::new(table_identifier, num_columns, buffer_pool_manager))
            .collect();

        PageRange {
            table_identifier,
            base_pages: base_page_vec,
            tail_pages: vec![LogicalPage::<Tail>::new(
                table_identifier,
                num_columns,
                buffer_pool_manager,
            )],
            next_base_page: 0,
            num_columns,
            buffer_pool_manager,
            num_updates: 0,
            tps: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Read an entire base record given the page index and physical offset.
    pub fn read_base_record(&self, page: usize, offset: Offset, projection: &Vec<usize>) -> Result<Vec<Option<i64>>, DatabaseError> {
        self.base_pages[page].read(offset, projection)
    }

    /// Read an entire tail record given the page index and physical offset.
    pub fn read_tail_record(&self, page: usize, offset: Offset, projection: &Vec<usize>) -> Result<Vec<Option<i64>>, DatabaseError> {
        self.tail_pages[page].read(offset, projection)
    }

    /// Update the indirection column of a base record given its address and the new RID.
    pub fn update_base_indirection(&mut self, address: Address, new_rid: RID) -> Result<(), DatabaseError> {
        self.base_pages[address.page].update_indirection(address.offset, new_rid)
    }

    /// Insert a tail record into this page range. Returns the logical page index and physical offset.
    pub fn insert_tail(&mut self, columns: &Vec<Option<i64>>, indirection: Option<i64>) -> (usize, Offset) {
        let next_tail_page = self.tail_pages.len() - 1;

        match self.tail_pages[next_tail_page].insert(&columns, indirection) {
            Ok(offset) => {
                self.num_updates += 1;
                (next_tail_page, offset)
            },

            Err(_) => {
                // Add a new tail page and try to insert again
                self.tail_pages.push(LogicalPage::new(
                    self.table_identifier,
                    self.num_columns,
                    self.buffer_pool_manager,
                ));

                // Recursively insert which will have at most one level of recursion
                return self.insert_tail(columns, indirection);
            }
        }
    }

    /// Insert a base record into this page range. Returns the logical page index and physical offset if successful
    /// and an error otherwise.
    pub fn insert_base(&mut self,columns: &Vec<Option<i64>>) -> Result<(usize, Offset), DatabaseError> {
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
                self.base_pages.push(LogicalPage::new(
                    self.table_identifier,
                    self.num_columns,
                    self.buffer_pool_manager,
                ));

                // Note that although this call is recursive, it will have a depth of at most one
                return self.insert_base(columns);
            }
        }
    }
}

/// Represents the _record_ identifier.
pub type RID = usize;

/// Represents the _logical_ identifier.
pub type LID = i64;

/// Represents the address of a record. We obtain this address from the page directory,
/// which maps from RIDs to physical addresses.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct Address {
    /// Page range index.
    range: usize,

    /// Logical page index.
    page: usize,

    /// Physical page offset.
    offset: Offset,
}

impl Address {
    // Create a new base address.
    pub fn new(range: usize, page: usize, offset: usize) -> Self {
        Address { range, page, offset }
    }
}

#[pyclass]
pub struct PyRecord {
    #[pyo3(get)]
    pub rid: RID,
    #[pyo3(get)]
    pub key: LID,
    #[pyo3(get)]
    pub columns: Vec<Option<i64>>,
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
    /// Directory holding all the tables
    directory: String,

    /// Name of the table.
    #[pyo3(get)]
    pub name: String,

    /// Identifier of the table. This is determined by the buffer pool manager
    table_identifier: usize,

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

    /// Index of the next available page range.
    next_page_range: usize,

    /// Buffer pool manager shared by all tables.
    buffer_pool_manager: &'static BufferPool,

    /// B-Tree indexes on all columns (except metadata).
    indexer: Indexer,

    /// List of "dead" RIDs... in other words, RIDs belonging to deleted records. This
    /// list tells the merge operation what addresses may be deallocated.
    dead_rids: Vec<RID>,

    /// Sender channel used to send merge requests.
    merge_sender: Option<Sender<MergeRequest>>,
}

/// Represents the indexer of a table.
#[derive(Clone, Serialize, Deserialize)]
pub struct Indexer {
    /// If enabled[i] is `false`, the index for column `i` is considered dropped.
    enabled: Vec<bool>,

    /// B-Tree indexes for every column except for metadata columns.
    b_trees: Vec<BTreeMap<i64, HashSet<RID>>>,
}

impl Indexer {
    /// Initialize a new indexer.
    pub fn new(num_columns: usize) -> Self {
        // The default is that no index exists for any column, except for the primary key

        let mut enabled_vec = vec![false; num_columns];
        enabled_vec[0] = true;

        Indexer {
            enabled: enabled_vec,
            b_trees: vec![BTreeMap::new(); num_columns],
        }
    }

    /// Inserts a key, RID pair. If the key is already present in the tree, add the RID
    /// to its hash set. Otherwise, create a new hashset with only the RID present.
    /// NOTE - This function should only be called on completely filled rows (or, rows without
    /// `None` values) so it should be safe to unwrap the values inside `columns`
    pub fn insert(&mut self, columns: &Vec<i64>, rid: RID) {
        for (column_value, tree) in columns.iter().zip(self.b_trees.iter_mut()) {
            if tree.contains_key(&column_value) {
                tree.get_mut(&column_value).unwrap().insert(rid);
            } else {
                // Doesn't contain the key yet - insert
                tree.insert(*column_value, HashSet::from([rid]));
            }
        }
    }

    /// Adds a value to a column.
    pub fn add_column_index(&mut self, value: i64, column: usize, base_rid: RID) {
        if self.b_trees[column].contains_key(&value) {
            self.b_trees[column]
                .get_mut(&value)
                .unwrap()
                .insert(base_rid);
        } else {
            // Doesn't contain the key yet - insert
            self.b_trees[column].insert(value, HashSet::from([base_rid]));
        }
    }

    /// Update the index of a column given a RID, the original value, and the new value. This will delete
    /// (original, RID) from the corresponding b-tree and add (update, RID) to the b-tree.
    pub fn update_column_index(&mut self, original: i64, update: i64, column: usize, base_rid: RID) {
        // Delete the old pair
        self.b_trees[column]
            .get_mut(&original)
            .unwrap()
            .remove(&base_rid);

        // Now add the RID to the correct hash set
        self.add_column_index(update, column, base_rid);
    }

    /// Given a start key, an end key, and a column index, return all of the RIDs stored under
    /// that range of keys
    fn locate_range(&self, start_key: i64, end_key: i64, column: usize) -> Vec<RID> {
        let mut result = Vec::new();
        for (_, value) in self.b_trees[column].range((Included(&start_key), Included(&end_key))) {
            result.extend(value);
        }

        result
    }

    pub fn remove_from_index(&mut self, columns: Vec<Option<i64>>, rid: RID) {
        for (column_value, tree) in columns.iter().zip(self.b_trees.iter_mut()) {
            if column_value.is_some() {
                match tree.get_mut(&column_value.unwrap()) {
                    Some(map) => {
                        map.remove(&rid);
                    }
                    None => continue,
                };
            }
        }
    }
}

/// Represents a merge request. Sent through the merge sender channel
/// when a page range reaches some update threshold.
struct MergeRequest {
    /// Copy of logical base pages sent w/merge request
    base_pages: Vec<LogicalPage<Base>>,

    /// Copy of logical tail pages sent w/merge request
    tail_pages: Vec<LogicalPage<Tail>>,

    /// Current TPS value (atomic so we can swap it safely accross threads)
    tps: Arc<AtomicUsize>,

    /// Page directory (sent w/merge request due to some complications
    /// with Rust ownership rules and threads)
    page_directory: HashMap<RID, Address>,
}

impl MergeRequest {
    pub fn new(
        base_pages: &Vec<LogicalPage<Base>>,
        tail_pages: &Vec<LogicalPage<Tail>>,
        tps: &Arc<AtomicUsize>,
        page_directory: &HashMap<RID, Address>,
    ) -> Self {
        MergeRequest {
            base_pages: base_pages.clone(),
            tail_pages: tail_pages.clone(),
            tps: tps.clone(),
            page_directory: page_directory.clone(),
        }
    }
}

#[pymethods]
impl Table {
    /// Create a new table given its name, number of columns, primary key column index, and shared
    /// buffer pool manager.
    #[new]
    //pub fn new(directory: String, name: String, num_columns: usize, key_column: usize, buffer_pool_manager: &'static BufferPool)
    pub fn new(directory: String, name: String, num_columns: usize, key_column: usize, is_load: bool) -> Self {
        println!("DEBUG: about to access BPM");

        BPM.set_directory(is_load, &directory as &str);
        println!("DEBUG: trying to register table");
        // TODO problem here
        let table_identifier = BPM.register_table_name(&name);

        println!("DEBUG: set bpm dir");

        // Create the table directory, every column file inside that table, and the header file for every column
        match std::fs::create_dir(format!("{}/{}", directory, table_identifier)) {
            Ok(_) => {
                // This is a completely new table, which means we need to create all its associated files
                for i in 0..num_columns + NUM_METADATA_COLS {
                    OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(format!("{}/{}/{}.dat", directory, table_identifier, i))
                        .unwrap();

                    let col_hdr = ColumnPeristable { next_page_index: 0 };

                    let col_hdr_serialized = serde_json::to_string(&col_hdr).unwrap();

                    let mut col_hdr_file = OpenOptions::new()
                        .write(true)
                        .create_new(true)
                        .open(format!("{}/{}/{}.hdr", directory, table_identifier, i))
                        .unwrap();

                    let _result = col_hdr_file.write(col_hdr_serialized.as_bytes());
                }

                let page_ranges = vec![PageRange::new(
                    table_identifier,
                    num_columns + NUM_METADATA_COLS,
                    &BPM,
                )];
                let merge_sender = start_merge_thread(num_columns, &BPM);
                return Table {
                    directory,
                    name,
                    table_identifier,
                    num_columns,
                    key_column,
                    next_rid: 0,

                    // The columns are _ | _ | ... | INDIRECTION
                    page_ranges: page_ranges,
                    page_directory: HashMap::new(),
                    next_page_range: 0,
                    buffer_pool_manager: &BPM,

                    indexer: Indexer::new(num_columns),
                    dead_rids: vec![],
                    merge_sender,
                };
            }

            Err(error) => {
                println!("{:?}", error);

                // Table files already exist - load from those disk
                // Also disregard the `num_columns` and `key_column` arguments
                let metadata_path = format!("{}/{}/table.hdr", directory, table_identifier);
                let mut metadata_file = File::open(metadata_path).unwrap();

                let mut metadata_string = String::new();
                let _result = metadata_file.read_to_string(&mut metadata_string);

                let metadata: TableMetadata = serde_json::from_str(&metadata_string).unwrap();

                let merge_sender = start_merge_thread(metadata.num_columns, &BPM);
                return Table {
                    directory,
                    name,
                    table_identifier: metadata.table_identifier,
                    num_columns: metadata.num_columns,
                    key_column: metadata.key_column,
                    next_rid: metadata.next_rid,
                    page_ranges: metadata
                        .page_ranges
                        .iter()
                        .map(|serialized_range| PageRange {
                            table_identifier,
                            base_pages: serialized_range
                                .base_pages
                                .iter()
                                .map(|serialized_base_page| LogicalPage {
                                    columns: serialized_base_page.columns.clone(),
                                    buffer_pool_manager: &BPM,
                                    phantom: PhantomData::<Base>,
                                })
                                .collect(),
                            tail_pages: serialized_range
                                .tail_pages
                                .iter()
                                .map(|serialized_tail_page| LogicalPage {
                                    columns: serialized_tail_page.columns.clone(),
                                    buffer_pool_manager: &BPM,
                                    phantom: PhantomData::<Tail>,
                                })
                                .collect(),
                            next_base_page: serialized_range.next_base_page,
                            num_columns: metadata.num_columns + NUM_METADATA_COLS,
                            buffer_pool_manager: &BPM,
                            num_updates: 0,
                            tps: Arc::new(AtomicUsize::new(serialized_range.tps)),
                        })
                        .collect(),
                    page_directory: metadata.page_directory,
                    next_page_range: metadata.next_page_range,
                    buffer_pool_manager: &BPM,
                    indexer: metadata.indexer,
                    dead_rids: vec![],
                    merge_sender,
                };
            }
        };
    }


    /// Persist table metadata onto disk
    pub fn persist(&self) {
        // First, collect everything into the metadata struct
        let metadata = TableMetadata {
            name: self.name.clone(),
            table_identifier: self.table_identifier,
            num_columns: self.num_columns,
            key_column: self.key_column,
            next_rid: self.next_rid,
            page_ranges: self
                .page_ranges
                .iter()
                .map(|page_range| PageRangePersistable {
                    base_pages: page_range
                        .base_pages
                        .iter()
                        .map(|base_page| LogicalPagePersistable {
                            columns: base_page.columns.clone(),
                        })
                        .collect(),
                    tail_pages: page_range
                        .tail_pages
                        .iter()
                        .map(|tail_page| LogicalPagePersistable {
                            columns: tail_page.columns.clone(),
                        })
                        .collect(),
                    next_base_page: page_range.next_base_page,
                    tps: page_range.tps.load(std::sync::atomic::Ordering::Relaxed),
                })
                .collect(),
            page_directory: self.page_directory.clone(),
            next_page_range: self.next_page_range,
            indexer: self.indexer.clone(),
        };

        let serialized_metadata = serde_json::to_string(&metadata).unwrap();

        let metadata_path = format!("{}/{}/table.hdr", self.directory, self.table_identifier);

        let mut metadata_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(metadata_path)
            .unwrap();

        metadata_file.write(serialized_metadata.as_bytes());
    }

    /// Create a new **base record**.
    pub fn insert(&mut self, columns: Vec<i64>) -> bool {
        // Some functions take a vector of optionals rather than integers because updates use `None`
        // to signal that a value isn't updated. However, we want to require that all columns are
        // provided for _new_ records. For this reason, we wrap them inside `Some` here.
        let mut columns_wrapped: Vec<Option<i64>> = columns.iter().map(|val| Some(*val)).collect();

        // Preemtively add the RID of the tail record copy we will add later
        columns_wrapped.push(Some((self.next_rid) as i64));

        if columns.len() < self.num_columns {
            return false;
        }

        let matching_rids = self.indexer.locate_range(
            columns[self.key_column],
            columns[self.key_column],
            self.key_column,
        );

        if matching_rids.len() != 0 {
            return false;
        }

        match self.page_ranges[self.next_page_range].insert_base(&columns_wrapped) {
            Ok((page, offset)) => {
                // Add the new RID to physical address mapping
                self.page_directory.insert(
                    self.next_rid,
                    Address::new(self.next_page_range, page, offset),
                );

                self.indexer.insert(&columns, self.next_rid);

                // Increment the RID for the next record
                self.next_rid += 1;

                // Also add the tail record corresponding to this base record
                let _result = self.update(
                    columns_wrapped[self.key_column].unwrap(),
                    columns_wrapped.into_iter().take(self.num_columns).collect(),
                );

                // Ok(())
                return true;
            },

            Err(_) => {
                // This page range is full - add new range
                self.page_ranges.push(PageRange::new(
                    self.table_identifier,
                    self.num_columns + NUM_METADATA_COLS,
                    self.buffer_pool_manager,
                ));
                self.next_page_range += 1;

                return self.insert(columns);
            }
        }
    }

    /// Update an existing record (in other words, insert a **tail record**). Note that we are using the **cumulative** update scheme.
    pub fn update(&mut self, key: i64, columns: Vec<Option<i64>>) -> bool {
        if columns.len() < self.num_columns {
            // Project specifies that we should return `false` when something goes wrong
            return false;
        }

        let key_value = key;
        let matching_rids = self
            .indexer
            .locate_range(key_value, key_value, self.key_column);

        if matching_rids.len() == 0 {
            // Project specifies that we should return `false` when something goes wrong
            return false;
        }

        let base_rid = matching_rids[0];
        let base_address = self.page_directory[&base_rid];

        // Since we're using the cumulative update scheme, we need to grab the remaining values first
        let mut cumulative_columns: Vec<Option<i64>> = vec![None; self.num_columns + NUM_METADATA_COLS];

        // Grab the base page because we need to check the indirection column
        match self.page_ranges[base_address.range].read_base_record(
            base_address.page,
            base_address.offset,
            &vec![1; self.num_columns + NUM_METADATA_COLS],
        ) {
            Ok(base_columns) => {
                let indirection = base_columns[base_columns.len() - 1];

                if indirection.is_some() && indirection.unwrap() != base_rid as i64 {
                    // We need to grab the last tail record
                    let tail_rid = indirection.unwrap();
                    let tail_address = self.page_directory[&(tail_rid as usize)];

                    match self.page_ranges[tail_address.range].read_tail_record(
                        tail_address.page,
                        tail_address.offset,
                        &vec![1; self.num_columns + NUM_METADATA_COLS],
                    ) {
                        Ok(tail_columns) => {
                            // We've got the tail columns - let's combine them with the requested updates
                            let mut i = 0;

                            for (update, (original, target)) in columns
                                .iter()
                                .zip(tail_columns.iter().zip(cumulative_columns.iter_mut()))
                            {
                                if update.is_none() {
                                    // This column isn't being updated, so use the original value
                                    *target = *original;
                                } else {
                                    // This column is being updated, so use the updated value
                                    *target = *update;

                                    self.indexer.update_column_index(
                                        original.unwrap(),
                                        update.unwrap(),
                                        i,
                                        base_rid,
                                    );
                                }

                                i += 1;
                            }
                        }

                        Err(_error) => {
                            // We couldn't access the tail record for some reason
                            // Project specifies that we should return `false` when something goes wrong
                            return false;
                        }
                    }
                } else {
                    // TODO - Fix this (mixing indexes with iterators is probably not a good idea)
                    let mut i = 0;

                    // This is our first update, so columns that aren't updated should come from `base_columns`
                    for (update, (original, target)) in columns
                        .iter()
                        .zip(base_columns.iter().zip(cumulative_columns.iter_mut()))
                    {
                        if update.is_none() {
                            // This column isn't being updated, so use the original value
                            *target = *original;
                        } else {
                            // This column is being updated, so use the updated value
                            *target = *update;

                            self.indexer.update_column_index(
                                original.unwrap(),
                                update.unwrap(),
                                i,
                                base_rid,
                            );
                        }

                        i += 1;
                    }
                }

                // At this point, `cumulative_columns` contains all of our changes and original data that wasn't updated
                let indirection_or_base_rid = indirection.unwrap_or(base_rid as i64);
                let (page, offset) = self.page_ranges[base_address.range]
                    .insert_tail(&cumulative_columns, Some(indirection_or_base_rid));

                if self.page_ranges[base_address.range].num_updates >= THRESHOLD {
                    self.page_ranges[base_address.range].num_updates = 0;
                    match &self.merge_sender {
                        Some(sender) => {
                            sender
                                .send(MergeRequest::new(
                                    &self.page_ranges[base_address.range].base_pages.clone(),
                                    &self.page_ranges[base_address.range].tail_pages.clone(),
                                    &self.page_ranges[base_address.range].tps.clone(),
                                    &self.page_directory,
                                ))
                                .unwrap();
                        },

                        None => panic!("[ERROR] Query called before merge thread initialized."),
                    }
                }

                // Add the new RID to physical address mapping
                self.page_directory.insert(
                    self.next_rid,
                    Address::new(base_address.range, page, offset),
                );

                // Update the base record indirection column
                let result = self.page_ranges[base_address.range]
                    .update_base_indirection(base_address, self.next_rid);

                if let Err(_error) = result {
                    // Project specifies that we should return `false` when something goes wrong
                    return false;
                }

                // Increment the RID for the next record
                self.next_rid += 1;

                return true;
            }

            Err(_) => panic!("[ERROR] Failed to grab base record."),
        }
    }

    /// Select the most recent version of a record given its primary key.
    pub fn select(&mut self, search_key: i64, search_key_index: usize, projected_columns: Vec<usize>) -> PyResult<Vec<PyRecord>> {
        let rids = self
            .indexer
            .locate_range(search_key, search_key, search_key_index);
        let mut results: Vec<PyRecord> = Vec::new();

        let mut projected_columns = projected_columns;

        // Add indirection column to projection
        projected_columns.push(1);

        for rid in rids {
            match self.select_by_rid(rid, &projected_columns) {
                Ok(row_vec) => {
                    results.push(PyRecord::new(rid, search_key, row_vec));
                }
                Err(_error) => {
                    // error not yet handled.
                    panic!("Couldn't select.")
                }
            }
        }

        Ok(results)
    }

    /// Given the start and end of an (inclusive) range, find all entries with primary keys
    /// within that range and sum the column at `column_index`.
    pub fn sum(&self, start_range: i64, end_range: i64, column_index: usize) -> PyResult<i64> {
        let rids = self
            .indexer
            .locate_range(start_range, end_range, self.key_column);

        // One hot encoding 🔥🥵
        let mut projection = vec![0; self.num_columns];
        projection[column_index] = 1;
        projection.push(1);

        let mut sum = 0;
        for rid in rids {
            // Grab the value of the specified column in the record identified by `rid`.
            // If it returns `Some(value)`, add the value to the sum. Otherwise, add zero (this
            // technically shouldn't happen).
            sum += match self.select_by_rid(rid, &projection).unwrap()[0] {
                Some(val) => val,
                None => 0,
            };
        }

        Ok(sum)
    }

    /// Given the start and end of an (inclusive) range, find all entries with primary keys
    /// within that range and sum the column at `column_index`.
    pub fn sum_version(&self, start_range: i64, end_range: i64, column_index: usize, relative_version: i64) -> PyResult<i64> {
        let rids = self
            .indexer
            .locate_range(start_range, end_range, self.key_column);

        // One hot encoding 🔥🥵
        let mut projection = vec![0; self.num_columns];
        projection[column_index] = 1;

        let mut sum = 0;
        for rid in rids {
            // Grab the value of the specified column in the record identified by `rid`.
            // If it returns `Some(value)`, add the value to the sum. Otherwise, add zero (this
            // technically shouldn't happen).

            sum += match self
                .select_by_rid_version(rid, &projection, relative_version)
                .unwrap()[0]
            {
                Some(val) => val,
                None => 0,
            };
        }

        Ok(sum)
    }

    pub fn select_version(&mut self, search_key: i64, search_key_index: usize, proj: Vec<usize>, relative_version: i64) -> PyResult<Vec<PyRecord>> {
        let rids = self
            .indexer
            .locate_range(search_key, search_key, search_key_index);
        let mut results: Vec<PyRecord> = Vec::new();

        for rid in rids {
            match self.select_by_rid_version(rid, &proj, relative_version) {
                Ok(column_values) => {
                    // Now, ensure we only include the requested projected columns
                    if let Some(_first_column_value) = column_values.get(0) {
                        let record_values: Vec<Option<i64>> =
                            column_values.into_iter().take(proj.len()).collect();
                        results.push(PyRecord::new(rid, search_key, record_values));
                    }
                },

                Err(_) => {
                    // Convert Rust error to Python error
                    return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Error in select_version"
                    )));
                }
            }
        }
        Ok(results)
    }

    /// Returns (true, RID) for base record and (false, RID) for tail record.
    pub fn get_version(&self, rid: RID, mut tail_rid: RID, relative_version: i64) -> (bool, RID) {
        let mut version = 0;
        // TODO - This is set to 1 only for printing purposes.
        let mut projected_columns: Vec<usize> = vec![0; self.num_columns + NUM_METADATA_COLS];

        // get indirection
        projected_columns[self.num_columns + NUM_METADATA_COLS - 1 - INDIRECTION_REV_IDX] = 1;
        let mut tail_rid_indirection: usize;
        // starting from the newest version...
        while version > relative_version {
            let tail_address = self.page_directory[&tail_rid];

            match self.page_ranges[tail_address.range].read_tail_record(
                tail_address.page,
                tail_address.offset,
                &projected_columns,
            ) {
                Ok(tail_columns) => {
                    tail_rid_indirection = tail_columns
                        [tail_columns.len() - 1 - INDIRECTION_REV_IDX]
                        .unwrap() as usize;
                },

                Err(_) => {
                    panic!("Error looping through tail rids for select_version");
                }
            }

            if tail_rid_indirection == rid {
                break;
            }

            tail_rid = tail_rid_indirection;
            version -= 1;
        }

        return ((tail_rid == rid), tail_rid as RID);
    }

    pub fn delete(&mut self, primary_key: i64) -> PyResult<()> {
        // First, we need to get the RID corresponding to the base record with this primary key
        let base_rid = self
            .indexer
            .locate_range(primary_key, primary_key, self.key_column);

        if base_rid.len() == 0 {
            // Doesn't exist or has already been deleted... not an error because the behavior matches what we expect
            return Ok(());
        }

        let base_rid = base_rid[0];
        let base_address = self.page_directory[&base_rid];
        let projection = vec![1; self.num_columns + NUM_METADATA_COLS];

        match self.page_ranges[base_address.range].read_base_record(
            base_address.page,
            base_address.offset,
            &projection,
        ) {
            Ok(base_columns) => {
                // Add the base record RID to the list of dead RIDs for removal
                self.dead_rids.push(base_rid);

                // Remove the base record's values from the indexer
                if let Some((_, elements)) = base_columns.split_last() {
                    self.indexer.remove_from_index(elements.to_vec(), base_rid);
                }

                // Now that we've taken care of the base RID, let's also remove all the tail records
                // This means we need to (1) remove their columns from the index, and (2) add their RIDs
                // to the list of dead RIDs to be deallocated during merging
                let tail_rid = base_columns[base_columns.len() - 1];

                if tail_rid.is_some() && tail_rid.unwrap() == base_rid as i64 {
                    // There are no tail records - we're done
                    return Ok(());
                }

                // Otherwise, there are tail records and we need to traverse all of them
                let mut tail_rid = tail_rid.unwrap();
                let mut tail_address = self.page_directory[&(tail_rid as usize)];

                loop {
                    match self.page_ranges[tail_address.range].read_tail_record(
                        tail_address.page,
                        tail_address.offset,
                        &projection,
                    ) {
                        Ok(tail_columns) => {
                            // Add the last tail record's RID to the list of dead RIDs
                            self.dead_rids.push(tail_rid as usize);

                            // Calculate the RID of the previous tail record (or the base record)
                            let prev_tail_rid = tail_columns[tail_columns.len() - 1].unwrap();

                            // Drop the indirection column and remove all value - RID pairs from the indexer
                            if let Some((_, elements)) = tail_columns.split_last() {
                                self.indexer.remove_from_index(elements.to_vec(), base_rid);
                            }

                            if prev_tail_rid == base_rid as i64 {
                                // We've returned to the base record - nothing more to do here
                                break;
                            }

                            // Prepare for the next iteration of this loop
                            tail_rid = prev_tail_rid;
                            tail_address = self.page_directory[&(tail_rid as usize)];
                        },

                        Err(_) => {
                            break;
                        }
                    }
                }
            },

            Err(_) => {
                // It doesn't exist or another error occurred... that's fine!
                return Ok(());
            }
        }

        Ok(())
    }
}

// These methods aren't exposed to Python via PyO3
impl Table {
    /// Select one record given its RID and a column projection.
    fn select_by_rid(&self, rid: RID, proj: &Vec<usize>) -> Result<Vec<Option<i64>>, DatabaseError> {
        let base_address = self.page_directory[&rid];

        let mut effective_proj = proj.clone();
        effective_proj.resize(self.num_columns + NUM_METADATA_COLS, 0);
        effective_proj[self.num_columns + NUM_METADATA_COLS - 1] = 1;

        // First, get the base record
        match self.page_ranges[base_address.range].read_base_record(
            base_address.page,
            base_address.offset,
            &effective_proj,
        ) {
            Ok(base_columns) => {
                // Check if we have a most recent tail record
                if base_columns[base_columns.len() - 1].is_some() && base_columns[base_columns.len() - 1].unwrap() == rid as i64 {
                    // There is no record more recent than this one! Return it
                    let length = base_columns.len() - 1;
                    return Ok(base_columns.into_iter().take(length).collect());
                }

                // We DO have a most recent tail record - let's find it!
                let tail_rid = base_columns[base_columns.len() - 1].unwrap();
                let tail_address = self.page_directory[&(tail_rid as usize)];

                match self.page_ranges[tail_address.range].read_tail_record(
                    tail_address.page,
                    tail_address.offset,
                    &effective_proj,
                ) {
                    Ok(tail_columns) => {
                        let length = tail_columns.len() - 1;
                        return Ok(tail_columns.into_iter().take(length).collect());
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

        // Silently fail by returning an empty vector - replace with an error in the future
        Ok(vec![])
    }

    /// Select record by RID given a relative version.
    fn select_by_rid_version(&self, rid: RID, proj: &Vec<usize>, relative_version: i64) -> Result<Vec<Option<i64>>, DatabaseError> {
        let base_address = self.page_directory[&rid];

        // We need to guarantee that the metadata (indirection) will be present
        let mut effective_proj = proj.clone();
        effective_proj.resize(self.num_columns + NUM_METADATA_COLS, 0);

        let onehot_indir_idx = self.num_columns + NUM_METADATA_COLS - 1 - INDIRECTION_REV_IDX;
        effective_proj[onehot_indir_idx] = 1;

        // First, get the base record
        match self.page_ranges[base_address.range].read_base_record(
            base_address.page,
            base_address.offset,
            &effective_proj,
        ) {
            Ok(base_columns) => {
                let col_length = base_columns.len() - NUM_METADATA_COLS;
                let indir_idx = base_columns.len() - 1 - INDIRECTION_REV_IDX;

                // Check if we have a most recent tail record
                if base_columns[indir_idx].is_some() && base_columns[indir_idx].unwrap() == rid as i64 {
                    // There is no record more recent than this one! Return it
                    return Ok(base_columns.into_iter().take(col_length).collect());
                }

                // We DO have a most recent tail record - let's find it!
                let tail_rid = base_columns[indir_idx].unwrap() as usize;

                let (is_base, historic_rid) = self.get_version(rid, tail_rid, relative_version);
                let historic_address = self.page_directory[&historic_rid];

                if is_base {
                    match self.page_ranges[historic_address.range].read_base_record(
                        historic_address.page,
                        historic_address.offset,
                        &effective_proj,
                    ) {
                        Ok(cols) => {
                            return Ok(cols.into_iter().take(col_length).collect());
                        },

                        Err(_) => {
                            panic!("Couldn't get relative version.")
                        }
                    }
                } else {
                    match self.page_ranges[historic_address.range].read_tail_record(
                        historic_address.page,
                        historic_address.offset,
                        &effective_proj,
                    ) {
                        Ok(cols) => {
                            return Ok(cols.into_iter().take(col_length).collect());
                        },

                        Err(_) => {
                            panic!("Couldn't get relative version.")
                        }
                    }
                }
            },

            Err(_) => {
                // Do nothing for now
            }
        }

        // Silently fail by returning an empty vector - replace with an error in the future
        Ok(vec![])
    }
}

/// Initializes the internal merge thread.
pub fn start_merge_thread(num_columns: usize, bpm: &'static BufferPool) -> Option<Sender<MergeRequest>> {
    let (tx, rx) = mpsc::channel::<MergeRequest>();

    thread::spawn(move || {
        loop {
            match rx.recv() {
                Ok(MergeRequest {
                    base_pages,
                    tail_pages,
                    tps,
                    page_directory,
                }) => {
                    continue;
                    println!("[DEBUG] Merge started.");
                    // We'd like to collect relevant physical pages here only _once_
                    let mut physical_base_pages = vec![vec![Page::new(); num_columns + NUM_METADATA_COLS]; base_pages.len()];
                    let mut physical_tail_pages = vec![vec![Page::new(); num_columns + NUM_METADATA_COLS]; tail_pages.len()];

                    // Maps tail page indices to the base RIDs they correspond to (that we are interested in)
                    let mut tail_page_to_rids: HashMap<usize, Vec<RID>> = HashMap::new();

                    // Local copy of the TPS that we'll update as we go
                    let mut temp_tsp = 0;

                    for (logical_bp, physical_bps) in
                        base_pages.iter().zip(physical_base_pages.iter_mut())
                    {
                        // Grab a copy of the indirection column associated with this logical base page
                        let indirection_column_identifier =
                            logical_bp.columns[num_columns + NUM_METADATA_COLS - 1];
                        let page = bpm
                            .request_page(indirection_column_identifier);

                        // NOTE - We must skip the last cell as it contains the next available offset and NOT an RID
                        for i in 0..page.get_cells().len() - 1 {
                            let cell = page.get_cells()[i];
                            if let Some(tail_rid) = cell.value() {
                                let tail_address = match page_directory.get(&(tail_rid as usize)) {
                                    Some(value) => value,
                                    None => {
                                        // This record must have been deleted at some point
                                        continue;
                                    }
                                };

                                // Keep track of the largest merged tail RID
                                if tail_rid > temp_tsp {
                                    temp_tsp = tail_rid;
                                }

                                // Index of the logical tail pages holding the _physical_
                                // pages we're interested in
                                let physical_pages_index = tail_address.page;

                                tail_page_to_rids
                                    .entry(physical_pages_index)
                                    .or_insert_with(|| vec![tail_rid.clone() as usize])
                                    .push(tail_rid as usize);
                            }
                        }

                        physical_bps[num_columns + NUM_METADATA_COLS - 1] = page;
                    }

                    // Here we are basically grabbing copies of the tail record values we are interested in
                    for tp_index in tail_page_to_rids.keys() {
                        for i in 0..num_columns {
                            let page = bpm
                                .request_page(tail_pages[*tp_index].columns[i]);
                            physical_tail_pages[*tp_index][i] = page;
                        }
                    }

                    // At this point, every logical base page has its indirection filled
                    // We also have a map from logical tail pages to the RIDs contained in them
                    // that _we are interested in_

                    for physical_bp in physical_base_pages.iter_mut() {
                        let indirection_page = physical_bp[num_columns + NUM_METADATA_COLS - 1];

                        // Skip the indirection column when writing
                        for i in 0..num_columns {
                            for j in 0..indirection_page.get_cells().len() - 1 {
                                let indir_cell = indirection_page.get_cells()[j];
                                if let Some(tail_rid) = indir_cell.value() {
                                    let tail_addr = match page_directory.get(&(tail_rid as usize)) {
                                        Some(value) => {
                                            value
                                        },

                                        None => {
                                            // This record must have been deleted at some point
                                            continue;
                                        }
                                    };

                                    // Value of this tail record
                                    let tail_val = physical_tail_pages[tail_addr.page][i]
                                        .get_cells()[tail_addr.offset]
                                        .value();

                                    if tail_val.is_none() {
                                        println!("------");
                                        panic!("WARNING - Trying to write a `None` value from tail record into base record.\nBase value to be replaced is {:?} @ i = {}, j = {}", physical_bp[i].get_cells()[j], i, j);
                                        println!("{:?}", physical_tail_pages[tail_addr.page][i]);
                                        println!("------")
                                    }

                                    // Write copy
                                    physical_bp[i]
                                        .write(j, tail_val)
                                        .expect("Failed to write to offset");
                                }
                            }
                        }
                    }

                    // Lock and unwrap ONCE to ensure nobody else messes around with it while we work on it (may not be needed)

                    for (logical_bp, corresp_phys_bps) in
                        base_pages.iter().zip(physical_base_pages.iter())
                    {
                        // Note that we are NOT writing the indirection column
                        for i in 0..num_columns {
                            bpm.
                                write_page_masked(corresp_phys_bps[i], logical_bp.columns[i]);
                        }
                    }

                    tps.swap(temp_tsp as usize, std::sync::atomic::Ordering::SeqCst);
                }

                Err(_) => {
                    break;
                }
            }
        }
    });

    return Some(tx);
}

#[pyfunction]
pub fn persist_bpm() {
    BPM.persist();
}