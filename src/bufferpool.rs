use pyo3::{pyclass, pymethods};

use crate::constants::*;
use crate::errors::*;
use crate::persistables::*;

use serde::{Serialize, Deserialize};
use rand::Rng;

use std::path::Path;
use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::sync::{RwLock, Arc, RwLockWriteGuard};
use std::io::{Seek, SeekFrom, Read, Write};

use::std::sync::atomic::{AtomicUsize, Ordering};

/// Contains a single field. Because all fields are 64 bit integers, we use `i64`.
/// If a field has been written, it contains `Some(i64)`. Otherwise, it holds `None`.
#[derive(Copy, Clone, Debug)]
pub struct Cell(Option<i64>);

/// Represents a physical page offset.
pub type Offset = usize;

impl Cell {
    /// Create a new cell.
    pub fn new(value: Option<i64>) -> Self {
        Cell(value)
    }

    /// Create a new empty cell.
    pub fn empty() -> Self {
        Cell(None)
    }

    /// Return the value in this cell
    pub fn value(&self) -> Option<i64> {
        self.0
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone, Copy)]
#[derive(Serialize, Deserialize)]
/// Contains the physical "address" of a page on the disk.
pub struct PhysicalPageID {
    /// Name of the table that this page belongs to.
    table_identifier: usize,

    /// Index of the column that this page belongs to.
    column_index: usize,

    /// The index of this page in its file.
    page_index: usize
}

impl PhysicalPageID {
    /// Create a new physical page ID given the table name, column index, and page index.
    fn new(table_identifier: usize, column_index: usize, page_index: usize) -> Self {
        PhysicalPageID { table_identifier, column_index, page_index, }
    }
}

/// Represents a physical page. In our design, every physical page has 512 cells. Therefore,
/// each has a size of **4096 bytes**.
#[derive(Clone, Copy, Debug)]
pub struct Page {
    /// Fixed size array of cells.
    cells: [Cell; CELLS_PER_PAGE], // Note that the last slot is reserved for the cell count
}

impl Page {
    /// Create new empty page.
    pub fn new() -> Self {
        // Create the array of emtpy cells and set the last slot to the index of
        // the next available cell (zero at first)
        let mut cells = [Cell::empty(); CELLS_PER_PAGE];
        cells[CELLS_PER_PAGE - 1] = Cell::new(Some(0));
        Page { cells }
    }

    /// Returns all the cells stored in this page.
    pub fn get_cells(&self) -> &[Cell; CELLS_PER_PAGE] {
        &self.cells
    }

    /// Create a page from an array of cells.
    pub fn from_data(cells: [Cell; CELLS_PER_PAGE]) -> Self {
        Page { cells }
    }

    /// Write a value to this page at the given offset.
    pub fn write(&mut self, offset: Offset, value: Option<i64>) -> Result<Offset, DatabaseError> {
        if offset >= CELLS_PER_PAGE - 1 {
            // The user may be trying to write to the cell count cell or beyond, which is not allowed
            return Err(DatabaseError::OffsetOOB);
        }

        self.cells[offset] = Cell::new(value);
        Ok(offset)
    }

    /// Get the index of the next available cell for this page.
    fn cell_count(&self) -> i64 {
        self.cells[CELLS_PER_PAGE - 1].value().unwrap()
    }

    /// Increment the index of the next available cell for this page.
    fn increment_cell_count(&mut self) {
        let previous_count = self.cell_count();
        self.cells[CELLS_PER_PAGE - 1] = Cell::new(Some(previous_count + 1));
    }

    /// Write a value to the next available cell in this page.
    pub fn write_next(&mut self, value: Option<i64>) -> Result<Offset, DatabaseError> {
        // First, try writing to the next available cell (which may return an error)
        self.write(self.cell_count() as usize, value)?;

        // Then, increment the cell count
        self.increment_cell_count();

        // Return the cell that we wrote to (which is now the current cell count, minus one)
        Ok((self.cell_count() - 1) as usize)
    }

    /// Read a single cell from a physical page.
    pub fn read(&self, offset: usize) -> Result<Option<i64>, DatabaseError> {
        if offset >= CELLS_PER_PAGE - 1 {
            // The user may be trying to read from the cell count cell or beyond, which is not allowed
            return Err(DatabaseError::OffsetOOB);
        }

        // Otherwise, just return the value from the requested cell
        Ok(self.cells[offset].value())
    }
}

/// Represents a single buffer pool frame.
#[derive(Debug)]
pub struct Frame {
    /// Page data stored inside this frame.
    page: Option<Page>,

    /// If this field is true, the page must be written to the disk before eviction.
    dirty: bool,

    /// If this field is true, this frame should actually be considered empty.
    empty: bool,

    /// The physical page ID of the page currently held by this frame.
    id: Option<PhysicalPageID>
}

impl Frame {
    /// Create a new empty frame.
    pub fn new() -> Self {
        Frame {
            page: None,
            dirty: false,
            empty: true,
            id: None
        }
    }
}

/// Represents the buffer pool manager. One instance of the buffer pool manager is
/// shared by _all_ tables using `Arc<Mutex<>>`.
#[derive(Debug)]
#[pyclass]
pub struct BufferPool {
    /// Working directory.
    directory: Arc<RwLock<String>>,

    /// Contains all the frames for the buffer pool.
    frames: Vec<Arc<RwLock<Frame>>>,

    // please note that Atomic types are thread safe WITHOUT the need for locks
    // https://wikipedia.org/wiki/Compare-and-swap
    /// track pin counts per frame
    pin_counts: Vec<AtomicUsize>,

    /// Maps a physical page ID to the index of the frame that contains it. If this map
    /// doesn't contain a physical page ID, that means the buffer pool doesn't have it.
    page_map: Arc<RwLock<HashMap<PhysicalPageID, usize>>>,

    /// Contains all the table names that have already been registered.
    table_identifiers: Arc<RwLock<HashMap<String, usize>>>,

    /// Next available table identifier.
    next_table_id: AtomicUsize
}

#[pymethods]
impl BufferPool {
    /// Create the buffer pool manager.
    #[new]
    pub fn new() -> Self {
        // Initialize the frames
        // let empty_frame = Arc::new(RwLock::new(Frame::new()));
        let frames = (0..BP_NUM_FRAMES)
            .map(|_| Arc::new(RwLock::new(Frame::new()))).collect();

        let pin_counts = (0..BP_NUM_FRAMES)
            .map(|_| AtomicUsize::new(0)).collect();

        // TODO - Open the default directory
        BufferPool {
            directory: Arc::new(RwLock::new(String::from("./COW_DAT"))),
            frames,
            page_map: Arc::new(RwLock::new(HashMap::new())),
            table_identifiers: Arc::new(RwLock::new(HashMap::new())),
            next_table_id: AtomicUsize::new(0),
            pin_counts,
        }
    }

    /// Set the working directory on disk. This will create the requested directory if it doesn't
    /// exist yet and open it otherwise. If the directory exists, it will also load all relevant
    /// metadata into memory.
    pub fn set_directory(&self, path: &str) {
        let dir_path = Path::new(path);

        if dir_path.exists()  {
            // The requested directory already exists - load all metadata
            let metadata_path = format!("{}/bp.hdr", path);
            let mut metadata_file = File::open(metadata_path).unwrap();

            let mut metadata_string = String::new();
            let _result = metadata_file.read_to_string(&mut metadata_string);

            let metadata: BufferPoolPersistable = serde_json::from_str(&metadata_string).unwrap();

            let mut tbl_id_wlock = self.table_identifiers.write().unwrap();
            *tbl_id_wlock = metadata.table_identifiers;
            self.next_table_id.store(metadata.next_table_id, Ordering::SeqCst);
        } else {
            // Directory doesn't exist, so create it
            std::fs::create_dir(path).unwrap();
        }

        // At this point the directory definitely exists so we can set the directory field
        let mut bp_dir_wlock = self.directory.write().unwrap();
        *bp_dir_wlock = path.to_string();
    }

    /// Persist buffer pool metadata on close.
    pub fn persist(&self) {
        // First, collect the metadata into `BufferPoolPersistable`
        let metadata = BufferPoolPersistable {
            table_identifiers:  self.table_identifiers.read().unwrap().clone(),
            next_table_id: self.next_table_id.load(Ordering::SeqCst),
        };

        // Next, generate the buffer pool header path
        let metadata_path = format!("{}/bp.hdr", self.directory.read().unwrap());

        // Next, write the data to disk
        let serialized_metadata = serde_json::to_string(&metadata).unwrap();
        let mut metadata_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(metadata_path)
            .unwrap();

        let _result = metadata_file.write(serialized_metadata.as_bytes());

        // Now write all the dirty frames
        for i in 0..BP_NUM_FRAMES {
            let frame = self.frames[i].read().unwrap();
            if frame.dirty {
                // Write page inside this frame to the disk
                self.write_page_to_disk(frame.page.unwrap(), frame.id.unwrap());
            }
        }

        // All data has been persisted!
    }
}

// These methods aren't exposed to Python
impl BufferPool {
    /// Adds a table name to the map if it isn't there already.
    pub fn register_table_name(&self, name: &str) -> usize {
        let tbl_ids_read = self.table_identifiers.read().unwrap();
        if tbl_ids_read.contains_key(name) {
            return tbl_ids_read[name];
        }
        drop(tbl_ids_read);

        // Otherwise, use the next available table ID
        self.table_identifiers.write().unwrap()
            .insert(name.to_string(), self.next_table_id.load(Ordering::SeqCst));
        return self.next_table_id.fetch_add(1, Ordering::SeqCst);
    }

    /// Get a page from the disk given its physical page ID.
    fn get_page_from_disk(&self, id: PhysicalPageID) -> Page {
        let dir_rlock = self.directory.read().unwrap();
        let path = format!("{}/{}/{}.dat", dir_rlock, id.table_identifier, id.column_index);

        let line_to_seek = id.page_index * CELLS_PER_PAGE;
        let byte_to_seek = line_to_seek * 8;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        let _result = file.seek(SeekFrom::Start(byte_to_seek as u64));

        let mut page_buffer: [u8; CELLS_PER_PAGE * 8] = [0; CELLS_PER_PAGE * 8];
        file.read_exact(&mut page_buffer).unwrap();

        let page: [i64; CELLS_PER_PAGE] = unsafe {
            // SAFETY - This assumes that the memory layouts of [u8; 4096] and [i64; 512] are the same
            std::mem::transmute(page_buffer)
        };

        let page = page.map(|value| if value == i64::MIN { Cell::empty() } else { Cell::new(Some(value)) });
        Page::from_data(page)
    }

    /// Write a page to the disk given its physical page ID.
    fn write_page_to_disk(&self, page: Page, id: PhysicalPageID) {
        let dir_rlock = self.directory.read().unwrap();
        let path = format!("{}/{}/{}.dat", dir_rlock, id.table_identifier, id.column_index);

        let line_to_seek = id.page_index * CELLS_PER_PAGE;
        let byte_to_seek = line_to_seek * 8;

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)
            .unwrap();

        let _result = file.seek(SeekFrom::Start(byte_to_seek as u64));

        let page_as_integers = page.cells.map(|value| match value.0 {
            Some(number) => number,
            None => i64::MIN
        });

        let page_buffer: [u8; CELLS_PER_PAGE * 8] = unsafe {
            // Safety: This assumes that the memory layout of [u8; 4096] and [i64; 512] is the same
            std::mem::transmute(page_as_integers)
        };

        let _result = file.write(&page_buffer);
    }

    /// Get a reference to the frame given the physical page ID or `None` if the
    /// buffer pool isn't holding the requested page.
    pub fn get_frame_by_ppid(&self, global_page_index: PhysicalPageID) ->
        Option<Arc<RwLock<Frame>>> {

        let page_map_rlock = self.page_map.read().unwrap();

        // Return the index of the requested page (which may not exist here)
        let index = page_map_rlock.get(&global_page_index).cloned();

        match index {
            Some(i) => {
                return Some(self.frames[i].clone());
            },
            None => {
                return None;
            }
        }
    }


    /// Bring page into the buffer pool from the disk and get the index of the
    /// frame that's chosen to hold it.
    fn bring_page_into_pool(&self, global_page_index: PhysicalPageID) -> usize {
        // First, check if an empty frame exists

        for i in 0..BP_NUM_FRAMES {
            let mut frame = self.frames[i].write().unwrap();

            if frame.empty {
                // We found one! We acquired a write lock on purpose to guarantee that nobody
                // writes to it while we switch from a read to a write lock
                let page = self.get_page_from_disk(global_page_index);

                frame.page = Some(page);
                frame.empty = false;
                frame.dirty = false;
                frame.id = Some(global_page_index);

                // Next, let's update the page map
                let mut page_map_lock = self.page_map.write().unwrap();
                page_map_lock.entry(global_page_index).or_insert(i);

                // Finally, return the index of the frame that now holds this page
                return i;
            }

            drop(frame);
        }

        // At this point, we failed to get an empty frame (and there will never be an empty frame again)
        // For this reason, we need to check for a frame that we can evict

        /*for i in 0..BP_NUM_FRAMES {
            if Arc::strong_count(&self.frames[i]) - 1 == 0 {
                // The frame in question is only being used by the buffer pool so we can safely evict it
                // First, get a write lock on it
                let mut frame = self.frames[i].write().unwrap();

                // Now let's remove it from the page map
                let mut page_map_lock = self.page_map.write().unwrap();
                page_map_lock.remove(&frame.id.unwrap());

                if frame.dirty {
                    // We need to write this frame before evicting it
                    self.write_page_to_disk(frame.page.unwrap(), frame.id.unwrap());
                }

                // Now we can safely overwrite this frame
                // Let's start by grabbing the requested page from the disk
                let page = self.get_page_from_disk(global_page_index);

                frame.page = Some(page);
                frame.empty = false;
                frame.dirty = false;
                frame.id = Some(global_page_index);

                // Next, let's update the page map with the newly retrieved and loaded page
                page_map_lock.entry(global_page_index).or_insert(i);

                // Finally, return the index of the frame that now holds this page
                return i;
            }
        }*/

        // At this point, we did not find a page that could be evicted either. For that reason,
        // let's just latch onto a random frame until it no longer has any pins
        let random_frame_index = rand::thread_rng().gen_range(0..BP_NUM_FRAMES);
        let mut frame: RwLockWriteGuard<Frame>;

        loop {
            if Arc::strong_count(&self.frames[random_frame_index]) - 1 == 0 {
                // We can now evict the randomly chosen frame!
                frame = self.frames[random_frame_index].write().unwrap();
                break;
            }

            continue;
        }

        // We've exited the loop, which means `random_frame` now contains a random frame we can safely evict and overwrite!
        // The frame in question is only being used by the buffer pool so we can safely evict it

        // Now let's remove it from the page map
        let mut page_map_lock = self.page_map.write().unwrap();
        page_map_lock.remove(&frame.id.unwrap());

        if frame.dirty {
            // We need to write this frame before evicting it
            self.write_page_to_disk(frame.page.unwrap(), frame.id.unwrap());
        }

        // Now we can safely overwrite this frame
        // Let's start by grabbing the requested page from the disk
        let page = self.get_page_from_disk(global_page_index);

        frame.page = Some(page);
        frame.empty = false;
        frame.dirty = false;
        frame.id = Some(global_page_index);

        // Next, let's update the page map with the newly retrieved and loaded page
        page_map_lock.entry(global_page_index).or_insert(random_frame_index);

        // Finally, return the index of the frame that now holds this page
        return random_frame_index;
    }

    /// Return an entire page given its physical page ID. Requires a mutable reference
    /// to `self` because this function _may_ need to grab this page from the disk and
    /// write it to an available frame.
    pub fn request_page(&self, id: PhysicalPageID) -> Page {
        match self.get_frame_by_ppid(id) {
            Some(frame_ref) => {
                // This page is already in the buffer pool - return it
                return frame_ref.read().unwrap().page.unwrap();
            },

            None => {
                // This page isn't in the buffer pool yet - grab it and try again
                let index = self.bring_page_into_pool(id);
                return self.frames[index].read().unwrap().page.unwrap();
            }
        }
    }

    /// Write to a page but ignore `None` values and the last cell, which contains the next available slot.
    // TODO: check that this is thread safe
    pub fn write_page_masked(&self, page: Page, id: PhysicalPageID) {
        let mut modified_cells = [Cell::empty(); CELLS_PER_PAGE];

        let mut i = 0;
        for cell in page.cells {
            if cell.value().is_none() || i == CELLS_PER_PAGE - 1 {
                break;
            }

            modified_cells[i] = cell;
            i += 1;
        }

        match self.get_frame_by_ppid(id) {
            Some(frame_ref) => {
                // This page is already in the buffer pool - write to it
                let mut frame = frame_ref.write().unwrap();
                frame.dirty = true;

                while i < CELLS_PER_PAGE {
                    modified_cells[i].0 = frame.page.unwrap().cells[i].0;
                    i += 1;
                }

                frame.page = Some(Page::from_data(modified_cells));
            },

            None => {
                // This page isn't in the buffer pool yet - grab it and try again
                let index = self.bring_page_into_pool(id);

                let mut frame = self.frames[index].write().unwrap();
                frame.dirty = true;
                
                while i < CELLS_PER_PAGE {
                    modified_cells[i].0 = frame.page.unwrap().cells[i].0;
                    i += 1;
                }

                frame.page = Some(Page::from_data(modified_cells));
            }
        }
    }

    /// Write an entire page given its physical page ID. The page already exists on disk.
    pub fn write_page(&self, page: Page, id: PhysicalPageID) {
        match self.get_frame_by_ppid(id) {
            Some(frame_ref) => {
                // This page is already in the buffer pool - write to it
                let mut frame = frame_ref.write().unwrap();

                frame.dirty = true;
                frame.page = Some(page);
            },

            None => {
                // This page isn't in the buffer pool yet - grab it and try again
                let index = self.bring_page_into_pool(id);

                let mut frame = self.frames[index].write().unwrap();

                frame.dirty = true;
                frame.page = Some(page);
            }
        }
    }

    /// Determine the next available page index for a column given its index and the
    /// table it belongs to. This will access the table's header (metadata) file on disk. In the future,
    /// this should be done entirely in memory and only persisted to disk upon shutdown.
    fn next_page_index(&self, table_id: usize, column_index: usize) -> usize {
        let dir_rlock = self.directory.read().unwrap();
        let path = format!("{}/{}/{}.hdr", dir_rlock, table_id, column_index);

        let serialized_header = std::fs::read_to_string(path).unwrap();
        let header: ColumnPeristable = serde_json::from_str(&serialized_header).unwrap();
        header.next_page_index
    }

    /// Update the next available page index for a column on disk
    fn update_page_index(&self, table_id: usize, column_index: usize, new_id: usize) {
        let dir_rlock = self.directory.read().unwrap();
        let path = format!("{}/{}/{}.hdr", dir_rlock, table_id, column_index);
        let new_header = ColumnPeristable { next_page_index: new_id };

        let serialized_header= serde_json::to_string(&new_header).unwrap();

        let mut hdr_file = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(path)
            .unwrap();

        let _result = hdr_file.write(serialized_header.as_bytes());
    }

    /// Allocate an entirely new page. Returns the index physical page ID
    /// for this newly allocated page.
    pub fn allocate_page(&self, table_id: usize, column_index: usize) -> PhysicalPageID {
        // Since we're allocating this page, it clearly doesn't exist in memory OR on
        // the disk, so we need to add it.

        // We have the table name and column index - all we need to know is the page index to
        // create the physical page ID. However, we can't just initialize a new one without writing
        // it to the disk first. Otherwise, we can't use functions like `write_page`
        let page_index = self.next_page_index(table_id, column_index);
        let id = PhysicalPageID::new(table_id, column_index, page_index);

        // Next, write an empty page to the right column file
        self.write_page_to_disk(Page::new(), id);

        // Finally, write an empty page to the correct buffer pool frame. This function now works
        // because the newly allocated page is on the disk!
        self.write_page(Page::new(), id);

        // Before we finish here, make sure to update the next available page index
        // This will write to the header file on disk
        self.update_page_index(table_id, column_index, page_index + 1);

        // At this point, we have allocated a new page for the requested table and column. This page
        // exists on the disk (although it's empty) and is probably also in the buffer pool at this point.
        return id;
    }

    /// Create _several_ entirely new pages.
    pub fn allocate_pages(&self, table_id: usize, count: usize) -> Vec<PhysicalPageID> {
        (0..count).map(|i| self.allocate_page(table_id, i)).collect()
    }

    /// Write a value given an offset on a page.
    pub fn write_value(&self, page_id: PhysicalPageID, offset: Offset, value: Option<i64>) -> Result<(), DatabaseError> {
        // First, grab the requested page
        let mut page = self.request_page(page_id);

        // Next, write the data
        page.write(offset, value)?;

        // Finally, write the page back and return
        self.write_page(page, page_id);
        Ok(())
    }

    /// Write a value to the next available offset on a page.
    pub fn write_next_value(&self, page_id: PhysicalPageID, value: Option<i64>) -> Result<Offset, DatabaseError>{
        // First, grab the requested page
        let mut page = self.request_page(page_id);

        // Next, write the data
        let offset = page.write_next(value)?;

        // Finally, write the page back and return
        self.write_page(page, page_id);
        Ok(offset)
    }

    /// Read the value at index `offset` on the page at index `page`.
    pub fn read(&self, page_id: PhysicalPageID, offset: Offset) -> Result<Option<i64>, DatabaseError> {
        // First, grab the requested page
        let page = self.request_page(page_id);

        // Then, return the value at the specified offset
        page.read(offset)
    }
}
