use pyo3::{pyclass, pymethods};

use crate::constants::*;
use crate::errors::*;
use std::path::Path;
use std::fs::{File, OpenOptions};
use std::sync::{RwLock, Arc};
use std::collections::{HashSet, HashMap};
use std::io::{self, BufRead, BufReader, Seek, SeekFrom, Read, Write};


// SPECIAL NULL HANDLING: we will reserve i64::MIN as our special null bit
// pattern. Internally to this file, this representation is invariant.
// WHEN WE RETURN a value, it will be made into Option<i64>.

// CELL COUNT: we use the 512th byte (if you start from 1) for cell count
// (as usize).

/// Contains one record field. Because all fields are 64 bit integers, we use `i64`.
/// If a field has been written, it contains `Some(i64)`. Otherwise, it holds `None`.
#[derive(Copy, Clone, Debug)]
struct RetCell(Option<i64>);

#[derive(Copy, Clone, Debug, PartialEq)]
struct InternCell(i64);

/// Represents the index of a page.
pub type PageIdentifier = usize;

/// Represents a physical page offset.
pub type Offset = usize;

impl InternCell {
    fn new(value: i64) -> Self {
        assert!(value != BP_NULL_VALUE, "InternCell can't be BP_NULL_VALUE");
        InternCell(value)
    }

    pub fn new_opt_i64(value: Option<i64>) -> Self {
        match value {
            Some(val) => {InternCell(val)},
            None => {InternCell(BP_NULL_VALUE)}
        }
    }

    /// Create a new **empty** cell.
    pub fn empty() -> Self {
        InternCell(BP_NULL_VALUE)
    }

    fn is_null(&self) -> bool {
        self.0 == BP_NULL_VALUE
    }

    pub fn value(&self) -> Option<i64> {
        if self.is_null() {
            None
        } else {
            Some(self.0)
        }
    }

    pub fn print(&self) {
        if self.0 == BP_NULL_VALUE { // Directly compare the i64 value inside InternCell
            println!("  -");
        } else {
            println!("  {}", self.0); // Print the inner i64 value directly
        }
    }
}


impl RetCell {
    /// Create a new cell.
    pub fn new(value: Option<i64>) -> Self {
        RetCell(value)
    }

    /// Create a new **empty** cell.
    pub fn empty() -> Self {
        RetCell(None)
    }

    pub fn print(&self) {
        match self.0 {
            Some(value) => println!("  {}", value),
            None => println!("  -")
        }
    }
}

#[derive(Hash, Eq, PartialEq, Debug, Clone)]
pub struct GlobalPageId {
    table: String,
    column_id: usize,
    page_id: usize
}

impl GlobalPageId {
    fn new(table: String, column_id: usize, page_id: PageIdentifier) -> Self {
        GlobalPageId { table, column_id, page_id, }
    }

    fn new_empty() -> GlobalPageId {
        GlobalPageId { table: String::new(), column_id: 0, page_id: 0, }
    }
}

/// Represents a physical page. In our design, every physical page has 512 cells. Therefore,
/// each has a size of **4096 bytes**.
#[derive(Clone, Copy, Debug)]
pub struct Page {
    /// Fixed size array of cells.
    cells: [InternCell; CELLS_PER_PAGE - 1],  // one cell is reserved for count

    /// The number of cells currently written. Also represents the next available index.
    cell_count: usize,
}

impl Page {
    /// Create a new memory represntation of an empty physical page.
    pub fn new() -> Self {
        Page {
            cells: [InternCell::empty(); CELLS_PER_PAGE - 1], // reserved
            cell_count: 0,
        }
    }

    pub fn print(&self) {
        println!("[");
        for cell in self.cells {
            cell.print()
        }
        println!("]");
    }

    /// Write a value to this page at the given offset.
    pub fn write(&mut self, offset: Offset, value: Option<i64>) -> Result<Offset, DatabaseError> {
        if offset >= CELLS_PER_PAGE - 1 {
            return Err(DatabaseError::OffsetOOB);
        }

        // TODO: fix case where the write isn't a write_next (aka append)
        // (cell count shouldn't be updated)

        self.cells[offset] = InternCell::new_opt_i64(value);
        self.cell_count += 1;

        Ok(self.cell_count - 1)
    }

    /// Write a value to the next available cell in this page.
    pub fn write_next(&mut self, value: Option<i64>) -> Result<Offset, DatabaseError> {
        self.write(self.cell_count, value)
    }

    /// Read a single cell from a physical page.
    pub fn read(&self, offset: usize) -> Result<Option<i64>, DatabaseError> {
        if offset >= self.cells.len() {
            return Err(DatabaseError::OffsetOOB);
        }

        Ok(self.cells[offset].value())
    }
}

//#[derive(Clone)]
pub struct Frame {
    page: Page,
    dirty: bool,
    empty: bool,
    gpid: GlobalPageId
}

impl Frame {
    pub fn new() -> Self {
        Frame {
            page: Page::new(),
            dirty: false,
            empty: true,
            gpid: GlobalPageId::new_empty(),
        }
    }
}

/// Represents the buffer pool manager. For now it only interacts with the memory, but in future
/// milestones, it'll interact with the disk as well. One instance of the buffer pool manager is
/// shared by _all_ tables using `Arc<Mutex<>>`.
#[derive(Clone)]
#[pyclass]
pub struct BufferPool {
    /// Contains physical pages for all tables. 
    frame_arr: [Arc<RwLock<Frame>>; BP_NUM_FRAMES],
    empty_pages: Arc<RwLock<HashSet<usize>>>,  // frames identified by index
    unpinned_pages: Arc<RwLock<HashSet<usize>>>,  // frames identified by index
    pinned_pages: Arc<RwLock<HashSet<usize>>>,
    pagemap: Arc<RwLock<HashMap<GlobalPageId, usize>>>

}

#[pymethods]
impl BufferPool {
    /// Create the buffer pool manager.
    #[new]
    pub fn new() -> Self {
        // Rust moment
        let mut temp_vec = Vec::with_capacity(BP_NUM_FRAMES);
        for _ in 0..BP_NUM_FRAMES {
            temp_vec.push(Arc::new(RwLock::new(Frame::new())));
        }

        let set_of_all_frames: HashSet<usize> = (0..BP_NUM_FRAMES).collect();

        BufferPool {
            frame_arr: temp_vec.try_into().unwrap_or_else(|v: Vec<_>| panic!("Expected a Vec of length {}, got {}", BP_NUM_FRAMES, v.len())),
            empty_pages: Arc::new(RwLock::new(set_of_all_frames.clone())), // careful to clone one
            unpinned_pages: Arc::new(RwLock::new(set_of_all_frames)),
            pinned_pages: Arc::new(RwLock::new(HashSet::new())),
            pagemap: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

// These methods aren't exposed to Python
impl BufferPool {
    pub fn get_page_from_file(&self, gpid: GlobalPageId) -> Page {
        let mut page_result = Page::new();

        let filepath = format!("{}-{}.dat", gpid.table, gpid.column_id); // TODO: CHANGE THIS TO THE ACTUAL NAME
        let line_number_to_jump_to = gpid.page_id * CELLS_PER_PAGE;
        let byte_to_jump = line_number_to_jump_to * 8; // TODO: +1??

        let mut file = File::open(filepath).unwrap();
        file.seek(SeekFrom::Start(byte_to_jump as u64));

        let reader = BufReader::new(file);
        let mut cells_remaining = CELLS_PER_PAGE;

        for line in reader.lines() {
            // Parse each line as an i64 and add it to the vector
            if cells_remaining == 0 {
                break
            }

            if let Some(number) = line.unwrap().parse::<i64>().ok() {
                if Some(number) == None {
                    break
                }

                page_result.write_next(Some(number));
                cells_remaining -= 1;
            }
        }
        return page_result;
    }

    pub fn flush_page(&self, page: Page, gpid: GlobalPageId) -> io::Result<()> {
        let filepath = format!("{}-{}.dat", gpid.table, gpid.column_id); // TODO: CHANGE THIS TO THE ACTUAL NAME
        let line_number_to_jump_to = gpid.page_id * CELLS_PER_PAGE;
        let byte_to_jump = line_number_to_jump_to * 8; // assuming no +1

        let mut file = OpenOptions::new()
            .write(true)
            .create(true)
            .open(Path::new(&filepath))?;

        file.seek(SeekFrom::Start(byte_to_jump as u64))?;

        for &cell in &page.cells {
            // Convert the i64 to bytes and write
            file.write_all(&cell.0.to_le_bytes())?;
        }

        Ok(())
    }

    pub fn read_pagemap(&self, global_page_id: GlobalPageId) -> Option<usize> {
        let pagemap_lock = self.pagemap.read().unwrap(); // no err handling
        pagemap_lock.get(&global_page_id).cloned() // Clone the result to return outside the lock
    }

    fn bring_page_into_pool(&self, global_page_id: GlobalPageId) -> io::Result<Arc<RwLock<Frame>>> {
        // this function is called when a page is needed, but it is not in the
        // pool. TODO: what if multiple readers cause multiple attempts to bring
        // the same page into a pool frame?

        // Does an unoccupied frame exist?
        let empty_tracker_read_lock = self.empty_pages.read().unwrap();
        if empty_tracker_read_lock.len() != BP_NUM_FRAMES {
            // YES, there is at least one empty frame.
            drop(empty_tracker_read_lock); // drop the read lock
            // because now we're writing
            let empty_tracker_write_lock = self.empty_pages.write().unwrap();

            if let Some(&empty_frame_idx) = empty_tracker_write_lock.iter().next() {
                empty_tracker_write_lock.remove(&empty_frame_idx);
                let page_from_disk = self.get_page_from_file(global_page_id);

                // lock frame arr
                let mut frame_lock = self.frame_arr[empty_frame_idx].write().unwrap();
                frame_lock.gpid = global_page_id;
                frame_lock.empty = false;
                frame_lock.dirty = false;
                frame_lock.page = page_from_disk;


                let mut pagemap_write_lock = self.pagemap.write().unwrap();
                pagemap_write_lock.entry(global_page_id).and_modify(|e| *e = empty_frame_idx);

                return Ok(self.frame_arr[empty_frame_idx].clone());

            } else {
                panic!("Set is not empty, but couldn't get first element.");
            }

        } else {
            // All frames are occupied!
            // TODO: evict
            let unpin_r_lock = self.unpinned_pages.read().unwrap();
            let mut frame_to_evict: usize;
            if unpin_r_lock.is_empty() {
                // All pages are pinned!
                drop(unpin_r_lock);
                // TODO: need way to wait for availability TODO TODO TODO
                // let pin_r_lock = self.pinned_pages.read().unwrap();
                // let frame_to_evict = pin_r_lock.iter().next().unwrap();
            } else {
                // there is an unpinned page that we can evict
                let frame_to_evict = unpin_r_lock.iter().next().unwrap(); // TODO: is it necessary to make this pinned?
            }

                let mut frame_lock = self.frame_arr[frame_to_evict].write().unwrap();
                if frame_lock.dirty {
                    self.flush_page(frame_lock.page, global_page_id)?;
                    frame_lock.dirty = false;
                }
                frame_lock.page = self.get_page_from_file(global_page_id);
                // it should not be necessary to set frame_lock.empty.

                return Ok(self.frame_arr[frame_to_evict]);
        }
    }

    pub fn locate_pool_frame(&self, tablename: String, column: usize, page_identifier: usize) -> io::Result<Arc<RwLock<Frame>>> {
        let global_page_id = GlobalPageId::new(tablename, column, page_identifier);
        // Attempt to locate frame in hash map.
        match self.read_pagemap(global_page_id) {
            Some(frame_idx) => {
                return Ok(self.frame_arr[frame_idx].clone());
            },
            None => {
                // need to *pull* Page into Frame. Maybe evict!
                return self.bring_page_into_pool(global_page_id);
            }
        }
    }


// TODO: these fns use the old "page" paradigm.

    /// Create a new page and add it to the vector of pages. Returns the index of this page.
    pub fn allocate_page(&mut self, ) -> PageIdentifier {
        self.pages.push(Page::new());
        self.pages.len() - 1
    }

    /// Create several pages and add them all to the pages vector. Return their indices in order.
    pub fn allocate_pages(&mut self, count: usize) -> Vec<PageIdentifier> {
        (0..count).map(|_| self.allocate_page()).collect()
    }

    /// Write a value to page at index `page` and offset `offset` on that page
    pub fn write(&mut self, page: PageIdentifier, offset: Offset, value: Option<i64>) -> Result<(), DatabaseError> {
        if page >= self.pages.len() {
            return Err(DatabaseError::PhysicalPageOOB)
        }

        // Page index is in bounds - proceed to write
        self.pages[page].write(offset, value).and_then(|_| Ok(()))
    }

    /// Write a value to the next available offset on the page at index `page`.
    pub fn write_next(&mut self, page: PageIdentifier, value: Option<i64>) -> Result<Offset, DatabaseError> {
        if page >= self.pages.len() {
            // Page index out of bounds
            return Err(DatabaseError::PhysicalPageOOB);
        }

        // Page index is in bounds - try writing
        self.pages[page].write_next(value)
    }

    /// Read the value at index `offset` on the page at index `page`.
    pub fn read(&mut self, page: PageIdentifier, offset: Offset) -> Result<Option<i64>, DatabaseError> {
        if page >= self.pages.len() {
            // Page index is out of bounds
            return Err(DatabaseError::PhysicalPageOOB)
        }

        // Page index is in bounds - proceed to write
        self.pages[page].read(offset)
    }
}
