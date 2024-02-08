use pyo3::{pyclass, pymethods};

use crate::constants::*;
use crate::errors::*;

/// Contains one record field. Because all fields are 64 bit integers, we use `i64`.
/// If a field has been written, it contains `Some(i64)`. Otherwise, it holds `None`.
#[derive(Copy, Clone, Debug)]
struct Cell(Option<i64>);

/// Represents the index of a page.
pub type PageIdentifier = usize;

/// Represents a physical page offset.
pub type Offset = usize;

impl Cell {
    /// Create a new cell.
    pub fn new(value: Option<i64>) -> Self {
        Cell(value)
    }

    /// Create a new **empty** cell.
    pub fn empty() -> Self {
        Cell(None)
    }

	pub fn print(&self) {
		match self.0 {
			Some(value) => println!("  {}", value),
			None => println!("  -")
		}
	}
}

/// Represents a physical page. In our design, every physical page has 512 cells. Therefore,
/// each has a size of **4096 bytes**.
#[derive(Clone, Copy, Debug)]
pub struct Page {
    /// Fixed size array of cells.
    cells: [Cell; CELLS_PER_PAGE],

    /// The number of cells currently written. Also represents the next available index.
    cell_count: usize,
}

impl Page {
    /// Create a new empty physical page.
    pub fn new() -> Self {
        Page {
            cells: [Cell::empty(); CELLS_PER_PAGE],
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
        if offset >= self.cells.len() {
            return Err(DatabaseError::OffsetOOB);
        }

        self.cells[offset] = Cell::new(value);
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

        Ok(self.cells[offset].0)
    }
}

/// Represents the buffer pool manager. For now it only interacts with the memory, but in future
/// milestones, it'll interact with the disk as well. One instance of the buffer pool manager is
/// shared by _all_ tables using `Arc<Mutex<>>`.
#[derive(Clone)]
#[pyclass]
pub struct BufferPool {
    /// Contains physical pages for all tables. 
    pages: Vec<Page>
}

#[pymethods]
impl BufferPool {
    /// Create the buffer pool manager.
    #[new]
    pub fn new() -> Self {
        BufferPool {
            pages: Vec::new()
        }
    }

	pub fn print_all(&self) {
		println!("[DEBUG] About to print all {} pages...", self.pages.len());
		
		for page in &self.pages {
			page.print();
		}

		println!("[DEBUG] Done printing all pages.");
	}
}

// These methods aren't exposed to Python
impl BufferPool {
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
