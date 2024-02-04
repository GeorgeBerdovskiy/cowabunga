// (Milestone One) Handle reads, writes, and creation of physical pages _in memory_ only.

use pyo3::{pyclass, pymethods};

/// Number of cells that can be stored in a page.
const CELLS_PER_PAGE: usize = 512;

/// Contains one record field. Because all fields are 64 bit integers, we use `i64`.
/// If a field has been written, it contains `Some(i64)`. Otherwise, it holds `None`.
#[derive(Copy, Clone, Debug)]
struct Cell(Option<i64>);

pub type PageIdentifier = usize;

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
/// each has a size of **4096 bytes**. In order to calculate the physical location of a record,
/// we divide the RID by 512 for the page index and calculate the remainder for the offset (the
/// cell index).
#[derive(Clone, Copy, Debug)]
#[pyclass]
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

    /// Write a new record to a physical page. For now, assume the offset is always valid.
    pub fn write(&mut self, offset: usize, value: Option<i64>) -> Result<usize, ()> {
        if offset >= self.cells.len() {
            return Err(());
        }

        self.cells[offset] = Cell::new(value);
        self.cell_count += 1;

        Ok(self.cell_count - 1)
    }

    pub fn write_next(&mut self, value: Option<i64>) -> Result<usize, ()> {
        self.write(self.cell_count, value)
    }

    /// Read a single cell from a physical page. For now, assume the offset is always valid.
    pub fn read(&self, offset: usize) -> Option<i64> {
        // TODO: Ensure that `offset` is within the acceptable range and return an error if it isn't.
        self.cells[offset].0
    }
}

#[derive(Clone)]
#[pyclass]
pub struct BufferPool {
    pages: Vec<Page>
}

#[pymethods]
impl BufferPool {
    #[new]
    pub fn new() -> Self {
        BufferPool {
            pages: Vec::new()
        }
    }
}

impl BufferPool {
    /// Create a new page and add it to the vector of pages. Returns the index of this page.
    pub fn allocate_page(&mut self, ) -> PageIdentifier {
        self.pages.push(Page::new());
        self.pages.len() - 1
    }

    pub fn allocate_pages(&mut self, count: usize) -> Vec<PageIdentifier> {
        let mut result = Vec::new();

        for _ in [0..count] {
            result.push(self.allocate_page());
        }

        result
    }

    /// Write a value to page at index `page` and offset `offset` on that page
    /*pub fn write(&mut self, page: PageIdentifier, offset: usize, value: Option<i64>) -> Result<(), ()> {
        if page >= self.pages.len() {
            // Page index is out of bounds
            return Err(())
        }

        // Page index is in bounds - proceed to write
        self.pages[page].write(offset, value)
        Ok() =>
        Err() => 
    }*/

    pub fn write_next(&mut self, page: PageIdentifier, value: Option<i64>) -> Result<usize, ()> {
        if page >= self.pages.len() {
            // Page index out of bounds
            return Err(());
        }

        // Page index is in bounds - try writing
        self.pages[page].write_next(value)
    }

    pub fn read(&mut self, page: usize, offset: usize) -> Result<Option<i64>, ()> {
        if page >= self.pages.len() {
            // Page index is out of bounds
            return Err(())
        }

        // Page index is in bounds - proceed to write
        Ok(self.pages[page].read(offset))
    }
}