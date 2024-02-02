// (Milestone One) Handle reads, writes, and creation of physical pages _in memory_ only.
use crate::table::Page;

struct BufferPool {
    pages: Vec<Page>
}

impl BufferPool {
    /// Create a new page and add it to the vector of pages. Returns the index of this page.
    fn allocate_page(&mut self, ) -> usize {
        self.pages.push(Page::new());
        self.pages.len() - 1
    }

    /// Write a value to page at index `page` and offset `offset` on that page
    fn write(&mut self, page: usize, offset: usize, value: Option<i64>) -> Result<(), ()> {
        if page >= self.pages.len() {
            // Page index is out of bounds
            return Err(())
        }

        // Page index is in bounds - proceed to write
        self.pages[page].write(offset, value)
    }

    fn read(&mut self, page: usize, offset: usize) -> Result<Option<i64>, ()> {
        if page >= self.pages.len() {
            // Page index is out of bounds
            return Err(())
        }

        // Page index is in bounds - proceed to write
        Ok(self.pages[page].read(offset))
    }
}