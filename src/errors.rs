#[derive(Debug)]
pub enum DatabaseError {
    // Offset is out of bounds.
    OffsetOOB,

    // Physical page is out of bounds.
    PhysicalPageOOB,

    // Page range has been filled to capacity.
    PageRangeFilled
}