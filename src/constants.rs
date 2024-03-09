/// The number of (logical) base pages per page range.
pub const BASE_PAGES_PER_RANGE: usize = 1;

/// Number of cells that can be stored in a page.
pub const CELLS_PER_PAGE: usize = 2;

/// Number of metadata columns.
pub const NUM_METADATA_COLS: usize = 1;

/// Index (zero-based) of INDIRECTION column starting from the end of our columns
pub const INDIRECTION_REV_IDX: usize = 0;

/// Number of frames in the buffer pool.
pub const BP_NUM_FRAMES: usize = 1;

/// Merge threshold.
pub const THRESHOLD: usize = 1;
