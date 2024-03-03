/// The number of (logical) base pages per page range.
pub const BASE_PAGES_PER_RANGE: usize = 1;

/// Number of cells that can be stored in a page.
pub const CELLS_PER_PAGE: usize = 16;

/// Number of metadata columns.
pub const NUM_METADATA_COLS: usize = 1;

/// 0-based index of INDIRECTION starting from the end of our columns
pub const INDIRECTION_REV_IDX: usize = 0;

/// Internal special null value for bufferpool
pub const BP_NULL_VALUE: i64 = i64::MIN;

/// Internal special null value for bufferpool
pub const BP_NUM_FRAMES: usize = 32;

pub const THRESHOLD: usize = 64;