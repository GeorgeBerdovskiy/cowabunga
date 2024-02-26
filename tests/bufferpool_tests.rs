extern crate cowabunga_rs;
use cowabunga_rs::bufferpool::*;

#[test]
fn test_simple_write() {
    // Setup test, e.g., create a new instance of the part of your library you're testing.
    let mut bp = BufferPool::new(); // Adjust according to your actual API

    let ppid = bp.allocate_page("test_tbl".to_string(), 0);

    let offset = bp.write_next_value(ppid, Some(42))?;
    let val_read = bp.read(ppid, offset);

    assert_eq!(val_read, 42);
}
