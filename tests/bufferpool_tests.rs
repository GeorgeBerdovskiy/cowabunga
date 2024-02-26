extern crate cowabunga_rs;
use cowabunga_rs::bufferpool::*;
use cowabunga_rs::constants::*;

/// Simplest conceivable write_next_value() use
#[test]
fn test_simple_write_next_value()  -> Result<(), String> {
    let mut bp = BufferPool::new();

    let ppid = bp.allocate_page("test_tbl".to_string(), 0);

    let offset = bp.write_next_value(ppid, Some(42))?;
    let val_read = bp.read(ppid, offset);

    assert_eq!(val_read, 42);
    Ok(())
}


/// Force an eviction to occur using allocate, reads and writes
#[test]
fn test_full_bufferpool()  -> Result<(), String> {
    let mut bp = BufferPool::new();

    let mut ppids: Vec<PhysicalPageID> = Vec::new();

    // create more pages than frames (all in one column)
    for _ in (1..=BP_NUM_FRAMES+1) {
        ppids.push(bp.allocate_page("test_tbl".to_string(), 0));
    }

    for (i, ppid) in ppids.iter().enumerate() {
        bp.write_next_value(*ppid, Some(i as i64 + 1));
    }

    let expected_values: Vec<i64> = (1..=BP_NUM_FRAMES+1).collect();
    let mut observed_values: Vec<i64> = Vec::new();

    for ppid in &ppids {
        observed_values.push(bp.read(*ppid, 0)?);
    }

    assert_eq!(expected_values, observed_values);
    Ok(())
}
