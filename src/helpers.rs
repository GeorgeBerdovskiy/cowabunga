pub fn bitmask(vec: &Vec<Option<i64>>, skip: usize) -> i64 {
    let mut mask: i64 = 0;
    for (index, value) in vec.iter().skip(skip).rev().enumerate() {
        if value.is_some() {
            // Set the bit at position `index` to 1
            mask |= 1 << index;
        }
    }
    mask
}