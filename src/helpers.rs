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

pub fn copy_by_mask<T: Copy>(orig: &Vec<T>, target: &mut Vec<T>, mask: i64) {
    let mut bit_index: usize = 0;
    let mut current_mask = mask;

    while current_mask > 0 {
        // Check if the least significant bit of current_mask is 1
        if current_mask & 1 == 1 {
            // Make sure the index is within the bounds of orig
            if bit_index < orig.len() {
                // Copy the element from orig to target
                target.push(orig[bit_index]);
            }
        }
        // Shift current_mask to the right by 1 to check the next bit
        current_mask >>= 1;
        bit_index += 1;
    }
}