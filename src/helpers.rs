/// Generate a bitmask given a vector of either `Some(_)`` or `None` values. When `None` is encountered,
/// we set the corresponding bit to `1`. Otherwise, we set it to `0`.
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

/// Copy values from one vector to another using a bitmask. Only positions corresponding
/// to set bits (values of `1`) will be copied.
pub fn copy_using_mask<T: Copy>(original: &Vec<T>, target: &mut Vec<T>, mask: i64) {
    let mut bit_index: usize = 0;
    let mut current_mask = mask;

    while current_mask > 0 {
        // Check if the least significant bit of current_mask is 1
        if current_mask & 1 == 1 {
            // Make sure the index is within the bounds of orig
            if bit_index < original.len() {
                // Copy the element from orig to target
                target.push(original[bit_index]);
            }
        }

        // Shift current_mask to the right by 1 to check the next bit
        current_mask >>= 1;
        bit_index += 1;
    }
}