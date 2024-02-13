use std::collections::HashMap;

/// Prototype hashmap index. Currently not in use.
pub struct HashMapIndex {
    /// Maps values to RIDs
    hashmap: HashMap<i64, Vec<usize>>
}

impl HashMapIndex {
    /// Create a new hashmap index
    pub fn new() -> Self {
        let mut hashmap: HashMap<i64, Vec<usize>> = HashMap::new();
    }

    // Add a RID associated with a value
    pub fn add_hash(value: i64, RID: usize) {
        if !hashmap.contains(value) {
            let vector:Vec<usize> = [RID];
            hashmap.insert(value, vector);
        }
        else {
            hashmap.get(value).push(RID);
        }
    }

    // Remove a RID associated with a value
    pub fn delete_hash(value:i64, RID: usize) {
        if !hashmap.contains(value) {
            panic("Invalid value for delete hash, make sure the value was previously inserted into the index")
        }
        else {
            hashmap.get(value).remove(RID);
        }
    }
}