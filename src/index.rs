use std::collections::HashMap;

pub struct HashMapIndex {
    // a record value to rid
    hashmap: HashMap<i64, Vec<usize>>
}

impl HashMapIndex {
    pub fn new() -> Self {
        let mut hashmap: HashMap<i64, Vec<usize>> = HashMap::new();
    }

    // add_hash adds a RID associated with a value
    pub fn add_hash(value: i64, RID: usize) {
        if !hashmap.contains(value) {
            let vector:Vec<usize> = [RID];
            hashmap.insert(value, vector);
        }
        else {
            hashmap.get(value).push(RID);
        }
    }

    // delete_hash removes a RID associated with a value
    pub fn delete_hash(value:i64, RID: usize) {
        if !hashmap.contains(value) {
            panic("Invalid value for delete hash, make sure the value was previously inserted into the index")
        }
        else {
            hashmap.get(value).remove(RID);
        }
    }
}