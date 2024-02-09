use std::collections::HashMap;

pub struct HashMapIndex {
    // a record value to rid
    hashmap: HashMap<i64, Vec<usize>>
}

impl HashMapIndex {
    pub fn new() -> Self {
        HashMapIndex {
            hashmap: HashMap::new()
        }
    }

    // add_hash adds a RID associated with a value
    pub fn add_hash(&mut self, value: i64, RID: usize) {
        // If it doesnt have the value then insert a vector with RID as the only element
        if !self.hashmap.contains_key(&value) {
            self.hashmap.insert(value, vec![RID]);
        }
        // If it already exists then add the New rid to the vector
        else {
            if let Some(vec) = self.hashmap.get_mut(&value) {
                vec.push(RID);
            }
        }
    }

    // delete_hash removes a RID associated with a value
    pub fn delete_hash(&mut self, value:i64, RID: usize) {
        // Check if it contains the value, if it doesnt return, end the program
        if !self.hashmap.contains_key(&value) {
            panic!("Invalid value for delete hash, make sure the value was previously inserted into the index")
        }
        // Else, retain every value that isn't the RID
        else {
            if let Some(vec) = self.hashmap.get_mut(&value) {
                if let Some(pos) = vec.iter().position(|&x| x == RID){
                    vec.remove(pos);
                }
            }
        }
    }
}