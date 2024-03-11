use pyo3::prelude::*;

pub struct TransactionManager {

}

impl TransactionManager {
    pub fn new() -> Self {
        return TransactionManager {

        }
    }

    pub fn get_locks(&mut self, transaction: &Py<PyAny>) -> bool {
        // unimplemented!()
        return true;
    }

    pub fn release_locks(&mut self, transactions: Vec<Py<PyAny>>) {
        unimplemented!()
    }

}
