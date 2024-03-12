use pyo3::prelude::*;

use crate::table::{PyRecord, Table};
use crate::bufferpool::BufferPool;
use crate::transaction::Transaction;

use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::fs;

use std::thread::{self, JoinHandle};

#[pyclass]
pub struct PyTableProxy {
    #[pyo3(get)]
    id: usize,

    #[pyo3(get)]
    num_columns: usize
}

#[pyclass]
pub struct Database {
    /// Current working directory (changes whenever `open` is called).
    directory: Option<String>,

    /// Tables created in this database.
    tables: Vec<Table>,

    /// Buffer pool manager shared by all tables in this database.
    bpm: Arc<Mutex<BufferPool>>,

    /// Describes whether data has been loaded from the disk.
    loaded: bool,

    running_workers: HashMap<usize, JoinHandle<()>>,
    next_worker_id: usize
}

#[pymethods]
impl Database {
    /// Create a new database
    #[new]
    pub fn new() -> Self {
        // Clear the default directory if it exists
        // Note that this will break any databases using the default directory
        let _clear_result = fs::remove_dir_all("./COW_DAT");
        let _create_result = fs::create_dir("./COW_DAT");

        // Create the database
        Database {
            directory: Some("./COW_DAT".to_string()),
            tables: Vec::new(),
            loaded: false,
            bpm: Arc::new(Mutex::new(BufferPool::new())),
            next_worker_id: 0,
            running_workers: HashMap::new()
        }
    }

    /// Set the working directory to `path`.
    pub fn open(&mut self, path: String) {
        self.directory = Some(path.clone());
        self.bpm.lock().unwrap().set_directory(&path);
    }

    /// Persist all tables in this directory, as well as its buffer pool manager.
    pub fn close(&self) {
        for table in &self.tables {
            table.persist();
        }

        self.bpm.lock().unwrap().persist();
    }

    /// Create a new table associated with this database and BPM.
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) -> PyTableProxy {
        let table = Table::new(self.directory.as_ref().unwrap().clone(), name, num_columns, key_index, self.bpm.clone());
        self.tables.push(table);

        PyTableProxy {
            id: self.tables.len() - 1,
            num_columns: self.tables[self.tables.len() - 1].num_columns
        }
    }

    /// Drop a table from this database.
    pub fn drop_table(&mut self, _name: String) {
        // TODO - Implement.
    }

    /// Get a table that already exists using its name.
    pub fn get_table(&mut self, name: String) -> PyTableProxy {
        let table = Table::new(self.directory.as_ref().unwrap().clone(), name, 0, 0, self.bpm.clone());
        self.tables.push(table);
        
        PyTableProxy {
            id: self.tables.len() - 1,
            num_columns: self.tables[self.tables.len() - 1].num_columns
        }
    }

    // The following methods serve as a membrane between the `Query` class and `Table` struct,
    // which is required to overcome PyO3's limitations and the incompatability between Python's
    // and Rust's ownership models. It's not ideal, but it works!

    /// Insert a new record in the specified table.
    pub fn insert(&self, table: usize, columns: Vec<i64>) -> bool {
        self.tables[table].insert(columns)
    }

    /// Update a record in the specified table given its primary key.
    pub fn update(&self, table: usize, primary_key: i64, columns: Vec<Option<i64>>) -> bool {
        self.tables[table].update(primary_key, columns)
    }

    /// Select records given a search key and a projection vector.
    pub fn select(&self, table: usize, search_key: i64, search_key_index: usize, projected_columns: Vec<usize>) -> PyResult<Vec<PyRecord>> {
        self.tables[table].select(search_key, search_key_index, projected_columns)
    }

    /// Sum records given a range of primary keys and the column being aggregated.
    pub fn sum(&self, table: usize, start_range: i64, end_range: i64, column_index: usize) -> PyResult<i64> {
        self.tables[table].sum(start_range, end_range, column_index)
    }

    /// Select records given a search key, projection vector, and version.
    pub fn select_version(&self, table: usize, search_key: i64, search_key_index: usize, proj: Vec<usize>, relative_version: i64) -> PyResult<Vec<PyRecord>> {
        self.tables[table].select_version(search_key, search_key_index, proj, relative_version)
    }

    /// Sum records given a range of primary keys, the column being aggregated, and the version.
    pub fn sum_version(&self, table: usize, start_range: i64, end_range: i64, column_index: usize, relative_version: i64) -> PyResult<i64> {
        self.tables[table].sum_version(start_range, end_range, column_index, relative_version)
    }

    /// Delete a record given its table and primary key.
    pub fn delete(&self, table: usize, primary_key: i64) -> PyResult<()> {
        self.tables[table].delete(primary_key)
    }

    pub fn run_worker(&mut self, transactions: Vec<&PyAny>) -> usize {
        let new_worker = thread::spawn(move || {
            println!("Hello!")
        });

        self.running_workers.insert(self.next_worker_id, new_worker);
        self.next_worker_id += 1;
        self.next_worker_id - 1
    }

    pub fn join_worker(&mut self, worker_id: usize) {
        let worker = self.running_workers.remove(&worker_id);
        if worker.is_some() {
            worker.unwrap().join().unwrap();
        }
    }
}
