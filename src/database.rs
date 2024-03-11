use pyo3::prelude::*;

use crate::table::Table;
use crate::bufferpool::BufferPool;

use std::sync::{Arc, Mutex};
use std::fs;

#[pyclass]
pub struct Database {
    /// Current working directory (changes whenever `open` is called).
    directory: Option<String>,

    /// Tables created in this database.
    tables: Vec<Table>,

    /// Buffer pool manager shared by all tables in this database.
    bpm: Arc<Mutex<BufferPool>>,

    /// Describes whether data has been loaded from the disk.
    loaded: bool
}

#[pymethods]
impl Database {
    /// Create a new database
    #[new]
    pub fn new() -> Self {
        // Clear the default directory if it exists
        // Note that this will break any databases using the default directory
        let _result = fs::remove_dir_all("./COWDAT");

        // Create the database
        Database {
            directory: Some("./COWDAT".to_string()),
            tables: Vec::new(),
            loaded: false,
            bpm: Arc::new(Mutex::new(BufferPool::new()))
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
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) -> Table {
        let table = Table::new(self.directory.as_ref().unwrap().clone(), name, num_columns, key_index, self.bpm.clone());
        // self.tables.push(table.clone());
        table
    }

    /// Drop a table from this database.
    pub fn drop_table(&mut self, _name: String) {
        // TODO - Implement.
    }

    /// Get a table that already exists using its name.
    pub fn get_table(&mut self, name: String) -> Table {
        let table = Table::new(self.directory.as_ref().unwrap().clone(), name, 0, 0, self.bpm.clone());
        //self.tables.push(table);
        table
    }
}

/*

class Database():
    def __init__(self):
        self.directory = None
        self.tables = []
        self.loaded = False

        try:
            shutil.rmtree("./COWDAT")
        except:
            pass

        self.open("COWDAT")

    # Not required for milestone1
    def open(self, path):
        self.directory = path

    def close(self):
        for table in self.tables:
            table.persist()
        
        table_module.persist_bpm()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, num_columns, key_index):
        if not self.loaded:
            table = table_module.Table(self.directory, name, num_columns, key_index, True)
            self.loaded = True
        else:
            table = table_module.Table(self.directory, name, num_columns, key_index, False)
        
        self.tables.append(table)
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        pass

    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        table = table_module.Table(self.directory, name, 0, 0, True)
        self.tables.append(table)
        return table
*/