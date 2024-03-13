use pyo3::prelude::*;

use crate::table::{PyRecord, Table};
use crate::bufferpool::BufferPool;
use crate::transaction::{self, Transaction, Query, QueryName};
use std::time::Duration;
use crate::table::RID;

use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use std::{default, fs};

use std::thread::{self, JoinHandle};

#[pyclass]
pub struct PyTableProxy {
    #[pyo3(get)]
    id: usize,

    #[pyo3(get)]
    num_columns: usize,

    #[pyo3(get)]
    primary_key_index: usize
}

#[derive(Debug, PartialEq)]
pub enum AbortKind {
    Permanent,
    Temporary,
    None
}

#[pyclass]
pub struct Database {
    /// Current working directory (changes whenever `open` is called).
    directory: Option<String>,

    /// Tables created in this database.
    tables: Arc<Mutex<Vec<Table>>>,

    /// Buffer pool manager shared by all tables in this database.
    bpm: Arc<Mutex<BufferPool>>,

    /// Describes whether data has been loaded from the disk.
    loaded: bool,

    running_workers: HashMap<usize, JoinHandle<()>>,

    next_worker_id: usize,

    transaction_manager: Arc<Mutex<TransactionManager>>
}

pub type Counter = usize;
pub type TransactionID = usize;

struct TransactionManager {
    transactions_in_process: HashMap<TransactionID, Vec<i64>>,
    next_transaction_id: TransactionID,
    pkeys_in_process: HashMap<i64, QueryEffect>,
}

impl TransactionManager {
    pub fn new() -> Self {
        TransactionManager {
            transactions_in_process: HashMap::new(),
            next_transaction_id: 0,
            pkeys_in_process: HashMap::new()
        }
    }

    pub fn register_transaction_with(&mut self, pkeys: Vec<i64>) -> TransactionID {
        self.transactions_in_process.insert(self.next_transaction_id, pkeys);
        self.next_transaction_id += 1;
        self.next_transaction_id - 1
    }

    pub fn release_transaction(&mut self, transaction_id: TransactionID) {
        let associated_rids = &self.transactions_in_process[&transaction_id];

        for rid in associated_rids {
            self.pkeys_in_process.remove(rid);
        }

        //println!("[DEBUG] Released all locks for transaction with id {:?}", transaction_id);
    }
}

#[derive(PartialEq, Debug, Copy, Clone)]
enum QueryEffect {
    Create, // Insert queries
    Modify, // Update queries
    Read,   // Select, select version, sum, and sum version queries
    Delete  // Delete
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
            tables: Arc::new(Mutex::new(Vec::new())),
            loaded: false,
            bpm: Arc::new(Mutex::new(BufferPool::new())),
            next_worker_id: 0,
            running_workers: HashMap::new(),
            transaction_manager: Arc::new(Mutex::new(TransactionManager::new()))
        }
    }

    /// Set the working directory to `path`.
    pub fn open(&mut self, path: String) {
        self.directory = Some(path.clone());
        self.bpm.lock().unwrap().set_directory(&path);
    }

    /// Persist all tables in this directory, as well as its buffer pool manager.
    pub fn close(&self) {
        for table in self.tables.lock().unwrap().iter() {
            table.persist();
        }

        self.bpm.lock().unwrap().persist();
    }

    /// Create a new table associated with this database and BPM.
    pub fn create_table(&mut self, name: String, num_columns: usize, key_index: usize) -> PyTableProxy {
        let table = Table::new(self.directory.as_ref().unwrap().clone(), name, num_columns, key_index, self.bpm.clone());
        
        let mut tables_lock = self.tables.lock().unwrap();
        tables_lock.push(table);

        PyTableProxy {
            id: tables_lock.len() - 1,
            num_columns: tables_lock[tables_lock.len() - 1].num_columns,
            primary_key_index: tables_lock[tables_lock.len() - 1].key_column
        }
    }

    /// Drop a table from this database.
    pub fn drop_table(&mut self, _name: String) {
        // TODO - Implement.
    }

    /// Get a table that already exists using its name.
    pub fn get_table(&mut self, name: String) -> PyTableProxy {
        let table = Table::new(self.directory.as_ref().unwrap().clone(), name, 0, 0, self.bpm.clone());
        
        let mut tables_lock = self.tables.lock().unwrap();
        tables_lock.push(table);
        
        PyTableProxy {
            id: tables_lock.len() - 1,
            num_columns: tables_lock[tables_lock.len() - 1].num_columns,
            primary_key_index: tables_lock[tables_lock.len() - 1].key_column
        }
    }

    // The following methods serve as a membrane between the `Query` class and `Table` struct,
    // which is required to overcome PyO3's limitations and the incompatability between Python's
    // and Rust's ownership models. It's not ideal, but it works!

    /// Insert a new record in the specified table.
    pub fn insert(&self, table: usize, columns: Vec<i64>) -> bool {
        self.tables.lock().unwrap()[table].insert(columns)
    }

    /// Update a record in the specified table given its primary key.
    pub fn update(&self, table: usize, primary_key: i64, columns: Vec<Option<i64>>) -> bool {
        self.tables.lock().unwrap()[table].update(primary_key, columns)
    }

    /// Select records given a search key and a projection vector.
    pub fn select(&self, table: usize, search_key: i64, search_key_index: usize, projected_columns: Vec<usize>) -> PyResult<Vec<PyRecord>> {
        self.tables.lock().unwrap()[table].select(search_key, search_key_index, projected_columns)
    }

    /// Sum records given a range of primary keys and the column being aggregated.
    pub fn sum(&self, table: usize, start_range: i64, end_range: i64, column_index: usize) -> PyResult<i64> {
        self.tables.lock().unwrap()[table].sum(start_range, end_range, column_index)
    }

    /// Select records given a search key, projection vector, and version.
    pub fn select_version(&self, table: usize, search_key: i64, search_key_index: usize, proj: Vec<usize>, relative_version: i64) -> PyResult<Vec<PyRecord>> {
        self.tables.lock().unwrap()[table].select_version(search_key, search_key_index, proj, relative_version)
    }

    /// Sum records given a range of primary keys, the column being aggregated, and the version.
    pub fn sum_version(&self, table: usize, start_range: i64, end_range: i64, column_index: usize, relative_version: i64) -> PyResult<i64> {
        self.tables.lock().unwrap()[table].sum_version(start_range, end_range, column_index, relative_version)
    }

    /// Delete a record given its table and primary key.
    pub fn delete(&self, table: usize, primary_key: i64) -> PyResult<()> {
        self.tables.lock().unwrap()[table].delete(primary_key)
    }

    pub fn run_worker(&mut self, transactions: Vec<&PyAny>) -> usize {
        // First, convert the input into something Rust can work on
        let mut transactions = Python::with_gil(|py| -> PyResult<VecDeque<Transaction>> {
            transactions
                .iter()
                .map(|py_obj| {
                    // Convert PyObject to PyRef
                    let py_ref: PyRef<Transaction> = py_obj.extract()?;
                    // Now, you can use py_ref or convert it to Transaction if your Transaction type supports it
                    // For example, assuming Transaction implements From<PyRef<Transaction>>
                    Ok(py_ref.clone())
                })
                .collect()
        }).unwrap();

        let tables_shared = self.tables.clone();
        let transaction_mgr_shared = self.transaction_manager.clone();

        let new_worker = thread::spawn(move || {
            let mut query_count = 0;

            for transaction in &transactions {
                for query in &transaction.queries {
                    query_count += 1;
                }
            }

            //println!("[DEBUG] Worker started with {:?} transactions and {:?} TOTAL queries.", transactions.len(), query_count);
            //println!("{:?}", tables_shared.lock().unwrap().len());

            // Next, we will continuously pop and check if we can run the transaction
            // If we can, we'll send it to the "run_transaction" function. Otherwise,
            // we'll send it to the back of the queue

            while transactions.len() > 0 {
                let next_transaction = transactions.pop_front().unwrap();
                //println!("[DEBUG] Checking transaction compatability...");

                let (abort_kind, transaction_id) = confirm_transaction_compatability(tables_shared.clone(), transaction_mgr_shared.clone(), next_transaction.clone());
                //println!("{:?} + {:?}", abort_kind, transaction_id);
                //println!("[DEBUG] Compatability check done! {:?} + {:?}", abort_kind, transaction_id);

                if abort_kind == AbortKind::Temporary {
                    // If we've failed three or more times, don't try it again
                    if next_transaction.try_count < 10 {
                        // We've failed less than three (or four ??) times
                        let transaction_retry = Transaction {
                            queries: next_transaction.queries.clone(),
                            try_count: next_transaction.try_count + 1
                        };
    
                        transactions.push_back(transaction_retry);
                    } else {
                        println!("[WARNING] Dropping transact because it tried too many times unsuccessfully.");
                    }
                } else if abort_kind == AbortKind::None {
                    for query in next_transaction.clone().queries {
                        run_query(tables_shared.clone(), query);
                    }

                    transaction_mgr_shared.lock().unwrap().release_transaction(transaction_id);
                } else {
                    //println!("WARNING - Permanent abort detected.");
                }

                //thread::sleep(Duration::from_millis(5000));
                //println!("\n[DEBUG] Popped another transaction. The queue is now {:?}\n", next_transaction);
            }

            //println!("[DEBUG] Worker finished.");
        });

        self.running_workers.insert(self.next_worker_id, new_worker);
        self.next_worker_id += 1;

        //println!("[DEBUG] Worker registered, returning ID!");
        self.next_worker_id - 1
    }

    pub fn join_worker(&mut self, worker_id: usize) {
        let worker = self.running_workers.remove(&worker_id);
        if worker.is_some() {
            worker.unwrap().join().unwrap();
        }
    }
}

/// Confirm that this transaction is compatible with all currently running queries
// TODO - Refactor this to return an enum or something... this is hacky and unpleasant
pub fn confirm_transaction_compatability(tables: Arc<Mutex<Vec<Table>>>, transaction_mgr: Arc<Mutex<TransactionManager>>, transaction: Transaction) -> (AbortKind, TransactionID) {
    // Acquire transaction manager lock
    let mut transact_mgr_lock = transaction_mgr.lock().unwrap();

    // Initialize transaction-local hash for compatability
    let mut transact_local_pkey_compat: HashMap<i64, QueryEffect> = HashMap::new();

    for query in transaction.queries {
        match query.query {
            QueryName::Insert => {
                let primary_key = query.list_arg[query.primary_key_index].unwrap();
                if let Some(query_effect) = transact_local_pkey_compat.get(&primary_key) {
                    if *query_effect != QueryEffect::Delete {
                        // This transaction will fail every time because the primary key is
                        // already in existance - abort permamently
                        return (AbortKind::Permanent, 0);
                    }
                }

                // This query is compatible with all the other queries in THIS transaction
                // we've seen so far! Now, make sure it's compatible with transactions already running

                if transact_mgr_lock.pkeys_in_process.get(&primary_key).is_some() {
                    // We can only perform this query if this primary key is absent from all other running transactions
                    // However, if this record is deleted at some point, we will be able to run this query - abort and retry
                    return (AbortKind::Temporary, 0);
                }

                // This query is compatible with all the currently running transactions as well! Finally,
                // is it compatible with the database in its current state?
                let matched_rids = tables.lock().unwrap()[query.table].locate_range(primary_key, primary_key, query.primary_key_index);
                if matched_rids.len() > 0 {
                    // This primary key already exists in the database - we can't perform it now,
                    // but we might be able to in the future (if it's deleted or updated)
                    return (AbortKind::Temporary, 0)
                }

                // This query is compatible!
                transact_local_pkey_compat.insert(primary_key, QueryEffect::Create);
            },

            QueryName::Update => {
                let old_primary_key = query.single_arg_1.unwrap();
                let new_primary_key = query.list_arg[query.primary_key_index];

                // We need to check two things -
                // (1) That the old primary key exists and isn't being worked on by other transactions, and
                // (2) that the new primary key doesn't exist or has been deleted by a previous query in THIS transaction
                // If one of these conditions doesn't hold, we need to abort

                // We'll start with the first condition
                if let Some(query_effect) = transact_local_pkey_compat.get(&old_primary_key) {
                    // We can only update if the last query working on this primary key WASN'T a delete
                    if *query_effect == QueryEffect::Delete {
                        // Will never be able to run this transaction
                        println!("[DEBUG] Permanently aborting because last query had a delete effect.");
                        return (AbortKind::Permanent, 0);
                    }
                } else {
                    // Primary key wasn't added in this transaction, so it must have been
                    // added earlier and is in the database... right?
                    let matched_rids = tables.lock().unwrap()[query.table].locate_range(old_primary_key, old_primary_key, query.primary_key_index);
                    if matched_rids.len() == 0 {
                        // This record doesn't exist in the database, but it may be added some time
                        // in the future - abort and retry another time
                        //println!("[DEBUG] Temporarily aborting {:?} query because primary key doesn't exist.", query.query);
                        return (AbortKind::Temporary, 0)
                    }
                }

                // This query is compatible with all the other queries in THIS transaction
                // we've seen so far and it definitelye exists somewhere! Now, make sure
                // it's compatible with transactions already running

                if transact_mgr_lock.pkeys_in_process.get(&old_primary_key).is_some() {
                    // We can only perform this query if no other transactions are working on the record in
                    // question - abort and retry another time
                    //println!("[DEBUG] Temporarily aborting {:?} query because primary key being used somewhere else.", query.query);
                    return (AbortKind::Temporary, 0);
                }

                // We know the old version of this record exists and isn't being worked on
                // concurrently by another transaction. Now, we want to know if we are allowed
                // to create the new requested primary key (if there is one specified)
                if let Some(new_pkey) = new_primary_key {
                    // User has specified a new primary key - checking it
                    // should be the same as the checking for insert

                    if let Some(query_effect) = transact_local_pkey_compat.get(&new_pkey) {
                        if *query_effect != QueryEffect::Delete {
                            // This transaction will fail every time because the primary key is
                            // already in existance locally - abort permamently
                            println!("[DEBUG] The new primary key {:?} cannot be added because it has already been added in a previous query - aborting permanently.", new_pkey);
                            return (AbortKind::Permanent, 0);
                        }
                    }

                    // This query is compatible with all the other queries in THIS transaction
                    // we've seen so far! Now, make sure it's compatible with transactions already running

                    if transact_mgr_lock.pkeys_in_process.get(&new_pkey).is_some() {
                        // We can only perform this query if this primary key is absent from all other running transactions
                        // However, if this record is deleted at some point, we will be able to run this query - abort and retry
                        //println!("[DEBUG] Temporarily aborting {:?} query because new primary key is present in shared pool.", query.query);
                        return (AbortKind::Temporary, 0);
                    }

                    // This query is compatible with all the currently running transactions as well! Finally,
                    // is it compatible with the database in its current state?
                    let matching_rids = tables.lock().unwrap()[query.table].locate_range(new_pkey, new_pkey, query.primary_key_index);
                    if matching_rids.len() > 0 {
                        // This primary key already exists in the database - we can't perform it now,
                        // but we might be able to in the future (if it's deleted or updated)
                        //println!("[DEBUG] Temporarily aborting {:?} query because primary key already exists in database.", query.query);
                        return (AbortKind::Temporary, 0);
                    }
                    
                    // The query is compatible!
                    transact_local_pkey_compat.insert(new_pkey, QueryEffect::Create);
                }

                // At this point, we know this entire query is compatible!
                transact_local_pkey_compat.insert(old_primary_key, QueryEffect::Modify);

                //println!("[DEBUG] Update successful!");
            },

            QueryName::Select => {
                // Technically, select is always good because it can't violate integrity constraints. Since
                // we don't need to handle phantoms in this milestone, we choose to add it anyways.
            },

            QueryName::Sum => {
                // Again, sum is always good because it can't violate integrity constraints
            },

            QueryName::SumVersion => {
                // Again, sum (version) is always good because it can't violate integrity constraints
            },

            QueryName::SelectVersion => {
                // Again, select (version) is always good because it can't violate integrity constraints
            },

            QueryName::Delete => {
                // For delete to run successfully...
                // (1) The primary key being deleted must exist in this transaction or the database, and
                // (2) it cannot be touched by another transaction at the same time.

                // Preemtively grab the matching RIDs from the database
                let primary_key = query.single_arg_1.unwrap();
                let matching_rids = tables.lock().unwrap()[query.table].locate_range(primary_key, primary_key, query.primary_key_index);

                // Let's start with the first condition - has this transaction created the primary key?
                match transact_local_pkey_compat.get(&primary_key) {
                    Some(effect) => {
                        // This transaction has worked on this primary key before! But does it still exist?
                        if *effect == QueryEffect::Delete {
                            // Nope - abort permamently
                            return (AbortKind::Permanent, 0);
                        }

                        // Otherwise, we can safely delete the record associated with this primary key IF
                        // it isn't also being worked on by another transaction... that's coming up
                    }, None => {
                        // If the primary key doesn't already exist in the database we're in trouble...
                        if matching_rids.len() == 0 {
                            // Primary key doesn't already exist in the database - abort and retry another time
                            return (AbortKind::Temporary, 0);
                        }
                    }
                }

                // We've established that the primary key already exists in the database or was created within
                // this transaction previously. Now, make sure it isn't being worked on by a currently running transaction

                if transact_mgr_lock.pkeys_in_process.get(&primary_key).is_some() {
                    // We cannot acquire the "lock" for this record - abort and try again
                    return (AbortKind::Temporary, 0);
                }

                // We're good to go!
                transact_local_pkey_compat.insert(primary_key, QueryEffect::Delete);
            }
        }
    }

    // If we've reached this point, that means the transaction is compatible with all other
    // currently running transactions (and all queries within this transaction are compatible
    // with one another)

    // Before returning successfully, lock on all the primary keys we'll be touching...
    for key in transact_local_pkey_compat.keys() {
        transact_mgr_lock.pkeys_in_process.insert(*key, transact_local_pkey_compat[key]);
    }

    // ... and register this transaction with the transaction manager
    let id = transact_mgr_lock.register_transaction_with(transact_local_pkey_compat.keys().cloned().collect());

    // We're done 🎉 return the registered transaction ID
    (AbortKind::None, id)
}

pub fn run_query(tables: Arc<Mutex<Vec<Table>>>, query: Query) {
    let table = &tables.lock().unwrap()[query.table];

    match query.query {
        QueryName::Insert => {
            table.insert(
                query.list_arg
                    .into_iter()
                    .map(|opt_i64| opt_i64.unwrap())
                    .collect()
                );
        },

        QueryName::Update => {
            //println!("{:?}", query.list_arg);
            table.update(query.single_arg_1.unwrap(), query.list_arg);
        },

        QueryName::Select => {
            let _ = table.select(
                query.single_arg_1.unwrap(), 
                query.single_arg_2.unwrap() as usize, 
                query.list_arg
                    .into_iter()
                    .map(|opt_i64| opt_i64.unwrap() as usize)
                    .collect()
            );
        },

        QueryName::SelectVersion => {
            let _ = table.select_version(
                query.single_arg_1.unwrap(), 
                query.single_arg_2.unwrap() as usize, 
                query.list_arg
                    .into_iter()
                    .map(|opt_i64| opt_i64.unwrap() as usize)
                    .collect(),
                query.single_arg_3.unwrap()
            );
        },

        QueryName::Sum => {
            let _ = table.sum(
                query.single_arg_1.unwrap(),
                query.single_arg_2.unwrap(),
                query.single_arg_3.unwrap() as usize
            );
        },

        QueryName::SumVersion => {
            let _ = table.sum_version(
                query.single_arg_1.unwrap(),
                query.single_arg_2.unwrap(),
                query.single_arg_3.unwrap() as usize,
                query.single_arg_4.unwrap()
            );
        },

        QueryName::Delete => {
            let _ = table.delete(query.single_arg_1.unwrap());
        }
    };
}