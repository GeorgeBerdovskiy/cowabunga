use pyo3::prelude::*;
use std::collections::HashMap;

/// Represents a query by the same name as the variant.
#[derive(Debug, Clone, Copy)]
pub enum QueryName {
    Insert,
    Update,
    Select,
    Sum,
    SelectVersion,
    SumVersion,
    Delete
}

/// The kind of "effect" a query will have. Used for checking conflicts.
#[derive(PartialEq, Debug, Copy, Clone)]
pub enum QueryEffect {
    Create, // Insert queries
    Modify, // Update queries
    Read,   // Select, select version, sum, and sum version queries
    Delete  // Delete
}

/// Represents a query. Arguments are separated into different fields
/// to simplify the inferace with Python
#[derive(Debug, Clone)]
pub struct Query {
    /// Type of query.
    pub query: QueryName,

    /// Identifier of the table this query will work on.
    pub table: usize,

    /// Single-value arguments (example - primary key, search key index...).
    pub single_arg_1: Option<i64>,
    pub single_arg_2: Option<i64>,
    pub single_arg_3: Option<i64>,
    pub single_arg_4: Option<i64>,

    /// List argument (every query has at most one).
    pub list_arg: Vec<Option<i64>>,

    /// Index of the primary key in the table this query works on.
    pub primary_key_index: usize
}

/// Represents a transaction, moves between Rust and Python.
#[pyclass]
#[derive(Debug, Clone)]
pub struct Transaction {
    /// The queries this transaction will run.
    pub queries: Vec<Query>,

    /// The number of times this query has been tried.
    pub try_count: u8
}

#[pymethods]
impl Transaction {
    /// Create a new transaction.
    #[new]
    pub fn new() -> Self {
        Transaction {
            queries: Vec::new(),
            try_count: 0
        }
    }

    /// Add an insert query to this transaction.
    pub fn add_insert(&mut self, table: usize, primary_key_index: usize, args: Vec<Option<i64>>) {
        self.queries.push(Query {
            query: QueryName::Insert,
            table: table,

            single_arg_1: None,
            single_arg_2: None,
            single_arg_3: None,
            single_arg_4: None,

            list_arg: args,

            primary_key_index 
        });
    }

    /// Add an update query to this transaction.
    pub fn add_update(&mut self, table: usize, primary_key_index: usize, primary_key: i64, args: Vec<Option<i64>>) {
        self.queries.push(Query {
            query: QueryName::Update,
            table: table,

            single_arg_1: Some(primary_key),
            single_arg_2: None,
            single_arg_3: None,
            single_arg_4: None,

            list_arg: args,

            primary_key_index
        });
    }

    /// Add a select query to this transaction.
    pub fn add_select(&mut self, table: usize, primary_key_index: usize, search_key: i64, search_key_index: i64, projected_columns: Vec<Option<i64>>) {
        self.queries.push(Query {
            query: QueryName::Select,
            table: table,

            single_arg_1: Some(search_key),
            single_arg_2: Some(search_key_index),
            single_arg_3: None,
            single_arg_4: None,

            list_arg: projected_columns,
            
            primary_key_index
        });
    }

    /// Add a sum query to this transaction.
    pub fn add_sum(&mut self, table: usize, primary_key_index: usize, start_range: i64, end_range: i64, column_index: i64) {
        self.queries.push(Query {
            query: QueryName::Sum,
            table: table,

            single_arg_1: Some(start_range),
            single_arg_2: Some(end_range),
            single_arg_3: Some(column_index),
            single_arg_4: None,

            list_arg: Vec::new(),

            primary_key_index
        });
    }

    /// Add a sum version query to this transaction.
    pub fn add_sum_version(&mut self, table: usize, primary_key_index: usize, start_range: i64, end_range: i64, column_index: i64, relative_version: i64) {
        self.queries.push(Query {
            query: QueryName::SumVersion,
            table: table,

            single_arg_1: Some(start_range),
            single_arg_2: Some(end_range),
            single_arg_3: Some(column_index),
            single_arg_4: Some(relative_version),

            list_arg: Vec::new(),

            primary_key_index
        });
    }

    /// Add a select version query to this transaction.
    pub fn add_select_version(&mut self, table: usize, primary_key_index: usize, search_key: i64, search_key_index: i64, proj: Vec<Option<i64>>, relative_version: i64) {
        self.queries.push(Query {
            query: QueryName::SelectVersion,
            table: table,

            single_arg_1: Some(search_key),
            single_arg_2: Some(search_key_index),
            single_arg_3: Some(relative_version),
            single_arg_4: None,

            list_arg: proj,

            primary_key_index
        });
    }

    /// Add a delete query to this transaction.
    pub fn add_delete(&mut self, table: usize, primary_key_index: usize, primary_key: i64) {
        self.queries.push(Query {
            query: QueryName::Delete,
            table: table,

            single_arg_1: Some(primary_key),
            single_arg_2: None,
            single_arg_3: None,
            single_arg_4: None,

            list_arg: Vec::new(),

            primary_key_index
        });
    }
}

/// Uniquely identifies a running transaction.
pub type TransactionID = usize;

/// Manages transactions by keeping locks on records (to avoid conflicts).
pub struct TransactionManager {
    /// Contains all the transactions currently running and their associated primary keys.
    pub transactions_in_process: HashMap<TransactionID, Vec<i64>>,

    /// Next available transaction ID.
    pub next_transaction_id: TransactionID,

    /// Contains all the primary keys currently being worked on (locked).
    pub pkeys_in_process: HashMap<i64, QueryEffect>,
}

impl TransactionManager {
    /// Creates a new transaction manager.
    pub fn new() -> Self {
        TransactionManager {
            transactions_in_process: HashMap::new(),
            next_transaction_id: 0,
            pkeys_in_process: HashMap::new()
        }
    }

    /// Registers a transaction with a set of primary keys, returning its unique identifier.
    pub fn register_transaction_with(&mut self, pkeys: Vec<i64>) -> TransactionID {
        self.transactions_in_process.insert(self.next_transaction_id, pkeys);
        self.next_transaction_id += 1;
        self.next_transaction_id - 1
    }

    /// Given a transaction identifier, releases all the locks held by that transaction.
    pub fn release_transaction(&mut self, transaction_id: TransactionID) {
        let associated_pkeys = &self.transactions_in_process[&transaction_id];

        for pkey in associated_pkeys {
            self.pkeys_in_process.remove(pkey);
        }
    }
}
