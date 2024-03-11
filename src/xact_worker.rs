use pyo3::prelude::*;
use once_cell::sync::Lazy;
use crate::xact_mgr::*;
use pyo3::types::PyList;
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};

static XACT_MGR: Mutex<Lazy<TransactionManager>> = Mutex::new(Lazy::new(|| {
    // This block is run only once to initialize the instance of BufferPool
    TransactionManager::new()
}));

/// SKETCH of Rust implentation of Xact worker
#[pyclass]
pub struct TransactionWorker {
    transactions: Vec<Py<PyAny>>, // Transactions to be executed
    result: Arc<Mutex<u32>>, // To store the result (number of transactions that committed)
    handle: Mutex<Option<JoinHandle<()>>>, // Thread handle for join
}

#[pymethods]
impl TransactionWorker {
    #[new]
    fn new(transactions: Option<&PyList>) -> PyResult<Self> {
        Ok(TransactionWorker {
            transactions: transactions
                .map_or_else(|| Vec::new(), |t| t.iter().map(|item| item.into()).collect()),
            result: Arc::new(Mutex::new(0)),
            handle: Mutex::new(None),
        })
    }

    fn add_transaction(&mut self, py: Python, t: &PyAny) -> PyResult<()> {
        self.transactions.push(t.into_py(py));
        Ok(())
    }

    fn run(&self, py: Python) -> PyResult<()> {
        let transactions = self.transactions.clone();
        let result = self.result.clone();
        let handle = thread::spawn(move || {
            let mut committed = 0;
            for transaction in transactions.iter() {

                let mut mgr = XACT_MGR.lock().unwrap();
                let can_run = mgr.get_locks(transaction);
                drop(mgr);

                if can_run {
                    println!("xact info...");
                    println!("{:?}", transaction);
                    committed += 1;
                } else {
                    // put this transaction in the back of the queue
                }
            }

            let mut result = result.lock().unwrap();
            *result = committed;
        });

        // Store the handle in the struct for later joining
        let mut handle_lock = self.handle.lock().unwrap();
        *handle_lock = Some(handle);

        Ok(())
    }

    fn join(&self) -> PyResult<u32> {
        let mut handle = self.handle.lock().unwrap();
        if let Some(handle) = handle.take() {
            handle.join().unwrap(); // Wait for the thread to complete
        }

        let result = self.result.lock().unwrap();
        Ok(*result)
    }
}

