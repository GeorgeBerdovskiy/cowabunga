use pyo3::{prelude::*, wrap_pymodule};

pub mod constants;
pub mod bufferpool;
pub mod table;
pub mod errors;
pub mod persistables;
pub mod database;

#[pymodule]
fn buffer_pool_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<bufferpool::BufferPool>()?;
    Ok(())
}

#[pymodule]
fn table_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<table::Table>()?;
    Ok(())
}

#[pymodule]
fn database_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<database::Database>()?;
    Ok(())
}

#[pymodule]
fn record_type_module(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<table::PyRecord>()?;
    Ok(())
}

#[pymodule]
fn cowabunga_rs(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_wrapped(wrap_pymodule!(table_module))?;
    m.add_wrapped(wrap_pymodule!(buffer_pool_module))?;
    m.add_wrapped(wrap_pymodule!(record_type_module))?;
    m.add_wrapped(wrap_pymodule!(database_module))?;
    Ok(())
}
