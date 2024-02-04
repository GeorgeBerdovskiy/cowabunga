use pyo3::{prelude::*, wrap_pymodule};

mod bufferpool;
mod table;

/// Formats the sum of two numbers as string.
#[pyfunction]
fn sum_as_string(a: usize, b: usize) -> PyResult<String> {
    Ok((a + b).to_string())
}

#[pyfunction]
fn hello_from_rust() -> PyResult<String> {
    Ok("Hello from Rust!".to_string())
}

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

/// A Python module implemented in Rust.
#[pymodule]
fn ecs_165_database(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_function(wrap_pyfunction!(sum_as_string, m)?)?;
    m.add_function(wrap_pyfunction!(hello_from_rust, m)?)?;
    m.add_wrapped(wrap_pymodule!(table_module))?;
    m.add_wrapped(wrap_pymodule!(buffer_pool_module))?;
    Ok(())
}
