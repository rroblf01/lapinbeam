//! PyO3 module: bindings between the Rust core and the Python layer.
use pyo3::prelude::*;

/// Registers the classes/functions exposed to the Python package.
pub fn register(_m: &Bound<'_, PyModule>) -> PyResult<()> {
    Ok(())
}
