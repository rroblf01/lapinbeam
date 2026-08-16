//! PyO3 module: bindings between the Rust core and the Python layer.
use pyo3::prelude::*;

pub mod convert;
pub mod node;

/// Registers the classes/functions exposed to the Python package.
pub fn register(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<node::Node>()?;
    m.add_function(wrap_pyfunction!(convert::encode_payload, m)?)?;
    m.add_function(wrap_pyfunction!(convert::decode_payload, m)?)?;
    Ok(())
}
