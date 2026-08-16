use pyo3::prelude::*;

pub mod py;
pub mod runtime;
pub mod transport;
pub mod wire;

/// Native module `lapinbeam._core`.
///
/// Exposes the actor runtime and the multiplexed TCP transport
/// implemented in Rust (Tokio), without blocking the Python GIL.
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    py::register(m)?;
    Ok(())
}
