//! Conversions between Python objects and `serde_json::Value`.
//!
//! The wire payload is JSON-compatible: `None`, `bool`, `int` (bounded by
//! u64/i64), `float`, `str`, `list`/`tuple`, and `dict` with string keys.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyDict, PyFloat, PyList, PyTuple};
use serde_json::{Map, Number, Value};

/// Converts an arbitrary Python object into a `serde_json::Value`.
pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.extract::<bool>() {
        return Ok(Value::Bool(b));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(i) = obj.extract::<i64>() {
        return Ok(Value::Number(Number::from(i)));
    }
    if let Ok(u) = obj.extract::<u64>() {
        return Ok(Value::Number(Number::from(u)));
    }
    if let Ok(float) = obj.cast::<PyFloat>() {
        let f: f64 = float.extract()?;
        return match Number::from_f64(f) {
            Some(n) => Ok(Value::Number(n)),
            None => Err(PyValueError::new_err("non-finite floats are not supported")),
        };
    }
    if let Ok(list) = obj.cast::<PyList>() {
        let mut out = Vec::with_capacity(list.len());
        for item in list.iter() {
            out.push(py_to_json(&item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Ok(tuple) = obj.cast::<PyTuple>() {
        let mut out = Vec::with_capacity(tuple.len());
        for item in tuple.iter() {
            out.push(py_to_json(&item)?);
        }
        return Ok(Value::Array(out));
    }
    if let Ok(dict) = obj.cast::<PyDict>() {
        let mut out = Map::new();
        for (key, value) in dict.iter() {
            let key: String = key
                .extract()
                .map_err(|_| PyTypeError::new_err("payload dict keys must be strings"))?;
            out.insert(key, py_to_json(&value)?);
        }
        return Ok(Value::Object(out));
    }
    Err(PyTypeError::new_err(format!(
        "value of type {} is not JSON-compatible",
        obj.get_type().name()?
    )))
}

/// Converts a `serde_json::Value` into a Python object.
pub fn json_to_py(py: Python<'_>, value: &Value) -> PyResult<Py<PyAny>> {
    match value {
        Value::Null => Ok(py.None()),
        Value::Bool(b) => Ok((*b).into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(i.into_pyobject(py)?.to_owned().into_any().unbind())
            } else if let Some(u) = n.as_u64() {
                Ok(u.into_pyobject(py)?.to_owned().into_any().unbind())
            } else {
                Ok(n.as_f64()
                    .unwrap()
                    .into_pyobject(py)?
                    .to_owned()
                    .into_any()
                    .unbind())
            }
        }
        Value::String(s) => Ok(s.into_pyobject(py)?.to_owned().into_any().unbind()),
        Value::Array(items) => {
            let mut list = Vec::with_capacity(items.len());
            for item in items {
                list.push(json_to_py(py, item)?);
            }
            Ok(PyList::new(py, list)?.into_any().unbind())
        }
        Value::Object(map) => {
            let dict = PyDict::new(py);
            for (key, item) in map {
                dict.set_item(key, json_to_py(py, item)?)?;
            }
            Ok(dict.into_any().unbind())
        }
    }
}

/// Serializes a JSON-compatible Python object into wire payload bytes.
#[pyfunction]
pub fn encode_payload(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    let value = py_to_json(obj)?;
    serde_json::to_vec(&value)
        .map_err(|e| PyValueError::new_err(format!("payload serialization failed: {e}")))
}

/// Deserializes wire payload bytes into a Python object.
#[pyfunction]
pub fn decode_payload(py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
    let value: Value = serde_json::from_slice(data)
        .map_err(|e| PyValueError::new_err(format!("payload deserialization failed: {e}")))?;
    json_to_py(py, &value)
}
