//! Conversions between Python objects and `serde_json::Value`.
//!
//! The wire payload is JSON-compatible: `None`, `bool`, `int` (bounded by
//! u64/i64), `float`, `str`, `list`/`tuple`, and `dict` with string keys.

use pyo3::exceptions::{PyTypeError, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyAny, PyBool, PyDict, PyFloat, PyInt, PyList, PyTuple};
use serde_json::{Map, Number, Value};

/// Converts an arbitrary Python object into a `serde_json::Value`.
///
/// Dispatch deliberately uses `.cast::<T>()` (a cheap C-level type check)
/// rather than `.extract::<T>()` to *probe* what kind of value this is,
/// falling back to `.extract()` only once the concrete type is confirmed.
/// `.extract::<bool>()`/`.extract::<i64>()`/`.extract::<u64>()` are not
/// simply "try and cheaply fail" on the wrong type: PyO3's `bool` extractor
/// does a NumPy-interop fallback that looks up `type(obj).__module__` on
/// every failure, and its integer extractors call the C API's
/// `PyLong_As...` directly on *any* object on Python 3.10+ — which, for a
/// non-int (a float, list, or dict), raises and clears a real Python
/// exception internally. For a payload with many floats/containers, that
/// is dozens of raised exceptions per message; `.cast()` never raises.
pub fn py_to_json(obj: &Bound<'_, PyAny>) -> PyResult<Value> {
    if obj.is_none() {
        return Ok(Value::Null);
    }
    if let Ok(b) = obj.cast::<PyBool>() {
        return Ok(Value::Bool(b.is_true()));
    }
    if let Ok(s) = obj.extract::<String>() {
        return Ok(Value::String(s));
    }
    if let Ok(int) = obj.cast::<PyInt>() {
        if let Ok(i) = int.extract::<i64>() {
            return Ok(Value::Number(Number::from(i)));
        }
        if let Ok(u) = int.extract::<u64>() {
            return Ok(Value::Number(Number::from(u)));
        }
        return Err(PyTypeError::new_err(
            "integer out of range: must fit in i64 or u64",
        ));
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

/// Wraps a Python object so it can be fed straight to a `serde` `Serializer`
/// — see [`py_to_json_bytes`].
struct PyJson<'a, 'py>(&'a Bound<'py, PyAny>);

impl serde::Serialize for PyJson<'_, '_> {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::{Error as _, SerializeMap, SerializeSeq};

        let obj = self.0;
        if obj.is_none() {
            return serializer.serialize_none();
        }
        if let Ok(b) = obj.cast::<PyBool>() {
            return serializer.serialize_bool(b.is_true());
        }
        if let Ok(s) = obj.extract::<String>() {
            return serializer.serialize_str(&s);
        }
        if let Ok(int) = obj.cast::<PyInt>() {
            if let Ok(i) = int.extract::<i64>() {
                return serializer.serialize_i64(i);
            }
            if let Ok(u) = int.extract::<u64>() {
                return serializer.serialize_u64(u);
            }
            return Err(S::Error::custom("integer out of i64/u64 range"));
        }
        if let Ok(float) = obj.cast::<PyFloat>() {
            let f: f64 = float
                .extract()
                .map_err(|_| S::Error::custom("could not read float"))?;
            if !f.is_finite() {
                return Err(S::Error::custom("non-finite floats are not supported"));
            }
            return serializer.serialize_f64(f);
        }
        if let Ok(list) = obj.cast::<PyList>() {
            let mut seq = serializer.serialize_seq(Some(list.len()))?;
            for item in list.iter() {
                seq.serialize_element(&PyJson(&item))?;
            }
            return seq.end();
        }
        if let Ok(tuple) = obj.cast::<PyTuple>() {
            let mut seq = serializer.serialize_seq(Some(tuple.len()))?;
            for item in tuple.iter() {
                seq.serialize_element(&PyJson(&item))?;
            }
            return seq.end();
        }
        if let Ok(dict) = obj.cast::<PyDict>() {
            let mut map = serializer.serialize_map(Some(dict.len()))?;
            for (key, value) in dict.iter() {
                let key: String = key
                    .extract()
                    .map_err(|_| S::Error::custom("payload dict keys must be strings"))?;
                map.serialize_entry(&key, &PyJson(&value))?;
            }
            return map.end();
        }
        Err(S::Error::custom("value is not JSON-compatible"))
    }
}

/// Serializes a JSON-compatible Python object directly into wire bytes,
/// without ever building an intermediate `serde_json::Value` tree — for a
/// payload with many floats/containers, [`py_to_json`] followed by
/// `serde_json::to_vec` walks the data twice and allocates a `Value` node
/// per element for no benefit, since nothing ever inspects that tree
/// afterwards.
///
/// On the (rare) failure path, this re-runs the slower [`py_to_json`] to
/// get a properly typed `PyErr` (`TypeError` for an incompatible value,
/// `ValueError` for a non-finite float or an out-of-range int) — the fast
/// path itself only needs to know pass/fail, not produce a precise error,
/// since precision is only observable when something is already wrong.
pub fn py_to_json_bytes(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    match serde_json::to_vec(&PyJson(obj)) {
        Ok(bytes) => Ok(bytes),
        Err(_) => {
            let value = py_to_json(obj)?;
            serde_json::to_vec(&value)
                .map_err(|e| PyValueError::new_err(format!("payload serialization failed: {e}")))
        }
    }
}

/// Per-value seed threading the GIL token through a recursive JSON
/// deserialization — plain `Deserialize` has no way to carry the `Python<'py>`
/// context a Python object needs to be built, hence `DeserializeSeed`.
#[derive(Clone, Copy)]
struct PyJsonSeed<'py>(Python<'py>);

impl<'de> serde::de::DeserializeSeed<'de> for PyJsonSeed<'_> {
    type Value = Py<PyAny>;

    fn deserialize<D>(self, deserializer: D) -> Result<Self::Value, D::Error>
    where
        D: serde::de::Deserializer<'de>,
    {
        deserializer.deserialize_any(PyJsonVisitor(self.0))
    }
}

struct PyJsonVisitor<'py>(Python<'py>);

impl<'de> serde::de::Visitor<'de> for PyJsonVisitor<'_> {
    type Value = Py<PyAny>;

    fn expecting(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("a JSON-compatible value")
    }

    fn visit_unit<E>(self) -> Result<Self::Value, E> {
        Ok(self.0.None())
    }

    fn visit_none<E>(self) -> Result<Self::Value, E> {
        Ok(self.0.None())
    }

    fn visit_bool<E>(self, v: bool) -> Result<Self::Value, E> {
        Ok(v.into_pyobject(self.0)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind())
    }

    fn visit_i64<E>(self, v: i64) -> Result<Self::Value, E> {
        Ok(v.into_pyobject(self.0)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind())
    }

    fn visit_u64<E>(self, v: u64) -> Result<Self::Value, E> {
        Ok(v.into_pyobject(self.0)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind())
    }

    fn visit_f64<E>(self, v: f64) -> Result<Self::Value, E> {
        Ok(v.into_pyobject(self.0)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind())
    }

    fn visit_str<E>(self, v: &str) -> Result<Self::Value, E> {
        Ok(v.into_pyobject(self.0)
            .unwrap()
            .to_owned()
            .into_any()
            .unbind())
    }

    fn visit_string<E>(self, v: String) -> Result<Self::Value, E>
    where
        E: serde::de::Error,
    {
        self.visit_str(&v)
    }

    fn visit_seq<A>(self, mut seq: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::SeqAccess<'de>,
    {
        let mut items = Vec::new();
        while let Some(item) = seq.next_element_seed(PyJsonSeed(self.0))? {
            items.push(item);
        }
        PyList::new(self.0, items)
            .map(|l| l.into_any().unbind())
            .map_err(|e| serde::de::Error::custom(e.to_string()))
    }

    fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
    where
        A: serde::de::MapAccess<'de>,
    {
        let dict = PyDict::new(self.0);
        while let Some(key) = map.next_key::<String>()? {
            let value = map.next_value_seed(PyJsonSeed(self.0))?;
            dict.set_item(key, value)
                .map_err(|e| serde::de::Error::custom(e.to_string()))?;
        }
        Ok(dict.into_any().unbind())
    }
}

/// Deserializes JSON bytes directly into Python objects, without ever
/// building an intermediate `serde_json::Value` tree — the mirror image of
/// [`py_to_json_bytes`] on the receive side, where every incoming message
/// pays this cost.
pub fn bytes_to_py(py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
    use serde::de::DeserializeSeed;
    if data.is_empty() {
        return Ok(py.None());
    }
    let mut de = serde_json::Deserializer::from_slice(data);
    PyJsonSeed(py)
        .deserialize(&mut de)
        .map_err(|e| PyValueError::new_err(format!("payload deserialization failed: {e}")))
}

/// Serializes a JSON-compatible Python object into wire payload bytes.
#[pyfunction]
pub fn encode_payload(obj: &Bound<'_, PyAny>) -> PyResult<Vec<u8>> {
    py_to_json_bytes(obj)
}

/// Deserializes wire payload bytes into a Python object.
#[pyfunction]
pub fn decode_payload(py: Python<'_>, data: &[u8]) -> PyResult<Py<PyAny>> {
    bytes_to_py(py, data)
}
