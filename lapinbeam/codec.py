"""Type-preserving codecs for wire payloads.

Remote messages are serialized to JSON-compatible dicts before reaching the
Rust transport. Built-in codecs preserve ``@dataclass`` and Pydantic v2 model
types across nodes; custom types can be registered with :func:`register_codec`.

Encoded form of a typed object::

    {"__lb_type__": "module.QualName", "data": {...}}

The ``__lb_type__`` key is reserved. Local sends are never serialized, so
actors receive the exact object on the same node.
"""

import dataclasses
import importlib
from typing import Any, Callable, cast

RESERVED = "__lb_type__"

_CodecEntry = dict[str, Callable[..., Any]]
_registry: dict[str, _CodecEntry] = {}  # tag -> {"encode": encode_fn, "decode": decode_fn}


def _tag(cls: type) -> str:
    return f"{cls.__module__}.{cls.__qualname__}"


def _import_class(tag: str) -> type:
    module_name, _, qualname = tag.rpartition(".")
    obj: object = importlib.import_module(module_name)
    for part in qualname.split("."):
        obj = getattr(obj, part)
    return cast(type, obj)


def _is_pydantic(cls: type) -> bool:
    return hasattr(cls, "model_dump") and hasattr(cls, "model_validate")


def register_codec(
    cls: type,
    encode: Callable[[Any], dict],
    decode: Callable[[dict], Any],
) -> None:
    """Registers a custom codec for `cls`.

    `encode` converts an instance to a JSON-compatible dict; `decode` rebuilds
    an instance from that dict. Both sides of the cluster must register the
    same codec.
    """
    _registry[_tag(cls)] = {"encode": encode, "decode": decode}


def _encode_obj(obj: Any) -> Any:
    entry = _registry.get(_tag(type(obj)))
    if entry is not None:
        return {RESERVED: _tag(type(obj)), "data": entry["encode"](obj)}
    if dataclasses.is_dataclass(obj) and not isinstance(obj, type):
        data = {
            f.name: _encode_obj(getattr(obj, f.name))
            for f in dataclasses.fields(obj)
        }
        return {RESERVED: _tag(type(obj)), "data": data}
    if _is_pydantic(type(obj)):
        return {RESERVED: _tag(type(obj)), "data": obj.model_dump()}
    if isinstance(obj, dict):
        return {k: _encode_obj(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_encode_obj(v) for v in obj]
    return obj


def _decode_obj(obj: Any) -> Any:
    if isinstance(obj, dict):
        tag = obj.get(RESERVED)
        if tag is not None:
            return _rebuild(tag, obj["data"])
        return {k: _decode_obj(v) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_decode_obj(v) for v in obj]
    return obj


def _rebuild(tag: str, data: Any) -> Any:
    entry = _registry.get(tag)
    if entry is not None:
        return entry["decode"](data)
    try:
        cls = _import_class(tag)
    except (ImportError, AttributeError) as exc:
        raise ValueError(f"no codec registered for {tag!r}") from exc
    if dataclasses.is_dataclass(cls) and isinstance(cls, type):
        return cls(**{k: _decode_obj(v) for k, v in data.items()})
    if _is_pydantic(cls):
        return getattr(cls, "model_validate")(data)
    raise ValueError(f"no codec registered for {tag!r}")


def encode_payload(obj: Any) -> Any:
    """Serializes `obj` into a JSON-compatible payload (type-tagged if needed)."""
    return _encode_obj(obj)


def decode_payload(obj: Any) -> Any:
    """Restores an object from a wire payload (type-tagged envelopes decoded)."""
    return _decode_obj(obj)
