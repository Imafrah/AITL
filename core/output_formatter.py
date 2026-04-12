"""Flatten row dicts for table views and CSV export."""

from __future__ import annotations

import csv
import io
from typing import Any


def _flatten_value(prefix: str, val: Any, out: dict[str, Any]) -> None:
    if val is None:
        out[prefix] = None
    elif isinstance(val, dict):
        for k, v in val.items():
            nk = f"{prefix}.{k}" if prefix else str(k)
            _flatten_value(nk, v, out)
    elif isinstance(val, list):
        out[prefix] = json_list_repr(val)
    else:
        out[prefix] = val


def json_list_repr(val: list[Any]) -> str:
    return "; ".join(str(x) for x in val[:50])


def to_table(data: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Return a list of rows with nested dict/list values flattened (dynamic keys).
    """
    flat: list[dict[str, Any]] = []
    for row in data:
        out: dict[str, Any] = {}
        for k, v in row.items():
            if isinstance(v, dict):
                for fk, fv in v.items():
                    _flatten_value(f"{k}.{fk}", fv, out)
            elif isinstance(v, list):
                out[k] = json_list_repr(v)
            else:
                out[k] = v
        flat.append(out)
    return flat


def to_csv_file(data: list[dict[str, Any]], filename: str) -> bytes:
    """Serialize rows to UTF-8 CSV bytes (header = union of keys)."""
    if not data:
        return b""

    rows = to_table(data)
    keys: list[str] = []
    seen: set[str] = set()
    for r in rows:
        for k in r:
            if k not in seen:
                seen.add(k)
                keys.append(k)

    buf = io.StringIO()
    w = csv.DictWriter(buf, fieldnames=keys, extrasaction="ignore")
    w.writeheader()
    for r in rows:
        w.writerow({k: "" if r.get(k) is None else r.get(k) for k in keys})
    return buf.getvalue().encode("utf-8")
