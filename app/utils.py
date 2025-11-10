"""
Utility helpers for ingest: feature enforcement and light validation/coercion.
"""
from __future__ import annotations
from typing import Dict, Any, Tuple
import pandas as pd
import datetime as dt

def coerce_and_validate_data_dict(data: Dict[str, Any], features: Dict[str, Any]) -> Tuple[Dict[str, Any], str|None]:
    """
    Attempt to coerce types of data dict according to features contract.
    Returns (coerced_data, error_msg_or_None).
    """
    ts_col = features.get("timestamp_col", "timestamp")
    types = features.get("types", {})

    coerced = {}
    # timestamp: ensure non-empty and parseable
    ts = data.get(ts_col)
    if ts is None or str(ts).strip() == "":
        return {}, f"missing timestamp"
    try:
        # try parse ISO8601 - pandas is forgiving
        _ = pd.to_datetime(ts)
        coerced[ts_col] = str(ts)
    except Exception:
        return {}, f"invalid timestamp: {ts}"

    # signals
    for sig, t in types.items():
        if sig == ts_col:
            continue
        val = data.get(sig)
        if t == "float":
            try:
                # Accept numeric-like strings; coerce to float; None/'' => error
                if val is None or (isinstance(val, str) and val.strip() == ""):
                    return {}, f"missing numeric field {sig}"
                coerced[sig] = float(val)
            except Exception:
                return {}, f"field {sig} not float-coercible (value: {val})"
        elif t == "string":
            coerced[sig] = str(val) if val is not None else ""
        else:
            coerced[sig] = val
    return coerced, None
