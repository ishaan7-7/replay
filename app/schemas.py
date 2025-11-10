"""
Dynamic Pydantic v2 models for events using config/features.json
Builds:
 - DataModel (timestamp + signals)
 - MetaModel (row_hash, source_id, replay_mode, speed_factor, replayed_at) -> allow extra keys
 - EventModel: { data: DataModel, meta: MetaModel }
"""
from __future__ import annotations
from typing import Any, Dict
from pydantic import BaseModel, Field, create_model

def build_models_from_features(features: Dict[str, Any]):
    timestamp_col = features.get("timestamp_col", "timestamp")
    signals = features.get("signals", [])
    types_map = features.get("types", {})

    # Build DataModel fields dict: name -> (type, Field(...))
    data_fields: Dict[str, tuple] = {}
    # timestamp as str
    data_fields[timestamp_col] = (str, Field(..., description="ISO-8601 timestamp string"))
    for sig in signals:
        t = types_map.get(sig, "float")
        if t == "float":
            data_fields[sig] = (float, Field(...))
        elif t == "string":
            data_fields[sig] = (str, Field(...))
        else:
            # default to string to be safe
            data_fields[sig] = (str, Field(...))

    DataModel = create_model("DataModel", **data_fields, __base__=BaseModel)  # type: ignore

    # Meta model - allow extras (unknown keys)
    class MetaModel(BaseModel):
        row_hash: str
        source_id: str
        replay_mode: str | None = None
        speed_factor: float | None = None
        replayed_at: str | None = None

        model_config = {"extra": "allow"}  # accept extra metadata

    # Event wrapper
    class EventModel(BaseModel):
        data: DataModel
        meta: MetaModel

    return DataModel, MetaModel, EventModel
