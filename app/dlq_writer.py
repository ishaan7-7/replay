"""
DLQ writer for ingest service - rotates by day.
Writes one JSONL entry per failed record with metadata and error message.
"""
from __future__ import annotations
import os
import json
import datetime as dt
from typing import Dict, Any

class IngestDLQWriter:
    def __init__(self, dlq_dir: str, prefix: str = "ingest_dlq"):
        self.dlq_dir = dlq_dir
        os.makedirs(dlq_dir, exist_ok=True)
        self.prefix = prefix
        self._open_today()

    def _open_today(self):
        today = dt.date.today().strftime("%Y%m%d")
        self.path = os.path.join(self.dlq_dir, f"{self.prefix}_{today}.jsonl")
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, record: Dict[str, Any], error: str):
        entry = {"ts": dt.datetime.utcnow().isoformat() + "Z", "error": error, "record": record}
        self._fh.write(json.dumps(entry) + "\n")
        self._fh.flush()

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass
