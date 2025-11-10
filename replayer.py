"""
replayer.py - complete, config-aware streaming replayer

Features:
 - Config precedence: CLI args > ENV vars > replayer_config.json > hardcoded defaults
 - Streams CSV with pandas chunksize; enforces canonical features.json ordering & types
 - Posts batches to ingest endpoint (JSON payload {"events":[...]})
 - Retries with exponential backoff + jitter; writes DLQ on permanent failures
 - Checkpointing to data/checkpoints/replay_<source_id>.json
 - Global rows-per-second cap (max_rps) enforced per batch
 - Windows-friendly, robust logging

Usage examples:
  python replayer.py --csv data\raw\signals.csv --source-id sim001 --config replay\replayer_config.json
  replay\replayer_service.bat data\raw\signals.csv sim001 --mode fixed_rate --speed-factor 5 --max-rps 5
"""

from __future__ import annotations
import os
import sys
import json
import time
import math
import hashlib
import random
import argparse
import logging
import datetime as dt
from typing import Dict, Any, List, Optional, Iterator, Tuple

import pandas as pd
import requests

# -----------------------
# Logging
# -----------------------
LOG = logging.getLogger("replayer")
SESSION = requests.Session()

# -----------------------
# Hard-coded safe defaults
# -----------------------
HARDCODED_DEFAULTS = {
    "default_batch_size": 50,
    "max_retries": 6,
    "backoff_base_s": 0.2,
    "default_mode": "time_scaled",
    "default_speed_factor": 1.0,
    "checkpoint_dir": "data/checkpoints",
    "dlq_dir": "dlq",
    "max_rps": 5.0,
    "log_level": "INFO",
    "http_timeout_s": 15,
    "retry_jitter_factor": 0.2,
    "max_sleep_cap_s": 60.0,
}

# -----------------------
# Utilities
# -----------------------
def setup_logging(level_str: str = "INFO"):
    level = getattr(logging, level_str.upper(), logging.INFO)
    fmt = "%(asctime)s %(levelname)s %(message)s"
    logging.basicConfig(stream=sys.stdout, level=level, format=fmt)


def now_iso() -> str:
    return dt.datetime.utcnow().isoformat() + "Z"


def sha256_hex(s: str) -> str:
    return hashlib.sha256(s.encode("utf-8")).hexdigest()


def load_json_file(path: str) -> Dict[str, Any]:
    if not path:
        return {}
    try:
        with open(path, "r", encoding="utf-8") as fh:
            return json.load(fh)
    except Exception as exc:
        LOG.warning("Failed to load JSON from %s: %s", path, exc)
        return {}


# -----------------------
# Config merging & precedence
# -----------------------
def build_effective_config(cli_args: argparse.Namespace) -> Dict[str, Any]:
    # 1) start with hardcoded defaults
    cfg = dict(HARDCODED_DEFAULTS)

    # 2) load config file if provided
    config_file_path = getattr(cli_args, "config", None)
    if config_file_path:
        file_cfg = load_json_file(config_file_path)
        if file_cfg:
            cfg.update(file_cfg)

    # 3) environment variables override
    env_map = {
        "MAX_RPS": "max_rps",
        "MAX_BATCH_SIZE": "default_batch_size",
        "MAX_RETRIES": "max_retries",
        "BACKOFF_BASE_S": "backoff_base_s",
        "RETRY_JITTER_FACTOR": "retry_jitter_factor",
        "MAX_SLEEP_CAP_S": "max_sleep_cap_s",
        "HTTP_TIMEOUT_S": "http_timeout_s",
        "LOG_LEVEL": "log_level",
    }
    for env_k, cfg_k in env_map.items():
        v = os.environ.get(env_k)
        if v is not None and v != "":
            try:
                if isinstance(cfg[cfg_k], float):
                    cfg[cfg_k] = float(v)
                elif isinstance(cfg[cfg_k], int):
                    cfg[cfg_k] = int(v)
                else:
                    # fallback string -> try numeric then string
                    try:
                        num = float(v)
                        cfg[cfg_k] = num
                    except Exception:
                        cfg[cfg_k] = v
            except Exception:
                cfg[cfg_k] = v

    # 4) CLI args override everything (if set / not None)
    # Map CLI names to cfg keys
    cli_to_cfg = {
        "batch_size": "default_batch_size",
        "max_retries": "max_retries",
        "backoff_base_s": "backoff_base_s",
        "retry_jitter_factor": "retry_jitter_factor",
        "max_sleep_cap_s": "max_sleep_cap_s",
        "http_timeout_s": "http_timeout_s",
        "max_rps": "max_rps",
        "mode": "default_mode",
        "speed_factor": "default_speed_factor",
    }
    for cli_name, cfg_key in cli_to_cfg.items():
        if hasattr(cli_args, cli_name):
            v = getattr(cli_args, cli_name)
            if v is not None:
                cfg[cfg_key] = v

    # final normalization/casting
    try:
        cfg["default_batch_size"] = int(cfg.get("default_batch_size", 50))
    except Exception:
        cfg["default_batch_size"] = 50
    try:
        cfg["max_retries"] = int(cfg.get("max_retries", 6))
    except Exception:
        cfg["max_retries"] = 6
    cfg["backoff_base_s"] = float(cfg.get("backoff_base_s", 0.2))
    cfg["retry_jitter_factor"] = float(cfg.get("retry_jitter_factor", 0.2))
    cfg["max_sleep_cap_s"] = float(cfg.get("max_sleep_cap_s", 60.0))
    cfg["http_timeout_s"] = float(cfg.get("http_timeout_s", 15.0))
    cfg["max_rps"] = float(cfg.get("max_rps", 5.0))
    cfg["default_mode"] = str(cfg.get("default_mode", "time_scaled"))
    cfg["default_speed_factor"] = float(cfg.get("default_speed_factor", 1.0))
    cfg["dlq_dir"] = str(cfg.get("dlq_dir", "dlq"))
    cfg["checkpoint_dir"] = str(cfg.get("checkpoint_dir", "data/checkpoints"))
    cfg["log_level"] = str(cfg.get("log_level", "INFO"))

    return cfg


# -----------------------
# Feature enforcement
# -----------------------
def load_features(features_path: str) -> Dict[str, Any]:
    return load_json_file(features_path)


def ensure_order_and_types_frame(df: pd.DataFrame, features: Dict[str, Any]) -> pd.DataFrame:
    timestamp_col = features.get("timestamp_col", "timestamp")
    sigs = features.get("signals", [])
    required_cols = [timestamp_col] + sigs

    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns in chunk: {missing}")

    df = df[required_cols].copy()
    types_map = features.get("types", {})
    for col, t in types_map.items():
        if col not in df.columns:
            continue
        if t == "float":
            df[col] = pd.to_numeric(df[col], errors="coerce")
        elif t == "string":
            df[col] = df[col].astype(str)
    return df


# -----------------------
# Checkpoint and DLQ
# -----------------------
class Checkpoint:
    def __init__(self, path: str):
        self.path = path
        self._data: Dict[str, int] = {}
        self._load()

    def _load(self):
        if os.path.exists(self.path):
            try:
                with open(self.path, "r", encoding="utf-8") as fh:
                    self._data = json.load(fh)
            except Exception:
                LOG.warning("Failed reading checkpoint file %s; starting fresh", self.path)
                self._data = {}
        else:
            self._data = {}

    def get_offset(self, source_id: str) -> int:
        return int(self._data.get(source_id, 0))

    def set_offset(self, source_id: str, offset: int):
        self._data[source_id] = int(offset)
        tmp = self.path + ".tmp"
        os.makedirs(os.path.dirname(self.path), exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as fh:
            json.dump(self._data, fh)
        os.replace(tmp, self.path)


class DLQWriter:
    def __init__(self, dlq_dir: str, source_id: str):
        os.makedirs(dlq_dir, exist_ok=True)
        self.source_id = source_id
        self.dlq_dir = dlq_dir
        self._open_file_for_today()

    def _open_file_for_today(self):
        today = dt.date.today().strftime("%Y%m%d")
        fname = f"replayer_dlq_{self.source_id}_{today}.jsonl"
        self.path = os.path.join(self.dlq_dir, fname)
        self._fh = open(self.path, "a", encoding="utf-8")

    def write(self, record: Dict[str, Any], error: str):
        entry = {"ts": now_iso(), "source_id": self.source_id, "error": error, "record": record}
        self._fh.write(json.dumps(entry) + "\n")
        self._fh.flush()

    def close(self):
        try:
            self._fh.close()
        except Exception:
            pass


# -----------------------
# HTTP POST with retries
# -----------------------
def post_batch_with_retries(
    url: str,
    payload: Dict[str, Any],
    max_retries: int,
    backoff_base_s: float,
    jitter_factor: float,
    http_timeout_s: float,
) -> Tuple[bool, Any]:
    attempt = 0
    while True:
        try:
            resp = SESSION.post(url, json=payload, timeout=http_timeout_s)
            if 200 <= resp.status_code < 300:
                return True, resp
            else:
                LOG.warning("Non-2xx response %s: %.200s", resp.status_code, resp.text)
                raise Exception(f"HTTP {resp.status_code}")
        except Exception as exc:
            attempt += 1
            if attempt > max_retries:
                LOG.error("Max retries exceeded for batch post: %s", exc)
                return False, exc
            backoff = backoff_base_s * (2 ** (attempt - 1))
            jitter = random.uniform(0, backoff * jitter_factor)
            sleep_for = backoff + jitter
            LOG.warning("Post attempt %d failed: %s. Sleeping %.2fs before retry", attempt, exc, sleep_for)
            time.sleep(sleep_for)


# -----------------------
# Event creation & validation
# -----------------------
def make_event_from_row(row: pd.Series, features: Dict[str, Any], source_id: str, replay_mode: str, speed_factor: float) -> Dict[str, Any]:
    row_dict = row.to_dict()
    timestamp_col = features.get("timestamp_col", "timestamp")
    sigs = features.get("signals", [])
    ordered_cols = [timestamp_col] + sigs
    vals = ["" if pd.isna(row_dict.get(c)) else str(row_dict.get(c)) for c in ordered_cols]
    concat = "|".join(ordered_cols) + "|" + "|".join(vals)
    row_hash = sha256_hex(concat)
    meta = {"row_hash": row_hash, "source_id": source_id, "replay_mode": replay_mode, "speed_factor": speed_factor, "replayed_at": now_iso()}
    return {"data": row_dict, "meta": meta}


def validate_row(row: pd.Series, features: Dict[str, Any]) -> Optional[str]:
    timestamp_col = features.get("timestamp_col", "timestamp")
    types = features.get("types", {})
    ts = row.get(timestamp_col)
    if pd.isna(ts) or str(ts).strip() == "":
        return "Missing timestamp"
    try:
        pd.to_datetime(ts)
    except Exception:
        return f"Invalid timestamp format: {ts}"
    for col, t in types.items():
        if col == timestamp_col:
            continue
        if t == "float":
            v = row.get(col)
            if pd.isna(v):
                return f"Column {col} not numeric/coercible"
    return None


# -----------------------
# CSV streaming helper
# -----------------------
def stream_csv_chunks(csv_path: str, chunksize: int) -> Iterator[pd.DataFrame]:
    return pd.read_csv(csv_path, chunksize=chunksize, low_memory=False)


# -----------------------
# Main replay loop
# -----------------------
def run_replay(
    csv_path: str,
    source_id: str,
    ingest_url: str,
    features_path: str,
    cfg: Dict[str, Any],
    start_offset: int = 0,
):
    features = load_features(features_path)
    timestamp_col = features.get("timestamp_col", "timestamp")

    # checkpoint setup
    checkpoint_path = cfg.get("checkpoint_dir", "data/checkpoints")
    os.makedirs(checkpoint_path, exist_ok=True)
    checkpoint_file = os.path.join(checkpoint_path, f"replay_{source_id}.json")
    ckpt = Checkpoint(checkpoint_file)
    offset = max(start_offset, ckpt.get_offset(source_id))

    # DLQ
    dlq_dir = cfg.get("dlq_dir", "dlq")
    dlq = DLQWriter(dlq_dir, source_id)

    # streaming
    batch_size = int(cfg.get("default_batch_size", 50))
    max_retries = int(cfg.get("max_retries", 6))
    backoff_base_s = float(cfg.get("backoff_base_s", 0.2))
    jitter_factor = float(cfg.get("retry_jitter_factor", 0.2))
    http_timeout_s = float(cfg.get("http_timeout_s", 15.0))
    max_sleep_cap_s = float(cfg.get("max_sleep_cap_s", 60.0))
    max_rps = float(cfg.get("max_rps", 5.0))
    mode = str(cfg.get("default_mode", "time_scaled"))
    speed_factor = float(cfg.get("default_speed_factor", 1.0))

    LOG.info(
        "Starting replay csv=%s source=%s ingest=%s features=%s batch_size=%d mode=%s speed_factor=%s max_rps=%s",
        csv_path,
        source_id,
        ingest_url,
        features_path,
        batch_size,
        mode,
        speed_factor,
        max_rps,
    )

    reader = pd.read_csv(csv_path, chunksize=max(1, batch_size), low_memory=False)
    global_row_idx = 0
    # fast-forward: skip chunks until offset
    try:
        for chunk in reader:
            chunk_len = len(chunk)
            if global_row_idx + chunk_len <= offset:
                global_row_idx += chunk_len
                continue

            # slice chunk if offset falls inside
            if offset > global_row_idx:
                start_in_chunk = offset - global_row_idx
                chunk = chunk.iloc[start_in_chunk:].reset_index(drop=True)
                chunk_len = len(chunk)
                global_row_idx = offset

            # enforce order & types on this chunk
            try:
                chunk = ensure_order_and_types_frame(chunk, features)
            except Exception as exc:
                LOG.error("Chunk failed feature enforcement: %s. Writing chunk rows to DLQ", exc)
                for _, r in chunk.iterrows():
                    dlq.write(r.to_dict(), f"feature_enforce_error: {exc}")
                global_row_idx += chunk_len
                ckpt.set_offset(source_id, global_row_idx)
                continue

            # prepare timestamps for time_scaled mode
            try:
                chunk_timestamps = pd.to_datetime(chunk[timestamp_col], errors="coerce")
            except Exception:
                chunk_timestamps = pd.Series([pd.NaT] * chunk_len)

            # process chunk in internal sub-batches
            i = 0
            while i < chunk_len:
                sub_end = min(i + batch_size, chunk_len)
                batch_df = chunk.iloc[i:sub_end]
                events = []
                rows_processed_in_batch = 0
                for _, row in batch_df.iterrows():
                    # validate
                    err = validate_row(row, features)
                    if err:
                        LOG.warning("Validation failed (approx index %d): %s", global_row_idx, err)
                        dlq.write(row.to_dict(), err)
                        global_row_idx += 1
                        continue
                    ev = make_event_from_row(row, features, source_id, mode, speed_factor)
                    events.append(ev)
                    global_row_idx += 1
                    rows_processed_in_batch += 1

                if not events:
                    ckpt.set_offset(source_id, global_row_idx)
                    i = sub_end
                    continue

                # POST with retries
                payload = {"events": events}
                ok, resp_or_exc = post_batch_with_retries(
                    ingest_url, payload, max_retries=max_retries, backoff_base_s=backoff_base_s, jitter_factor=jitter_factor, http_timeout_s=http_timeout_s
                )
                if not ok:
                    LOG.error("Permanent failure posting batch: %s. Writing %d events to DLQ", resp_or_exc, len(events))
                    for ev in events:
                        dlq.write(ev, f"post_failure: {resp_or_exc}")
                    # checkpoint past these events to avoid retry storm on restart
                    ckpt.set_offset(source_id, global_row_idx)
                    i = sub_end
                    continue

                LOG.info("Posted %d events (global_idx now %d) status=%s", len(events), global_row_idx, getattr(resp_or_exc, "status_code", "OK"))
                ckpt.set_offset(source_id, global_row_idx)

                # Pacing: compute base_sleep from mode, then enforce global max_rps cap
                base_sleep = 0.0
                if mode == "fixed_rate":
                    desired_rps = max(1e-6, float(speed_factor))
                    base_sleep = rows_processed_in_batch / desired_rps
                elif mode == "time_scaled":
                    # Attempt to find the delta between last row of batch and next row (if available)
                    last_in_batch_idx = i + (rows_processed_in_batch - 1)
                    next_idx = last_in_batch_idx + 1
                    try:
                        if last_in_batch_idx < len(chunk_timestamps):
                            last_ts = chunk_timestamps.iloc[last_in_batch_idx]
                        else:
                            last_ts = pd.NaT
                        if next_idx < len(chunk_timestamps):
                            next_ts = chunk_timestamps.iloc[next_idx]
                        else:
                            next_ts = pd.NaT
                        if pd.isna(last_ts) or pd.isna(next_ts):
                            base_sleep = 0.0
                        else:
                            orig_delta = (next_ts - last_ts).total_seconds()
                            if orig_delta < 0:
                                orig_delta = 0.0
                            base_sleep = orig_delta / max(1.0, float(speed_factor))
                    except Exception:
                        base_sleep = 0.0
                else:
                    base_sleep = 0.0

                required_time_for_batch = rows_processed_in_batch / max(1e-6, max_rps)
                sleep_for = max(base_sleep, required_time_for_batch)

                # enforce safety cap
                if sleep_for > max_sleep_cap_s:
                    LOG.warning("Computed sleep_for %.1fs exceeds cap %.1fs; clamping", sleep_for, max_sleep_cap_s)
                    sleep_for = max_sleep_cap_s

                if sleep_for > 0:
                    LOG.debug("Sleeping %.3fs after sending %d rows (base_sleep=%.3f required_for_cap=%.3f)", sleep_for, rows_processed_in_batch, base_sleep, required_time_for_batch)
                    time.sleep(sleep_for)

                i = sub_end

    except KeyboardInterrupt:
        LOG.info("Interrupted by user; saved checkpoint and exiting.")
    finally:
        dlq.close()
        LOG.info("Replayer finished for source=%s", source_id)


# -----------------------
# CLI parser
# -----------------------
def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="CSV replayer with config/env/CLI precedence and global RPS cap.")
    p.add_argument("--csv", required=True, help="Path to CSV file")
    p.add_argument("--source-id", required=True, help="Source id used for checkpoint/DLQ naming")
    p.add_argument("--ingest-url", default=None, help="Full HTTP ingest endpoint (overrides env HOST/PORT/ENDPOINT)")
    p.add_argument("--features", default=None, help="Path to config/features.json (defaults to env FEATURES_CONFIG or config/features.json)")
    p.add_argument("--config", default=None, help="Path to replayer_config.json")
    p.add_argument("--batch-size", type=int, default=None, dest="batch_size", help="Override config batch size")
    p.add_argument("--mode", choices=["fixed_rate", "time_scaled", "burst"], default=None, help="Pacing mode override")
    p.add_argument("--speed-factor", type=float, default=None, help="Speed factor override")
    p.add_argument("--max-retries", type=int, default=None, help="Max retries override")
    p.add_argument("--backoff-base-s", type=float, default=None, help="Backoff base seconds override")
    p.add_argument("--retry-jitter-factor", type=float, default=None, help="Retry jitter factor override")
    p.add_argument("--max-sleep-cap-s", type=float, default=None, help="Max sleep cap seconds override")
    p.add_argument("--http-timeout-s", type=float, default=None, help="HTTP timeout seconds override")
    p.add_argument("--max-rps", type=float, default=None, help="Global cap rows/sec override")
    p.add_argument("--checkpoint", default=None, help="Explicit checkpoint directory override")
    p.add_argument("--dlq-dir", default=None, help="Explicit DLQ directory override")
    p.add_argument("--start-offset", type=int, default=0, help="Start row index offset to override checkpoint")
    return p.parse_args()


# -----------------------
# Entry point
# -----------------------
def main():
    args = parse_args()

    # Determine ingest URL (env fallback)
    env = os.environ
    if args.ingest_url:
        ingest_url = args.ingest_url
    else:
        host = env.get("INGEST_HOST", "127.0.0.1")
        port = env.get("INGEST_PORT", "8000")
        endpoint = env.get("INGEST_ENDPOINT", "/ingest/v1/events")
        ingest_url = f"http://{host}:{port}{endpoint}"

    features_path = args.features or env.get("FEATURES_CONFIG") or os.path.join(os.getcwd(), "config", "features.json")

    # Build effective config with precedence
    cfg = build_effective_config(args)

    # Apply explicit CLI overrides for checkpoint & dlq dirs if provided
    if args.checkpoint:
        cfg["checkpoint_dir"] = args.checkpoint
    if args.dlq_dir:
        cfg["dlq_dir"] = args.dlq_dir

    # Set up logging level now that config is known
    setup_logging(cfg.get("log_level", "INFO"))

    LOG.info("Effective config: %s", {k: cfg[k] for k in ["default_batch_size", "max_retries", "backoff_base_s", "retry_jitter_factor", "max_sleep_cap_s", "http_timeout_s", "max_rps", "default_mode", "default_speed_factor"]})

    run_replay(
        csv_path=args.csv,
        source_id=args.source_id,
        ingest_url=ingest_url,
        features_path=features_path,
        cfg=cfg,
        start_offset=args.start_offset,
    )


if __name__ == "__main__":
    main()
