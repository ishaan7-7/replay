#!/usr/bin/env python3
"""
Improved consumer_delta.py

Features:
 - Reads Kafka via Spark Structured Streaming
 - Idempotent writes into Delta (row_hash + MERGE)
 - Creates Delta table on first-run, handles non-delta directory cases
 - Optional backfill mode: captures target end offsets at startup (kafka-python required)
 - Stops automatically when backfill reaches captured end offsets (if enabled)
 - Prometheus metrics + backfill_done gauge
 - Avoids cloudpickle issues by keeping foreachBatch function top-level
"""

import os
import json
import logging
import time
import threading
from datetime import datetime, timezone
from pathlib import Path

# Prometheus (driver)
from prometheus_client import start_http_server, Counter, Gauge, Histogram

# PySpark / Delta
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType
from pyspark.sql.functions import col, concat_ws, sha2, lit
from delta.tables import DeltaTable

# Optional kafka admin client (kafka-python)
try:
    from kafka import KafkaConsumer, KafkaAdminClient
    from kafka.structs import TopicPartition
    KAFKA_PY_AVAILABLE = True
except Exception:
    KAFKA_PY_AVAILABLE = False

# ---------- Logging ----------
LOG = logging.getLogger("engine_module_delta_consumer")
logging.basicConfig(format="%(asctime)s %(levelname)s %(message)s")
LOG.setLevel(logging.INFO)

# ---------- Configuration from env with sane defaults ----------
PROJECT_ROOT = Path(os.environ.get("PROJECT_ROOT", r"C:\engine_module_pipeline"))
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "engine_module")
CONSUMER_GROUP = os.environ.get("CONSUMER_GROUP", "engine_module_backfill_fresh_v1")
BACKFILL_MODE = os.environ.get("BACKFILL_MODE", "true").lower() in ("1", "true", "yes")
CHECKPOINT_DIR = Path(os.environ.get("CHECKPOINT_DIR", PROJECT_ROOT / "data" / "checkpoints"))
DELTA_BASE = Path(os.environ.get("DELTA_BASE", PROJECT_ROOT / "delta"))
DELTA_TABLE_PATH = Path(os.environ.get("DELTA_TABLE_PATH", DELTA_BASE / f"{KAFKA_TOPIC}_delta"))
BATCH_MAX_WAIT_S = int(float(os.environ.get("BATCH_MAX_WAIT_S", "5.0")))
BATCH_SIZE = int(os.environ.get("BATCH_SIZE", "500"))
PROM_PORT = int(os.environ.get("PROMETHEUS_PORT", "8001"))

# Per-consumer checkpoint path ensures fresh consumer group behavior
CONSUMER_CHECKPOINT = CHECKPOINT_DIR / CONSUMER_GROUP

# Backfill marker file path (when reached target offsets)
BACKFILL_MARKER = PROJECT_ROOT / f".backfill_done.{CONSUMER_GROUP}"

# ---------- Prometheus metrics ----------
BATCHES = Counter("engine_module_batches_total", "Number of processed micro-batches")
ROWS = Counter("engine_module_rows_total", "Number of rows processed into Delta")
MALFORMED = Counter("engine_module_malformed_total", "Number of malformed rows seen")
ERRORS = Counter("engine_module_errors_total", "Number of errors during processing")
BACKFILL_DONE = Gauge("engine_module_backfill_done", "Flag: backfill finished (1=yes,0=no)")
BATCH_TIME = Histogram("engine_module_batch_duration_seconds", "Micro-batch processing time (s)")

# Try to start HTTP metrics server (best-effort)
try:
    start_http_server(PROM_PORT)
    LOG.info("Prometheus metrics server started on %s", PROM_PORT)
except Exception:
    LOG.exception("Could not start Prometheus server on %s", PROM_PORT)

# ---------- helper: create spark session ----------
def create_spark_session(app_name="consumer_delta"):
    builder = (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.shuffle.partitions", "2")
        # Delta extensions (requires delta-core on classpath)
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    )
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark

# ---------- helper: capture end offsets using kafka-python ----------
def capture_end_offsets(topic: str, bootstrap_servers: str):
    """
    Returns dict {partition: end_offset} captured at time of call.
    Requires kafka-python installed and accessible.
    """
    if not KAFKA_PY_AVAILABLE:
        LOG.warning("kafka-python not available; cannot capture end offsets automatically.")
        return None
    try:
        admin = KafkaConsumer(bootstrap_servers=bootstrap_servers, group_id=None,
                              enable_auto_commit=False)
        partitions = admin.partitions_for_topic(topic)
        if not partitions:
            LOG.warning("Topic '%s' not found or no partitions.", topic)
            return None
        tps = [TopicPartition(topic, p) for p in partitions]
        admin.assign(tps)
        # Seek to end to get offsets
        admin.poll(timeout_ms=100)  # ensure assignment
        end_offsets = {}
        for tp in tps:
            admin.seek_to_end(tp)
            # position returns offset after last message, we treat that as target (exclusive)
            end_offsets[tp.partition] = admin.position(tp)
        admin.close()
        LOG.info("Captured end offsets for topic %s: %s", topic, end_offsets)
        return end_offsets
    except Exception:
        LOG.exception("Failed to capture end offsets (kafka-python required).")
        return None

# ---------- top-level foreachBatch function (must be top-level to avoid pickling closures) ----------
def process_batch(batch_df, batch_id):
    """
    This function executes on the driver (foreachBatch).
    Avoid capturing heavy or non-serializable globals.
    """
    start = time.time()
    BATCHES.inc()
    LOG.info("Processing batch %s", batch_id)
    try:
        # Quick emptiness check without heavy operations
        # Using rdd.isEmpty() can trigger pickling issues; use take(1)
        first = None
        try:
            first = batch_df.take(1)
        except Exception:
            # fallback: attempt count as last resort
            try:
                if batch_df.rdd.getNumPartitions() == 0:
                    first = []
                else:
                    first = batch_df.count() and [1] or []
            except Exception:
                first = None

        if not first:
            LOG.info("Batch %s empty - nothing to do", batch_id)
            BATCH_TIME.observe(time.time() - start)
            return

        # Normalize and parse fields
        df = batch_df.selectExpr(
            "CAST(key AS STRING) AS kafka_key",
            "CAST(value AS STRING) AS payload",
            "topic",
            "partition",
            "offset",
            "timestamp"
        )

        # mark generated_at UTC
        df = df.withColumn("generated_at", lit(datetime.now(timezone.utc).isoformat()))

        # malformed
        malformed_df = df.filter("payload IS NULL OR length(payload) = 0")
        mal_count = 0
        try:
            mal_count = malformed_df.count()
        except Exception:
            LOG.debug("Could not count malformed rows")
        if mal_count:
            MALFORMED.inc(mal_count)
            LOG.warning("Batch %s had %d malformed rows", batch_id, mal_count)

        # compute row_hash (dedupe by key+payload). Exclude offset if you want content-level dedupe.
        df = df.withColumn("row_hash", sha2(concat_ws("||", col("kafka_key").cast(StringType()), col("payload")), 256))

        # Ensure delta folder parent exists
        DELTA_TABLE_PATH.parent.mkdir(parents=True, exist_ok=True)

        # If a path exists but isn't a delta table, move it aside (avoid accidental merges)
        delta_path_str = str(DELTA_TABLE_PATH)
        try:
            if DeltaTable.isDeltaTable(batch_df.sparkSession, delta_path_str):
                LOG.info("Delta table exists; MERGE incoming batch (idempotent insert)")
                dt = DeltaTable.forPath(batch_df.sparkSession, delta_path_str)
                (dt.alias("t")
                   .merge(df.alias("s"), "t.row_hash = s.row_hash")
                   .whenNotMatchedInsertAll()
                   .execute()
                )
            else:
                # Path either does not exist or not a Delta table.
                if os.path.exists(delta_path_str) and os.listdir(delta_path_str):
                    # Path exists but not delta -> back it up then create
                    backup = delta_path_str + ".bak." + datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
                    LOG.warning("Path %s exists but is not a Delta table. Backing up to %s and creating new Delta", delta_path_str, backup)
                    os.rename(delta_path_str, backup)
                LOG.info("Creating new Delta table at %s", delta_path_str)
                (df.write.format("delta").mode("overwrite").option("overwriteSchema", "true").save(delta_path_str))
                LOG.info("Delta table created at %s", delta_path_str)
        except Exception:
            ERRORS.inc()
            LOG.exception("Error while writing/merging to Delta at %s", delta_path_str)
            raise

        # Update rows metric (count)
        try:
            cnt = df.count()
            ROWS.inc(cnt)
            LOG.info("Wrote/merged %d rows from batch %s", cnt, batch_id)
        except Exception:
            LOG.debug("Could not count rows after write")

        # If we captured target offsets at startup (saved as JSON), check if we've reached them.
        try:
            # read per-partition max offset seen in this batch
            offsets = (df.groupBy("partition").agg({"offset": "max"}).collect())
            # convert to dict partition -> max_offset_in_batch
            batch_max_offsets = {int(r["partition"]): int(r["max(offset)"]) for r in offsets}
            # persist latest seen offsets to a small local file for monitor thread
            latest_offsets_file = PROJECT_ROOT / f".latest_offsets.{CONSUMER_GROUP}.json"
            prev = {}
            if latest_offsets_file.exists():
                try:
                    prev = json.loads(latest_offsets_file.read_text())
                except Exception:
                    prev = {}
            for p, o in batch_max_offsets.items():
                prev[str(p)] = max(prev.get(str(p), -1), o)
            latest_offsets_file.write_text(json.dumps(prev))
            LOG.debug("Updated latest offsets snapshot: %s", prev)
        except Exception:
            LOG.debug("Could not compute latest offsets snapshot for batch %s", batch_id)

        LOG.info("Batch %s processed successfully", batch_id)
    except Exception:
        ERRORS.inc()
        LOG.exception("Unhandled exception in process_batch for batch %s", batch_id)
        raise
    finally:
        elapsed = time.time() - start
        BATCH_TIME.observe(elapsed)
        LOG.info("Finished processing batch %s in %.3fs", batch_id, elapsed)


# ---------- monitor thread to auto-stop when backfill done ----------
def monitor_backfill_and_stop(query, target_offsets):
    """
    Polls a small "latest offsets" file (written by process_batch) and compares
    to the target_offsets captured at startup. When all partitions >= target, create marker
    and call query.stop().
    Runs in a background thread on driver.
    """
    if not target_offsets:
        LOG.info("No target_offsets provided; monitor will not auto-stop.")
        return

    LOG.info("Starting backfill monitor thread (auto-stop when reaching target offsets).")
    latest_file = PROJECT_ROOT / f".latest_offsets.{CONSUMER_GROUP}.json"
    try:
        while True:
            if BACKFILL_MARKER.exists():
                LOG.info("Backfill marker already present; stopping monitor.")
                break
            if not latest_file.exists():
                time.sleep(1)
                continue
            try:
                cur = json.loads(latest_file.read_text())
                # check all partitions present and reached target
                all_reached = True
                for p_str, target in target_offsets.items():
                    p = str(p_str)
                    cur_offset = int(cur.get(p, -1))
                    # target is exclusive end offset, we consider reached when cur_offset >= target - 1
                    if cur_offset < (int(target) - 1):
                        all_reached = False
                        break
                if all_reached:
                    LOG.info("Backfill reached target offsets: creating marker %s and stopping query", BACKFILL_MARKER)
                    try:
                        BACKFILL_DONE.set(1)
                        BACKFILL_MARKER.write_text(json.dumps({"stopped_at": datetime.now(timezone.utc).isoformat()}))
                    except Exception:
                        LOG.exception("Failed to write backfill marker file")
                    try:
                        query.stop()
                        LOG.info("Streaming query stopped by monitor (backfill complete).")
                    except Exception:
                        LOG.exception("Failed to stop streaming query from monitor")
                    break
            except Exception:
                LOG.exception("Monitor thread error parsing latest offsets")
            time.sleep(1)
    except Exception:
        LOG.exception("Backfill monitor thread exited with error")


# ---------- main entrypoint ----------
def main():
    LOG.info("Starting consumer_delta (group=%s) BACKFILL_MODE=%s", CONSUMER_GROUP, BACKFILL_MODE)
    # Ensure folders exist
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)
    DELTA_BASE.mkdir(parents=True, exist_ok=True)
    PROJECT_ROOT.mkdir(parents=True, exist_ok=True)

    # Capture target offsets if backfill mode on (best-effort; uses kafka-python)
    target_offsets = None
    if BACKFILL_MODE:
        LOG.info("BACKFILL_MODE enabled; attempting to capture topic end offsets (kafka-python required)")
        try:
            target_offsets = capture_end_offsets(KAFKA_TOPIC, KAFKA_BOOTSTRAP_SERVERS)
            if target_offsets is None:
                LOG.warning("Could not capture topic end offsets; monitor auto-stop will be disabled.")
        except Exception:
            LOG.exception("Exception while capturing end offsets; continuing without target offsets.")

    spark = create_spark_session("consumer_delta_" + CONSUMER_GROUP)

    starting_offsets = "earliest" if BACKFILL_MODE else "latest"
    LOG.info("Using startingOffsets=%s; checkpoint=%s", starting_offsets, str(CONSUMER_CHECKPOINT))

    # Build reader
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("kafka.group.id", CONSUMER_GROUP)
        .option("maxOffsetsPerTrigger", str(BATCH_SIZE))
        .option("kafka.max.poll.records", str(BATCH_SIZE))
    )

    kafka_df = reader.load()

    # Start the streaming query using top-level process_batch
    query = (
        kafka_df.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", str(CONSUMER_CHECKPOINT))
        .trigger(processingTime=f"{BATCH_MAX_WAIT_S} seconds")
        .start()
    )

    # If we captured target offsets, spawn monitor thread to stop query when reached
    if BACKFILL_MODE and target_offsets:
        t = threading.Thread(target=monitor_backfill_and_stop, args=(query, target_offsets), daemon=True)
        t.start()

    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        LOG.info("KeyboardInterrupt -> stopping query")
        try:
            query.stop()
        except Exception:
            LOG.exception("Error on stopping query on KeyboardInterrupt")
    except Exception:
        LOG.exception("Streaming query failed unexpectedly")
        raise
    finally:
        LOG.info("Shutting down Spark session")
        try:
            spark.stop()
        except Exception:
            LOG.exception("Error stopping Spark")

if __name__ == "__main__":
    main()
