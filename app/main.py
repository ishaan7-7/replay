"""
FastAPI ingest service.

API:
 - POST /ingest/v1/events  <- accepts {"events":[ {"data":{}, "meta":{}} , ... ]}

Behavior:
 - Validates each event using dynamically built Pydantic models (from config/features.json).
 - Valid rows are enqueued to an asyncio.Queue for background publishing to Kafka.
 - If the queue is full, returns 503 (backpressure).
 - Invalid rows written to DLQ immediately and counted in metrics.
 - Background worker publishes to Kafka using aiokafka (configurable concurrency).
 - Prometheus metrics server started in background (prometheus_client.start_http_server).
"""
from __future__ import annotations
import os
import sys
import json
import asyncio
import logging
from typing import Dict, Any, List
from fastapi import FastAPI, Request, HTTPException, status
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from aiokafka import AIOKafkaProducer
import time
import statistics

from .schemas import build_models_from_features
from .dlq_writer import IngestDLQWriter
from .metrics import (
    start_metrics_server,
    ingested_rows_total,
    validation_errors_total,
    dlq_writes_total,
    queue_full_events_total,
    kafka_publish_latency_ms,
    kafka_publish_p95_ms,
)
from .utils import coerce_and_validate_data_dict

# Basic config from env or .env
KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9092")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", "engine_module")
QUEUE_MAX = int(os.environ.get("QUEUE_MAX", "50000"))
PUBLISH_WORKERS = int(os.environ.get("PUBLISH_WORKERS", "4"))
COMPRESSION_TYPE = os.environ.get("COMPRESSION_TYPE", "gzip")
LINGER_MS = int(os.environ.get("LINGER_MS", "30"))
PROM_PORT = int(os.environ.get("PROMETHEUS_PORT", "8001"))
HTTP_TIMEOUT_S = float(os.environ.get("HTTP_TIMEOUT_S", "15"))
DLQ_DIR = os.environ.get("DLQ_DIR", os.path.join(os.getcwd(), "dlq"))
FEATURES_PATH = os.environ.get("FEATURES_CONFIG", os.path.join(os.getcwd(), "config", "features.json"))

# Logging
LOG = logging.getLogger("fastapi_ingest")
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# App
app = FastAPI(title="engine_module_ingest", version="0.1")

# Global runtime objects (initialized in startup)
FEATURES: Dict[str, Any] = {}
DataModel = None
MetaModel = None
EventModel = None

_publish_queue: asyncio.Queue = None
_producer: AIOKafkaProducer = None
_dlq_writer: IngestDLQWriter = None
_worker_tasks: List[asyncio.Task] = []
_kafka_latency_samples: List[float] = []  # ms


class IngestRequest(BaseModel):
    events: List[Dict[str, Any]]


@app.on_event("startup")
async def startup():
    global FEATURES, DataModel, MetaModel, EventModel, _publish_queue, _producer, _dlq_writer, _worker_tasks

    # Load features
    try:
        with open(FEATURES_PATH, "r", encoding="utf-8") as fh:
            FEATURES = json.load(fh)
    except Exception as exc:
        LOG.error("Failed to load features.json at %s: %s", FEATURES_PATH, exc)
        raise

    DataModel, MetaModel, EventModel = build_models_from_features(FEATURES)

    # start prometheus metrics server in background thread
    start_metrics_server()
    LOG.info("Prometheus metrics server started on port %s", PROM_PORT)

    # DLQ writer
    _dlq_writer = IngestDLQWriter(DLQ_DIR, prefix="ingest_dlq")

    # queue & kafka producer
    _publish_queue = asyncio.Queue(maxsize=QUEUE_MAX)

    # create aiokafka producer - configure compression and linger via config
    _producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BOOTSTRAP, linger_ms=LINGER_MS, compression_type=COMPRESSION_TYPE)
    await _producer.start()
    LOG.info("Aiokafka producer started (%s -> %s)", KAFKA_BOOTSTRAP, KAFKA_TOPIC)

    # start background publisher workers
    for i in range(PUBLISH_WORKERS):
        t = asyncio.create_task(publisher_worker(i))
        _worker_tasks.append(t)
    LOG.info("Started %d publisher worker(s)", PUBLISH_WORKERS)


@app.on_event("shutdown")
async def shutdown():
    global _producer, _worker_tasks, _dlq_writer
    LOG.info("Shutdown requested - stopping workers and closing producer")
    # cancel workers
    for t in _worker_tasks:
        t.cancel()
    # drain queue briefly
    try:
        await asyncio.sleep(0.1)
    except Exception:
        pass
    if _producer:
        try:
            await _producer.stop()
        except Exception:
            LOG.exception("Error stopping producer")
    if _dlq_writer:
        _dlq_writer.close()
    LOG.info("Shutdown complete")


@app.post("/ingest/v1/events")
async def ingest_events(req: Request):
    """
    Accepts {"events":[ {"data": {...}, "meta": {...} }, ... ] }
    Validates each event: coerces data fields, validates timestamp and numeric types.
    Enqueues validated events for publish; returns 202 when accepted.
    Returns 422 for bad request format; 503 if queue is full (backpressure)
    """
    global _publish_queue, _dlq_writer, DataModel, MetaModel, EventModel

    payload = await req.json()
    if not isinstance(payload, dict) or "events" not in payload:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="Invalid payload; expected {'events':[...]}")

    events = payload["events"]
    if not isinstance(events, list) or len(events) == 0:
        raise HTTPException(status_code=status.HTTP_422_UNPROCESSABLE_ENTITY, detail="No events provided")

    accepted = 0
    invalid = 0
    enqueue_fail = 0

    for ev in events:
        # Basic shape check
        if not isinstance(ev, dict) or "data" not in ev or "meta" not in ev:
            validation_errors_total.inc()
            invalid += 1
            _dlq_writer.write(ev, "bad_shape")
            dlq_writes_total.inc()
            continue

        data = ev["data"]
        meta = ev["meta"]

        # Coerce & validate using utils (lightweight)
        coerced_data, err = coerce_and_validate_data_dict(data, FEATURES)
        if err:
            validation_errors_total.inc()
            invalid += 1
            _dlq_writer.write({"data": data, "meta": meta}, f"validation_error: {err}")
            dlq_writes_total.inc()
            continue

        # Build pydantic objects (this also enforces field presence types)
        try:
            data_obj = DataModel(**coerced_data)
            # allow meta to have extra keys
            meta_obj = MetaModel(**meta)
            # Optional: validate row_hash matches computed? We accept meta.row_hash but don't re-compute here (replayer already computed)
            # Build final event payload for publishing
            publish_payload = {"data": data_obj.model_dump(), "meta": meta_obj.model_dump()}
        except Exception as exc:
            validation_errors_total.inc()
            invalid += 1
            _dlq_writer.write({"data": data, "meta": meta}, f"pydantic_validation_error: {exc}")
            dlq_writes_total.inc()
            continue

        # Enqueue for publishing - backpressure if queue full
        try:
            _publish_queue.put_nowait(publish_payload)
            ingested_rows_total.inc()
            accepted += 1
        except asyncio.QueueFull:
            queue_full_events_total.inc()
            enqueue_fail += 1
            # Return 503, but still continue to process remaining events (we want to signal backpressure)
            # Write this event to DLQ so it isn't lost (depending on downstream policy)
            _dlq_writer.write(publish_payload, "enqueue_failed_queue_full")
            dlq_writes_total.inc()
            # Instead of raising immediately, we'll return 503 at end if any enqueue failed
            continue

    if enqueue_fail > 0:
        # Signal backpressure - 503
        return JSONResponse(status_code=503, content={"detail": "ingest queue full; backpressure applied", "accepted": accepted, "invalid": invalid, "enqueue_fail": enqueue_fail})

    return JSONResponse(status_code=202, content={"accepted": accepted, "invalid": invalid})


async def publisher_worker(worker_id: int):
    """
    Background worker that pulls events from _publish_queue and publishes to Kafka.
    Records latency metrics and updates p95 gauge periodically.
    """
    global _publish_queue, _producer, _dlq_writer, _kafka_latency_samples
    LOG.info("Publisher worker %d started", worker_id)
    latencies = []

    try:
        while True:
            ev = await _publish_queue.get()
            # serialize
            try:
                key = None
                md = ev.get("meta", {})
                source_id = md.get("source_id")
                if source_id is not None:
                    key = str(source_id).encode("utf-8")
                value = json.dumps(ev).encode("utf-8")
                start = time.perf_counter()
                # aiokafka send_and_wait returns when acked by broker
                await _producer.send_and_wait(KAFKA_TOPIC, value=value, key=key)
                elapsed_ms = (time.perf_counter() - start) * 1000.0
                kafka_publish_latency_ms.observe(elapsed_ms)
                latencies.append(elapsed_ms)
                _kafka_latency_samples.append(elapsed_ms)
            except Exception as exc:
                LOG.exception("Worker %d: failed to publish to kafka: %s", worker_id, exc)
                # write to dlq with error
                try:
                    _dlq_writer.write(ev, f"kafka_publish_error: {exc}")
                    dlq_writes_total.inc()
                except Exception:
                    LOG.exception("Failed to write to DLQ after kafka publish error")
            finally:
                _publish_queue.task_done()

            # Periodically update p95 metric
            if len(latencies) >= 50:
                try:
                    p95 = statistics.quantiles(latencies, n=100)[94]  # 95th percentile
                    kafka_publish_p95_ms.set(p95)
                    latencies = []
                except Exception:
                    latencies = []

    except asyncio.CancelledError:
        LOG.info("Publisher worker %d cancelled", worker_id)
    except Exception:
        LOG.exception("Unexpected error in publisher worker %d", worker_id)
    finally:
        LOG.info("Publisher worker %d exiting", worker_id)
