"""
Prometheus metrics helper. Starts an HTTP server (prometheus_client.start_http_server)
on the configured PROMETHEUS_PORT so Prometheus can scrape metrics.

Metrics exposed:
 - ingested_rows_total (Counter)
 - validation_errors_total (Counter)
 - dlq_writes_total (Counter)
 - queue_full_events_total (Counter)
 - kafka_publish_latency_ms (Histogram)
 - kafka_publish_p95_ms (Gauge) - updated by worker periodically
"""
from __future__ import annotations
import os
from prometheus_client import Counter, Histogram, Gauge, start_http_server

PROM_PORT = int(os.environ.get("PROMETHEUS_PORT", "8001"))

ingested_rows_total = Counter("engine_module_ingested_rows_total", "Total successfully validated rows enqueued for publish")
validation_errors_total = Counter("engine_module_validation_errors_total", "Total validation failures")
dlq_writes_total = Counter("engine_module_dlq_writes_total", "Total rows written to DLQ")
queue_full_events_total = Counter("engine_module_queue_full_events_total", "Queue-full/backpressure events")
kafka_publish_latency_ms = Histogram("engine_module_kafka_publish_latency_ms", "Kafka publish latency (ms)", buckets=(1,5,10,50,100,200,500,1000,2000,5000))
kafka_publish_p95_ms = Gauge("engine_module_kafka_publish_p95_ms", "Kafka publish p95 latency (ms)")

def start_metrics_server():
    # start in background; this spawns a thread internally
    start_http_server(PROM_PORT)
