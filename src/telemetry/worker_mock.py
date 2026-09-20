"""
QueueMesh - Worker Lambda Mock Telemetry Generator
Simulates worker processing SQS records, making mock downstream API calls,
and emitting CloudWatch Embedded Metric Format (EMF) logs for downstream HTTP status codes.
"""

import json
import os
import random
import time
from typing import Any, Dict, List

# Target configuration from environment
QUEUE_NAME: str = os.environ.get("QUEUE_NAME", "QueueMesh-TargetQueue-dev")
TARGET_VENDOR: str = os.environ.get("TARGET_VENDOR", "MockVendor")

# Failure injection ratio: 80% HTTP 200 OK, 20% HTTP 429 Too Many Requests
HTTP_200_RATIO: float = 0.80


def emit_emf_metrics(queue_name: str, target_vendor: str, count_200: int, count_429: int) -> None:
    """
    Emits CloudWatch Embedded Metric Format (EMF) JSON directly to stdout.
    CloudWatch automatically parses stdout and converts these into zero-latency metrics.
    """
    timestamp_ms = int(time.time() * 1000)
    emf_payload = {
        "_aws": {
            "Timestamp": timestamp_ms,
            "CloudWatchMetrics": [
                {
                    "Namespace": "QueueMesh/Telemetry",
                    "Dimensions": [["QueueName", "TargetVendor"]],
                    "Metrics": [
                        {"Name": "Downstream200Count", "Unit": "Count"},
                        {"Name": "Downstream429Count", "Unit": "Count"},
                    ],
                }
            ],
        },
        "QueueName": queue_name,
        "TargetVendor": target_vendor,
        "Downstream200Count": count_200,
        "Downstream429Count": count_429,
    }
    # Direct print to stdout formatted as single-line JSON string per EMF specification
    print(json.dumps(emf_payload, separators=(",", ":")))


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Worker Lambda handler processing SQS batch records.
    Returns batchItemFailures for messages encountering simulated downstream 429s.
    """
    records: List[Dict[str, Any]] = event.get("Records", [])
    total_records = len(records)

    count_200 = 0
    count_429 = 0
    batch_item_failures: List[Dict[str, str]] = []

    # If invoked directly with an empty batch (e.g., test ping), emit sample telemetry
    if total_records == 0:
        roll = random.random()
        if roll < HTTP_200_RATIO:
            count_200 = 1
        else:
            count_429 = 1
        emit_emf_metrics(QUEUE_NAME, TARGET_VENDOR, count_200, count_429)
        return {
            "statusCode": 200,
            "message": "Direct invocation telemetry emitted",
            "count_200": count_200,
            "count_429": count_429,
        }

    for record in records:
        message_id = record.get("messageId", "")
        # Simulate downstream API call
        # 80% Success (200), 20% Throttled (429)
        roll = random.random()
        if roll < HTTP_200_RATIO:
            count_200 += 1
        else:
            count_429 += 1
            if message_id:
                # SQS ReportBatchItemFailures: keeps message in SQS for backpressure retry
                batch_item_failures.append({"itemIdentifier": message_id})

    # Emit CloudWatch EMF telemetry for this batch
    emit_emf_metrics(QUEUE_NAME, TARGET_VENDOR, count_200, count_429)

    # Return standard partial batch failure response
    return {"batchItemFailures": batch_item_failures}
