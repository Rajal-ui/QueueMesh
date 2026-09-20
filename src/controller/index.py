"""
QueueMesh - Controller Lambda Handler
Evaluates downstream telemetry, executes the AIMD mathematical engine,
dynamically adjusts SQS-to-Lambda Event Source Mapping MaximumConcurrency,
and records state machine transitions into the DynamoDB single-table ledger.
"""

import json
import logging
import os
import time
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Dict, Optional, Tuple

import boto3
from botocore.exceptions import ClientError

from aimd_engine import AIMDEngine, AIMDResult

# Logger configuration
logger = logging.getLogger("QueueMesh.Controller")
logger.setLevel(os.environ.get("LOG_LEVEL", "INFO").upper())

# Environment & Infrastructure Configuration
STATE_TABLE_NAME = os.environ.get("STATE_TABLE_NAME", "QueueMeshStateLedger-dev")
WORKER_ESM_UUID = os.environ.get("WORKER_ESM_UUID", "")
WORKER_FUNCTION_NAME = os.environ.get("WORKER_FUNCTION_NAME", "QueueMesh-Worker-dev")
QUEUE_ID = os.environ.get("QUEUE_ID", "QueueMesh-TargetQueue-dev")

BASELINE_MAX_CONCURRENCY = int(os.environ.get("BASELINE_MAX_CONCURRENCY", "100"))
ALPHA = int(os.environ.get("ALPHA", "5"))
BETA = float(os.environ.get("BETA", "0.50"))
T1_THRESHOLD = float(os.environ.get("T1_THRESHOLD", "0.05"))
T2_THRESHOLD = float(os.environ.get("T2_THRESHOLD", "0.98"))
C_MIN = int(os.environ.get("C_MIN", "2"))

# AWS SDK Clients
dynamodb = boto3.resource("dynamodb")
table = dynamodb.Table(STATE_TABLE_NAME)
lambda_client = boto3.client("lambda")

# Initialize AIMD Engine instance
aimd_engine = AIMDEngine(
    t1_threshold=T1_THRESHOLD,
    t2_threshold=T2_THRESHOLD,
    alpha=ALPHA,
    beta=BETA,
    c_min=C_MIN,
    c_max=BASELINE_MAX_CONCURRENCY,
)


def resolve_esm_uuid(override_uuid: Optional[str] = None) -> str:
    """
    Resolves the target Event Source Mapping UUID from:
    1. Runtime event override
    2. Environment variable
    3. Dynamic lookup via lambda:ListEventSourceMappings
    """
    if override_uuid:
        return override_uuid
    if WORKER_ESM_UUID:
        return WORKER_ESM_UUID

    logger.info(f"Resolving ESM UUID dynamically for function: {WORKER_FUNCTION_NAME}")
    try:
        response = lambda_client.list_event_source_mappings(
            FunctionName=WORKER_FUNCTION_NAME
        )
        mappings = response.get("EventSourceMappings", [])
        if mappings:
            resolved_uuid = mappings[0]["UUID"]
            logger.info(f"Discovered ESM UUID: {resolved_uuid}")
            return resolved_uuid
    except Exception as exc:
        logger.error(f"Failed to list event source mappings: {exc}")

    raise ValueError(
        f"Unable to resolve SQS EventSourceMapping UUID for {WORKER_FUNCTION_NAME}. "
        "Ensure WORKER_ESM_UUID is populated or ESM is active."
    )


def parse_event_payload(event: Dict[str, Any]) -> Tuple[float, float, str, Optional[str]]:
    """
    Extracts error_rate, success_rate, queue_id, and optional esm_uuid from diverse event triggers:
    - Direct invocation / CLI payload: {"error_rate": 0.20, "success_rate": 0.80}
    - SNS topic wrapping CloudWatch alarm
    - EventBridge scheduled rule or metric alarm
    """
    error_rate = 0.0
    success_rate = 1.0
    queue_id = QUEUE_ID
    esm_uuid = None

    # Handle SNS envelope
    if "Records" in event and event["Records"] and "Sns" in event["Records"][0]:
        sns_message = event["Records"][0]["Sns"].get("Message", "{}")
        try:
            event = json.loads(sns_message)
        except Exception:
            pass

    # Extract rates
    for key in ("error_rate", "ErrorRate", "errorRate", "downstream429Rate"):
        if key in event:
            error_rate = float(event[key])
            break

    for key in ("success_rate", "SuccessRate", "successRate", "downstream200Rate"):
        if key in event:
            success_rate = float(event[key])
            break
    else:
        # If success rate not explicitly given, compute complementary rate
        success_rate = max(0.0, 1.0 - error_rate)

    if "queue_id" in event:
        queue_id = str(event["queue_id"])
    elif "QueueName" in event:
        queue_id = str(event["QueueName"])

    if "esm_uuid" in event:
        esm_uuid = str(event["esm_uuid"])

    return error_rate, success_rate, queue_id, esm_uuid


def get_current_state(pk: str) -> Dict[str, Any]:
    """
    Queries DynamoDB Single-Table Ledger for current state item (SK = 'STATE#CURRENT').
    Returns default healthy state if item does not yet exist.
    """
    try:
        response = table.get_item(
            Key={"PK": pk, "SK": "STATE#CURRENT"},
            ConsistentRead=True,
        )
        if "Item" in response:
            return response["Item"]
    except ClientError as exc:
        logger.warning(f"DynamoDB get_item error: {exc}. Initializing with baseline state.")

    return {
        "PK": pk,
        "SK": "STATE#CURRENT",
        "CurrentState": "HEALTHY",
        "CurrentMaxConcurrency": BASELINE_MAX_CONCURRENCY,
        "ConsecutiveBreaches": 0,
    }


def update_esm_concurrency(
    esm_uuid: str, target_concurrency: int, max_retries: int = 5, base_delay: float = 1.0
) -> Tuple[bool, float]:
    """
    Executes AWS SDK update_event_source_mapping to set MaximumConcurrency.
    Handles AWS ResourceInUseException with backoff when ESM is transitioning states.
    Returns (success_flag, execution_latency_ms).
    """
    start_time = time.perf_counter()

    for attempt in range(max_retries):
        try:
            logger.info(
                f"Updating EventSourceMapping {esm_uuid} MaximumConcurrency -> {target_concurrency} "
                f"(Attempt {attempt + 1}/{max_retries})"
            )
            response = lambda_client.update_event_source_mapping(
                UUID=esm_uuid,
                ScalingConfig={"MaximumConcurrency": target_concurrency},
            )
            latency_ms = (time.perf_counter() - start_time) * 1000
            logger.info(
                f"ESM {esm_uuid} successfully updated to {target_concurrency} in {latency_ms:.2f}ms. "
                f"Status: {response.get('State', 'Updating')}"
            )
            return True, latency_ms
        except ClientError as exc:
            error_code = exc.response.get("Error", {}).get("Code", "")
            if error_code == "ResourceInUseException":
                if attempt < max_retries - 1:
                    sleep_sec = base_delay * (1.5 ** attempt)
                    logger.warning(
                        f"ESM {esm_uuid} currently in use (State=Updating). "
                        f"Retrying in {sleep_sec:.2f}s (attempt {attempt + 1}/{max_retries})..."
                    )
                    time.sleep(sleep_sec)
                    continue
                else:
                    logger.warning(
                        f"ESM {esm_uuid} is still converging from a recent update after {max_retries} attempts. "
                        "Retaining current active scaling state."
                    )
                    latency_ms = (time.perf_counter() - start_time) * 1000
                    return False, latency_ms
            raise exc

    return False, (time.perf_counter() - start_time) * 1000


def persist_state_and_audit(
    pk: str,
    esm_uuid: str,
    result: AIMDResult,
    error_rate: float,
    success_rate: float,
    latency_ms: float,
) -> None:
    """
    Saves updated state and an immutable audit record to DynamoDB.
    """
    now_iso = datetime.now(timezone.utc).isoformat()

    # 1. Update Active State item (STATE#CURRENT)
    state_item = {
        "PK": pk,
        "SK": "STATE#CURRENT",
        "CurrentState": result.next_state,
        "CurrentMaxConcurrency": result.target_concurrency,
        "PreviousMaxConcurrency": result.previous_concurrency,
        "ConsecutiveBreaches": result.consecutive_breaches,
        "LastAction": result.action,
        "LastReason": result.reason,
        "TargetEsmUuid": esm_uuid,
        "ControlLoopLatencyMs": Decimal(str(round(latency_ms, 2))),
        "LastEvaluatedTimestamp": now_iso,
    }
    table.put_item(Item=state_item)

    # 2. Append immutable Audit History item (AUDIT#<ISO_TIMESTAMP>)
    if result.target_concurrency < result.previous_concurrency:
        event_type = "CONCURRENCY_REDUCED"
    elif result.target_concurrency > result.previous_concurrency:
        event_type = "CONCURRENCY_INCREASED"
    else:
        event_type = "CONCURRENCY_UNCHANGED"

    audit_item = {
        "PK": pk,
        "SK": f"AUDIT#{now_iso}",
        "EventType": event_type,
        "PreviousValue": result.previous_concurrency,
        "NewValue": result.target_concurrency,
        "Action": result.action,
        "Reason": result.reason,
        "ErrorRate": Decimal(str(round(error_rate, 4))),
        "SuccessRate": Decimal(str(round(success_rate, 4))),
        "TargetEsmUuid": esm_uuid,
        "ControlLoopLatencyMs": Decimal(str(round(latency_ms, 2))),
        "Timestamp": now_iso,
    }
    table.put_item(Item=audit_item)


def lambda_handler(event: Dict[str, Any], context: Any) -> Dict[str, Any]:
    """
    Main entry point for QueueMesh Controller Lambda.
    """
    loop_start = time.perf_counter()
    logger.info(f"QueueMesh Controller invoked with event: {json.dumps(event)}")

    # 1. Parse incoming event parameters
    error_rate, success_rate, queue_id, event_esm_uuid = parse_event_payload(event)
    pk = f"QUEUE#{queue_id}"

    # 2. Resolve ESM UUID
    esm_uuid = resolve_esm_uuid(event_esm_uuid)

    # 3. Query current state from DynamoDB
    current_state_item = get_current_state(pk)
    current_concurrency = int(
        current_state_item.get("CurrentMaxConcurrency", BASELINE_MAX_CONCURRENCY)
    )
    consecutive_breaches = int(
        current_state_item.get("ConsecutiveBreaches", 0)
    )

    # 4. Execute AIMD calculation
    aimd_result = aimd_engine.calculate(
        current_concurrency=current_concurrency,
        error_rate=error_rate,
        success_rate=success_rate,
        consecutive_breaches=consecutive_breaches,
    )
    logger.info(f"AIMD Decision: {aimd_result}")

    # 5. Apply update via AWS Boto3 SDK if concurrency changed
    esm_latency_ms = 0.0
    esm_updated = True
    if aimd_result.target_concurrency != current_concurrency:
        esm_updated, esm_latency_ms = update_esm_concurrency(
            esm_uuid, aimd_result.target_concurrency
        )
    else:
        logger.info(
            f"MaximumConcurrency remains steady at {current_concurrency}. No SDK call needed."
        )

    # 6. Persist state and audit record to DynamoDB
    persist_state_and_audit(
        pk=pk,
        esm_uuid=esm_uuid,
        result=aimd_result,
        error_rate=error_rate,
        success_rate=success_rate,
        latency_ms=esm_latency_ms,
    )

    total_loop_time_ms = (time.perf_counter() - loop_start) * 1000

    effective_reason = (
        aimd_result.reason
        if esm_updated
        else f"{aimd_result.reason} (ESM update deferred: prior transition still in-flight)"
    )

    response_payload = {
        "statusCode": 200,
        "queue_id": queue_id,
        "esm_uuid": esm_uuid,
        "previous_concurrency": aimd_result.previous_concurrency,
        "target_concurrency": aimd_result.target_concurrency,
        "action": aimd_result.action,
        "state": aimd_result.next_state,
        "reason": effective_reason,
        "esm_updated": esm_updated,
        "execution_time_ms": round(total_loop_time_ms, 2),
    }
    logger.info(f"QueueMesh evaluation complete: {json.dumps(response_payload)}")
    return response_payload
