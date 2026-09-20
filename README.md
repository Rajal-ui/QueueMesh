# QueueMesh

> **Serverless Adaptive Backpressure Controller for Amazon SQS and AWS Lambda**  
> Dynamically regulates consumer concurrency in sub-second response times using real-time downstream HTTP error telemetry and the Additive Increase / Multiplicative Decrease (AIMD) algorithm.

---

## 1. Executive Summary

Modern serverless architectures frequently process asynchronous events through Amazon SQS coupled with AWS Lambda. When worker Lambdas make outbound requests to rate-limited downstream APIs (e.g., Stripe, Twilio, SendGrid, Shopify) or relational databases, high burst traffic quickly triggers **HTTP 429 (Too Many Requests)** errors.

In standard AWS architectures, failed messages are retried as message visibility timeouts expire. As Lambda concurrency scales out to drain queue depth, it hammers downstream endpoints with higher request volume, initiating a destructive **retry storm**:

```text
[High Queue Depth] ──► [Worker Concurrency Scales Up] ──► [Downstream API Throttles (HTTP 429)]
        ▲                                                                  │
        └────────────── [Visibility Timeout Retry Storm] ◄─────────────────┘
```

**QueueMesh** solves this problem without modifying worker application source code. It deploys an automated, closed-loop control plane that intercepts downstream error telemetry via **CloudWatch Embedded Metric Format (EMF)** and dynamically adjusts the SQS-to-Lambda Event Source Mapping `MaximumConcurrency` setting in **<1.5 seconds**.

---

## 2. Solution Architecture

```text
                               +-----------------------------+
                               |     Target SQS Queue        |
                               +--------------+--------------+
                                              |
                                 Event Source Mapping (ESM)
                                 Dynamic MaximumConcurrency
                                              |
                                              v
+-----------------------------+       +-----------------------------+
|    Downstream Third-Party   |<------|     AWS Lambda Worker       |
|    API (Stripe, Twilio)     |       | (Emits CloudWatch EMF Logs) |
+--------------+--------------+       +--------------+--------------+
               |                                     |
         HTTP 200 / 429                        CloudWatch EMF
               |                                     |
               +----------------------+--------------+
                                      |
                                      v
                      +-------------------------------+
                      |      QueueMesh Controller     |
                      |          (Python 3.11)        |
                      +---------------+---------------+
                                      |
                   +------------------+------------------+
                   |                                     |
                   v                                     v
+------------------------------------+  +--------------------------------+
|  lambda:UpdateEventSourceMapping   |  |   Amazon DynamoDB State Ledger |
|  ScalingConfig: MaximumConcurrency |  |   (PAY_PER_REQUEST, Single Table)  |
+------------------------------------+  +--------------------------------+
```

---

## 3. Control-Loop Logic: AIMD Algorithm

QueueMesh adapts TCP congestion control mathematics (**Additive Increase / Multiplicative Decrease**) specifically for AWS Lambda event source mappings:

```text
                         Downstream Error Telemetry
                                     │
                     ┌───────────────┴───────────────┐
                     ▼                               ▼
              HTTP 429 Spike                  HTTP 200 Healthy
             (Error Rate >= T1)              (Success Rate >= T2)
                     │                               │
                     ▼                               ▼
          Multiplicative Decrease             Additive Increase
    C_next = max(C_min, └C_current * β┘)    C_next = min(C_max, C_current + α)
```

### Parameter Thresholds Matrix

| Variable | Definition | Default Value | Description |
| :--- | :--- | :--- | :--- |
| **T1** | Throttle Trigger Threshold | `0.05` (5%) | Error rate that triggers multiplicative decrease |
| **T2** | Recovery Trigger Threshold | `0.98` (98%) | Success rate required to begin additive recovery |
| **β** | Multiplicative Decrease Factor | `0.50` | Halves concurrency upon rate-limit breach |
| **α** | Additive Increase Step Size | `5 workers` | Linear step increase per healthy evaluation cycle |
| **C_min** | Minimum Concurrency Floor | `2 workers` | SQS ESM minimum supported concurrency floor |
| **C_max** | Maximum Concurrency Ceiling | `100 workers`| Baseline concurrency configured for the queue |

### Finite State Machine

* **`HEALTHY`**: System operates at baseline capacity (`C_max`).
* **`THROTTLED`**: First rate-limit breach detected; concurrency scaled down via factor $\beta$.
* **`CRITICAL_BACKOFF`**: Consecutive breaches detected; aggressive throttle down toward $C_{min}$.
* **`RECOVERING`**: Error rate normalized; concurrency systematically stepped up by $+\alpha$.

---

## 4. DynamoDB Single-Table Ledger Schema

QueueMesh tracks state and maintains an immutable audit log inside the `QueueMeshStateLedger` table:

| Entity Type | PK | SK | Attributes |
| :--- | :--- | :--- | :--- |
| **Active State** | `QUEUE#<QueueName>` | `STATE#CURRENT` | `CurrentState`, `CurrentMaxConcurrency`, `PreviousMaxConcurrency`, `ConsecutiveBreaches`, `LastAction`, `LastReason`, `TargetEsmUuid`, `LastEvaluatedTimestamp` |
| **Audit Log** | `QUEUE#<QueueName>` | `AUDIT#<Timestamp>` | `EventType`, `PreviousValue`, `NewValue`, `Action`, `Reason`, `ErrorRate`, `SuccessRate`, `ControlLoopLatencyMs`, `Timestamp` |

---

## 5. Repository Structure

```text
QueueMesh/
├── infrastructure/
│   └── template.yaml              # AWS SAM Infrastructure Template
├── src/
│   ├── controller/
│   │   ├── aimd_engine.py         # Pure Python AIMD mathematical calculations
│   │   └── index.py               # Controller Lambda (Boto3, DynamoDB, retry backoff)
│   └── telemetry/
│       └── worker_mock.py         # Worker Lambda with CloudWatch EMF telemetry generator
├── docs/
│   └── QueueMeshPRD.md            # Product Master & Technical Architecture Document
├── samconfig.toml                 # SAM deployment configuration
├── .gitignore                     # Git ignore rules (SAM, Python, credentials)
└── README.md                      # Project documentation
```

---

## 6. Quickstart & Deployment

### Prerequisites
* [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configured with active credentials
* [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (>= 1.100.0)
* Python 3.11

### Step 1: Build the Serverless Application
```bash
sam build -t infrastructure/template.yaml
```

### Step 2: Deploy to AWS
```bash
# First-time interactive guided deploy
sam deploy --guided

# Subsequent automated deployments
sam deploy
```

---

## 7. Verification & Live Cloud Testing

Once deployed, you can verify the closed-loop control system directly using the AWS CLI:

### 1. Test Multiplicative Decrease (HTTP 429 Spike)
Send a simulated 20% error rate breach:
```powershell
Set-Content -Path "test_event.json" -Value '{"error_rate": 0.20, "success_rate": 0.80}'
aws lambda invoke --function-name QueueMesh-Controller-dev --payload file://test_event.json --cli-binary-format raw-in-base64-out response.json
Get-Content response.json
```
**Expected Output:**
```json
{
  "statusCode": 200,
  "queue_id": "QueueMesh-TargetQueue-dev",
  "esm_uuid": "ce1fded8-dc54-4f5e-99be-56d5d8ff4547",
  "previous_concurrency": 100,
  "target_concurrency": 50,
  "action": "MULTIPLICATIVE_DECREASE",
  "state": "THROTTLED",
  "reason": "HTTP 429 breach: Error rate 20.0% >= T1 (5.0%). Applied factor Beta=0.5. Scaled from 100 -> 50.",
  "esm_updated": true,
  "execution_time_ms": 570.22
}
```

### 2. Test Consecutive Breach (Transition to `CRITICAL_BACKOFF`)
Invoke again with an ongoing breach to verify state machine progression:
```powershell
aws lambda invoke --function-name QueueMesh-Controller-dev --payload file://test_event.json --cli-binary-format raw-in-base64-out response.json
Get-Content response.json
```
**Expected Output:** Concurrency drops from `50 -> 25` with state transitioning to `CRITICAL_BACKOFF`.

### 3. Test Additive Increase (Downstream Recovery)
Send healthy downstream telemetry (99% success rate, 1% error rate):
```powershell
Set-Content -Path "test_recovery.json" -Value '{"error_rate": 0.01, "success_rate": 0.99}'
aws lambda invoke --function-name QueueMesh-Controller-dev --payload file://test_recovery.json --cli-binary-format raw-in-base64-out response.json
Get-Content response.json
```
**Expected Output:** Concurrency increases from `25 -> 30` ($+\alpha$) with state transitioning to `RECOVERING`.

### 4. Inspect DynamoDB State Ledger
Review recorded audit events in real time:
```powershell
aws dynamodb scan --table-name QueueMeshStateLedger-dev
```

---

## 8. Key Performance & Resilience Indicators

* **Turnaround Latency:** Total control-loop turnaround from telemetry ingestion to `UpdateEventSourceMapping` completion averages **270 ms – 570 ms** (well below the <1.5s SLA target).
* **Fault Tolerance:** Incorporates an exponential retry loop with jitter to gracefully handle AWS Lambda `ResourceInUseException` during concurrent Event Source Mapping state transitions.
* **Cost Efficiency:** Designed 100% within the AWS Free Tier (DynamoDB On-Demand + Lambda ARM64 compute).

---

## 9. License

This project is licensed under the MIT License.
