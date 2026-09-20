# 🚦 QueueMesh

> **Self-healing serverless infrastructure protecting downstream APIs with sub-second backpressure—zero application code changes required.** 

## Serverless Adaptive Backpressure Controller for Amazon SQS and AWS Lambda**  
Dynamically regulates consumer concurrency in sub-second response times using real-time downstream HTTP error telemetry and the Additive Increase / Multiplicative Decrease (AIMD) algorithm.

Built for the **Ship It** track at the WeMakeDevs "First Commit" AWS Hackathon.

🎥 **[Watch the 3-Minute Demo Video](https://www.youtube.com/watch?v=5yX5kDagP4I)**  
📝 **[Read the Full AWS Builder Center Blog Post](https://builder.aws.com/content/3JaMEmIqOXThP0lLGisL1w5XH3X/queuemesh-automated-serverless-backpressure-for-aws-lambda-and-sqs)**

---

## 🛑 The "Infinite Scale" Problem
AWS Lambda and Amazon SQS are incredible because they scale infinitely. But what happens when the downstream services you rely on *don't*?

When Lambda scales out to hundreds of concurrent workers processing queue messages, downstream 3rd-party APIs (Stripe, Twilio, SendGrid, Shopify) or legacy relational databases get overwhelmed. They return **HTTP 429 (Too Many Requests)** or **HTTP 503** errors. 

Because of standard SQS behavior, failed messages go back into the queue for retry. Your Lambdas keep scaling, the API keeps rejecting them, and you enter a destructive **retry storm**:

```text
[High Queue Depth] ──► [Worker Concurrency Scales Up] ──► [Downstream API Throttles (HTTP 429)]
        ▲                                                                  │
        └────────────── [Visibility Timeout Retry Storm] ◄─────────────────┘
```

This leads to API key suspensions, DLQ spills, and 3 AM pager alerts. Fixing this traditionally requires manual intervention to dial down the queue concurrency in the AWS Console, taking 15 to 30 minutes.

---

## ✅ The QueueMesh Solution
**QueueMesh** is an automated, real-time backpressure controller that sits above your worker Lambdas like a smart circuit breaker. 

It monitors downstream HTTP response codes in real time. If it detects an HTTP 429 spike, it dynamically reaches into your AWS infrastructure and dials down the `MaximumConcurrency` of your SQS Event Source Mapping on the fly in **~300 milliseconds**.

**Crucially, QueueMesh requires zero changes to your application source code.**

---

## 🏗️ AWS Serverless Architecture

QueueMesh is deployed natively via **AWS SAM** and relies entirely on the AWS Free Tier.

1. **Worker Telemetry (CloudWatch EMF):** Worker Lambdas emit asynchronous HTTP status logs using CloudWatch Embedded Metric Format (EMF) with $0$ ms performance penalty.
2. **Signal Ingestion:** CloudWatch logs filter error rates and trigger the QueueMesh Controller Lambda.
3. **Control Loop (AWS Lambda & Boto3):** When triggered, the Controller Lambda calculates the math engine (AIMD) and uses the Boto3 SDK to execute `update_event_source_mapping`.
4. **Audit Ledger (Amazon DynamoDB):** A single-table design (`STATE#CURRENT` & `AUDIT#TIMESTAMP`) tracking real-time queue health and immutable transition histories.

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

## 🧠 The Math Engine (AIMD)
QueueMesh uses the **Additive Increase / Multiplicative Decrease (AIMD)** algorithm—the same math that prevents congestion on the global internet (TCP/IP), adapted specifically for AWS Lambda event source mappings.

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

## 📊 DynamoDB Single-Table Ledger Schema

QueueMesh tracks state and maintains an immutable audit log inside the `QueueMeshStateLedger` table:

| Entity Type | PK | SK | Attributes |
| :--- | :--- | :--- | :--- |
| **Active State** | `QUEUE#<QueueName>` | `STATE#CURRENT` | `CurrentState`, `CurrentMaxConcurrency`, `PreviousMaxConcurrency`, `ConsecutiveBreaches`, `LastAction`, `LastReason`, `TargetEsmUuid`, `LastEvaluatedTimestamp` |
| **Audit Log** | `QUEUE#<QueueName>` | `AUDIT#<Timestamp>` | `EventType`, `PreviousValue`, `NewValue`, `Action`, `Reason`, `ErrorRate`, `SuccessRate`, `ControlLoopLatencyMs`, `Timestamp` |

---

## 📁 Repository Structure

```text
QueueMesh/
├── infrastructure/
│   └── template.yaml               # AWS SAM template (DynamoDB, SQS, Worker ESM, Controller)
├── src/
│   ├── controller/
│   │   ├── aimd_engine.py          # Pure Python AIMD mathematical control-loop engine
│   │   └── index.py                # Controller Lambda (Boto3, DynamoDB ledger, retry backoff)
│   └── telemetry/
│       └── worker_mock.py          # Worker Lambda (CloudWatch EMF telemetry generator)
├── docs/
│   ├── AWS-inspect-guide.md        # Step-by-step AWS console inspection guide
│   └── QueueMesh Documentation.md  # Full Architecture & PRD reference
├── samconfig.toml                  # SAM deployment configuration
├── .gitignore                      # Git ignore rules (SAM, Python, credentials)
└── README.md                       # Project documentation
```

---

## 🚀 Quick Start (Deployment)

QueueMesh is packaged as an AWS SAM application.

### Prerequisites
* [AWS CLI v2](https://docs.aws.amazon.com/cli/latest/userguide/install-cliv2.html) configured with active credentials
* [AWS SAM CLI](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html) (>= 1.100.0)
* Python 3.11

### Step 1: Clone and Build
```bash
git clone https://github.com/Rajal-ui/QueueMesh.git
cd QueueMesh

# Build the serverless application
sam build -t infrastructure/template.yaml
```

### Step 2: Deploy to AWS
```bash
# First-time interactive guided deploy (us-east-1 recommended)
sam deploy --guided

# Subsequent automated deployments
sam deploy
```

---

## 🧪 Verification & Live Cloud Testing

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

## 🏆Key Performance & Resilience Indicators 

* **Verified Sub-Second Turnaround:** During the hackathon sprint, we verified that QueueMesh successfully executes a full state transition (Read DynamoDB $\rightarrow$ Calculate AIMD $\rightarrow$ Execute Boto3 Update $\rightarrow$ Write Audit Log) in **309.67 ms** (well below the <1.5s SLA target).
* **Zero Overhead Telemetry:** Utilizing **CloudWatch Embedded Metric Format (EMF)** allows worker Lambdas to emit telemetry simply by printing formatted JSON to `stdout` with $0$ ms blocking I/O penalty.
* **Fault Tolerance:** Incorporates an exponential retry loop with jitter to gracefully handle AWS Lambda `ResourceInUseException` during concurrent Event Source Mapping state transitions.
* **Cost Efficiency:** Designed 100% within the AWS Free Tier (DynamoDB On-Demand + Lambda ARM64 compute).

---

## 📄 License

This project is licensed under the MIT License.

---

*Built with ❤️ for the Bharat Builds Tour x AWS .*
