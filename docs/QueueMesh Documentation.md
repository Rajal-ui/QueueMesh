# QueueMesh: Product & Technical Architecture Document

## 1. Executive Summary

### Business Problem
Modern cloud applications built on AWS frequently use Amazon Simple Queue Service (SQS) coupled with AWS Lambda to process asynchronous events at massive scale. By default, the AWS SQS-to-Lambda Event Source Mapping scales out worker Lambda concurrency rapidly to drain queue depth. While this elastic scaling is ideal for internal workloads, it introduces a severe distributed systems failure mode when the worker Lambda calls third-party downstream APIs (e.g., Stripe, Twilio, SendGrid, Shopify) or legacy relational databases (e.g., Amazon RDS MySQL/PostgreSQL).

When Lambda worker concurrency scales out (e.g., to 500+ concurrent executions), the downstream endpoints become overwhelmed and respond with **HTTP 429 (Too Many Requests)** or **HTTP 503 (Service Unavailable)** error codes. Standard AWS SQS behavior returns these failed messages back to the queue for retry. This triggers a destructive **retry storm**:
1. SQS message visibility timeouts expire, re-exposing failed messages.
2. Lambda event source mappings continue scaling out or maintaining high concurrency.
3. The downstream API receives an even higher rate of requests, prolonging the outage.
4. Downstream providers invoke automatic IP bans or strict account-level rate-limiting penalties.
5. SQS Dead-Letter Queues (DLQs) fill with unhandled messages, causing permanent data loss or manual operator intervention.

Existing commercial APM tools (e.g., Datadog, Dynatrace) detect these rate-limit spikes but only emit passive alerts, leaving engineers to manually throttle queues via the AWS Console or CLI. This manual intervention takes 10 to 30 minutes—far too slow to prevent downstream outages and financial SLA breaches.

### Solution & Value Proposition
**QueueMesh** is an open-source, serverless, automated backpressure controller that forms a real-time, closed-loop feedback mechanism between downstream HTTP response codes and SQS-to-Lambda concurrency limits. Without modifying a single line of application source code inside worker Lambdas, QueueMesh monitors downstream rate-limit telemetry and dynamically throttles the `MaximumConcurrency` setting of the target SQS Event Source Mapping on the fly. 

### Success Definition
* **Sub-Second Control Loop:** Detection to concurrency modification completed in <1.5s.
* **Zero Dropped Messages:** 0% message loss due to rate limits or DLQ spills during downscaling.
* **100% Downstream SLA Protection:** Zero API key suspensions or extended 429 bans from external vendors.
* **Zero Application Code Changes:** Deploys entirely via AWS infrastructure configuration.
* **Cost Efficiency:** Operates 100% within the AWS Free Tier during normal execution.

---

## 2. Stakeholders and Roles

| Role | Primary Responsibility | Key Pain Point Addressed by QueueMesh |
| :--- | :--- | :--- |
| **Cloud Infrastructure / SRE Engineers** | System availability, incident response, MTTR reduction | Eliminates 3 AM alerts triggered by downstream rate limits and automated API bans. |
| **Backend Developers** | Feature delivery, service integrations | Removes the need to write complex custom rate-limiting/retry logic in application code. |
| **DevOps / Platform Teams** | CI/CD, Infrastructure as Code (IaC) governance | Provides reusable, standardized AWS SAM/CDK modules for safe SQS processing. |
| **FinOps Teams** | Cloud spend optimization, penalty mitigation | Prevents wasted Lambda invocations caused by infinite retry loops on HTTP 429. |
| **Enterprise API Consumers** | Vendor relationship management, SLA compliance | Maintains external vendor rate compliance automatically without manual monitoring. |

---

## 3. Product Vision and Objectives

### Long-Term Product Vision
To become the industry-standard, zero-overhead control-plane framework for asynchronous serverless event pipelines on AWS—enabling self-healing infrastructure that automatically matches execution velocity to downstream capacity across multi-region, multi-queue architectures.

### Core Objectives & Targets
* **MTTR Reduction:** Reduce Mean Time to Remediate downstream 429 spikes from ~15 mins (manual) to <2s (automated).
* **Cost Savings:** Reduce wasted Lambda execution compute cycles during rate-limit events by >= 90%.
* **Zero Code Intrusion:** Integration must require 0 lines of application source code modification.
* **Operational Simplicity:** Setup via AWS SAM or AWS CDK in <10 minutes.

---

## 4. Scope Definition

```text
┌────────────────────────────────────────────────────────────────────────┐
│                          IN SCOPE (MVP)                                │
├────────────────────────────────────────────────────────────────────────┤
│ • Amazon SQS Queue & AWS Lambda Event Source Mapping Monitoring        │
│ • Real-Time HTTP 429 / Downstream Error Telemetry Ingestion            │
│ • Control-Loop Lambda Executing Adaptive Backoff & Recovery            │
│ • Dynamic Runtime Updates to SQS ESM `MaximumConcurrency`              │
│ • Amazon DynamoDB Single-Table State and Metric Ledger                 │
│ • CLI Management Tool & Amazon CloudWatch Executive Dashboard          │
└────────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                         OUT OF SCOPE (POST-MVP)                        │
├────────────────────────────────────────────────────────────────────────┤
│ • Non-AWS Message Brokers (Azure Service Bus, GCP Pub/Sub, RabbitMQ)   │
│ • ML-Based Predictive Traffic Shaping (Amazon SageMaker Integration)   │
│ • Cross-Region / Multi-Region SQS Failover Orchestration              │
└────────────────────────────────────────────────────────────────────────┘
```

### 4.1 In Scope (Hackathon MVP)
1. **SQS Event Source Mapping Controller:** Intercepts and updates the `MaximumConcurrency` attribute (range: 2 to 1000) of `aws_lambda_event_source_mapping`.
2. **CloudWatch Metrics & Ingestion:** Custom metric logging via CloudWatch Embedded Metric Format (EMF) to track downstream HTTP 200, 429, and 503 response codes.
3. **Adaptive Backpressure Engine:** Closed-loop controller Lambda executing Multiplicative Decrease / Additive Increase (AIMD) algorithm.
4. **DynamoDB State Machine Ledger:** Single-table design tracking active concurrency limits, error velocities, and system health states.
5. **Operator Interfaces:** Sleek CLI tool (`queuemesh-cli`) and real-time CloudWatch Dashboard.

### 4.2 Out of Scope (Post-MVP)
1. Support for non-AWS message brokers (Kafka, RabbitMQ, Azure Service Bus).
2. Amazon SageMaker machine-learning models for predictive rate-limit forecasting.
3. Automated horizontal database scaling triggers (e.g., scaling RDS Aurora read replicas).

---

## 5. Users and Personas

### Persona 1: Priya — Lead SRE Engineer
* **Background:** 8 years of experience managing distributed cloud infrastructure. Responsible for 99.99% uptime across 40+ microservices on AWS.
* **Frustration:** Third-party APIs (Stripe, Twilio) frequently rate-limit their batch processing pipelines during peak hours. Lambda auto-scales rapidly, causing worker Lambdas to hammer the API with retries, filling DLQs and triggering high-priority pager alerts at night.
* **Goal:** Wants an automated, infrastructure-level guardrail that detects rate limits and throttles queue execution speed automatically without human intervention.

### Persona 2: Rahul — Senior Serverless Developer
* **Background:** Builds event-driven applications using AWS Lambda, Node.js, and Python.
* **Frustration:** Has to write complex custom exponential backoff and rate-limiting code inside every worker Lambda function. This bloats application code, adds maintenance overhead, and does not coordinate concurrency across multiple concurrent Lambda instances.
* **Goal:** Wants a plug-and-play AWS infrastructure pattern that handles backpressure transparently at the queue consumer level.

---

## 6. End-to-End Customer / Developer Journey

```text
┌──────────────────────────────────────────────────────────────────────────────────────────────┐
│                                   DEVELOPER JOURNEY STEPS                                    │
└──────────────────────────────────────────────────────────────────────────────────────────────┘

  1. DEPLOYMENT        2. NORMAL OPS       3. THROTTLE EVENT     4. AUTO-BACKPRESSURE  5. RECOVERY
┌───────────────┐   ┌───────────────┐   ┌─────────────────┐   ┌───────────────────┐   ┌───────────────┐
│ Developer     │   │ SQS processing│   │ External API    │   │ QueueMesh detects │   │ External API  │
│ deploys SAM   │──►│ 1,000 msg/sec │──►│ returns HTTP    │──►│ 429 spikes; dials │──►│ recovers;     │
│ template with │   │ at Max        │   │ 429; metrics    │   │ MaxConcurrency    │   │ QueueMesh     │
│ QueueMesh.    │   │ Concurrency   │   │ published to    │   │ down from 100 to  │   │ gradually     │
│               │   │ = 100.        │   │ CloudWatch EMF. │   │ 5 in <1.5s.       │   │ restores      │
└───────────────┘   └───────────────┘   └─────────────────┘   └───────────────────┘   │ concurrency.  │
                                                                                      └───────────────┘
```

---

## 7. Product Requirements Document (PRD)

### 7.1 Functional Requirements

| ID | Requirement Description | Priority | Target Component |
| :--- | :--- | :--- | :--- |
| **FR-01** | Capture downstream HTTP status codes (200, 429, 503) from worker Lambdas via CloudWatch EMF logs without blocking thread execution. | **P0** | Worker Telemetry Layer |
| **FR-02** | Trigger control-loop evaluation within <1s when HTTP 429 error rate exceeds configured threshold T1. | **P0** | EventBridge & Controller Lambda |
| **FR-03** | Dynamically update the target SQS Event Source Mapping `MaximumConcurrency` attribute via AWS Boto3 / SDK. | **P0** | Controller Lambda |
| **FR-04** | Maintain state history, execution state, and metric counts in Amazon DynamoDB. | **P0** | DynamoDB Single-Table |
| **FR-05** | Provide a CLI command (`queuemesh status`, `queuemesh override`) for manual operational control and overrides. | **P1** | CLI Tool |
| **FR-06** | Send alert notifications via Amazon SNS when a queue enters `CRITICAL_BACKOFF` state. | **P1** | Amazon SNS |

### 7.2 Non-Functional Requirements
* **NFR-01 (Latency):** Total control-loop turnaround time (detection to API call completion) must be <1.5 seconds.
* **NFR-02 (Availability):** Control plane availability must meet or exceed 99.99%, operating statelessly with DynamoDB persistence.
* **NFR-03 (Security):** Adhere strictly to AWS Least Privilege access principles. The Controller IAM role must only have permissions for specific `EventSourceMappingUUID` target resources.
* **NFR-04 (Cost Efficiency):** Execution overhead must cost < $1.00/month under standard workloads, utilizing AWS Free Tier resources fully.

---

## 8. Business Rules & Control-Loop Logic

QueueMesh uses an **Additive Increase / Multiplicative Decrease (AIMD)** algorithm, similar to TCP congestion control, adapted specifically for AWS Lambda event source mapping limits.

```text
                  Downstream HTTP Status Code
                              │
              ┌───────────────┴───────────────┐
              ▼                               ▼
       HTTP 429 Detected              HTTP 200 Success
       (Error Rate > T1)              (Success Rate > T2)
              │                               │
              ▼                               ▼
    Multiplicative Decrease            Additive Increase
  C_next = max(C_min, └C_current * β┘)    C_next = min(C_max, C_current + α)
```

### 8.1 Parameter Thresholds Matrix

| Variable | Definition | Default Value | Min Value | Max Value |
| :--- | :--- | :--- | :--- | :--- |
| T1 | Throttle Trigger Threshold (429 Rate) | 0.05 (5%) | 0.01 | 0.20 |
| T2 | Recovery Trigger Threshold (200 Rate) | 0.98 (98%) | 0.90 | 1.00 |
| β | Multiplicative Decrease Factor | 0.50 | 0.10 | 0.80 |
| α | Additive Increase Step Size | 5 workers | 1 | 50 |
| C_min | Minimum Concurrency Floor | 2 workers | 2 | 10 |
| C_max | Maximum Concurrency Ceiling | Configured Max | 10 | 1000 |

---

## 9. Event & State Machine

The QueueMesh Controller runs a finite state machine stored in DynamoDB:

```text
                            ┌──────────────┐
                            │   HEALTHY    │◄──────────────────────────────┐
                            └──────┬───────┘                               │
                                   │                                       │
                                   │ E_rate >= T1                          │ E_rate < T1
                                   ▼                                       │ & Sustained Success
                            ┌──────────────┐                               │
                            │  THROTTLED   │                               │
                            └──────┬───────┘                               │
                                   │                                       │
                                   │ E_rate >= T1                          │
                                   │ (Consecutive Breaches)                │
                                   ▼                                       │
                        ┌─────────────────────┐                   ┌────────┴─────────┐
                        │  CRITICAL_BACKOFF   │──────────────────►│    RECOVERING    │
                        └─────────────────────┘   E_rate < T1     └──────────────────┘
```

---

## 10. Solution Architecture

### 10.1 High-Level Architecture Diagram

```text
┌─────────────────┐
│ Event Publisher │
└────────┬────────┘
         │ 1. Ingest Messages
         ▼
┌─────────────────┐        2. Event Source Mapping        ┌───────────────────────────┐
│ Amazon SQS      ├──────────────────────────────────────►│ AWS Lambda Worker         │
│ (Target Queue)  │◄──┐   (Dynamic MaximumConcurrency)    │ (Executes App Logic)      │
└─────────────────┘   │                                   └─────────────┬─────────────┘
                      │                                                 │ 3. Call API
                      │                                                 ▼
                      │                                   ┌───────────────────────────┐
                      │                                   │ Downstream 3rd-Party API  │
                      │                                   │ (Stripe/Twilio/SendGrid)  │
                      │                                   └─────────────┬─────────────┘
                      │                                                 │ 4. HTTP 429 Response
                      │                                                 ▼
                      │                                   ┌───────────────────────────┐
                      │                                   │ CloudWatch EMF Logs       │
                      │                                   └─────────────┬─────────────┘
                      │                                                 │ 5. Trigger Signal
                      │                                                 ▼
                      │  7. Update                        ┌───────────────────────────┐
                      │     MaximumConcurrency            │ QueueMesh Controller      │
                      └───────────────────────────────────┤ (AIMD Engine Lambda)      │
                                                          └─────────────┬─────────────┘
                                                                        │ 6. Persist Audit
                                                                        ▼
                                                          ┌───────────────────────────┐
                                                          │ Amazon DynamoDB           │
                                                          │ (State Ledger Table)      │
                                                          └───────────────────────────┘
```

### 10.2 Repository Structure
```text
QueueMesh/
├── infrastructure/
│   └── template.yaml              # AWS SAM Infrastructure specification (DynamoDB, SQS, Worker, Controller)
├── src/
│   ├── controller/
│   │   ├── index.py               # Main Control-Loop Lambda Handler (Boto3, DynamoDB, retries)
│   │   └── aimd_engine.py         # Pure Python AIMD mathematical control engine
│   └── telemetry/
│       └── worker_mock.py         # SQS Worker Lambda with CloudWatch EMF telemetry generator
├── docs/
│   ├── QueueMeshPRD.md            # Full architectural specification
│   └── AWS-inspect-guide.md       # Step-by-step AWS console inspection guide
├── samconfig.toml                 # AWS SAM CLI deployment configuration
├── .gitignore                     # Git ignore rules (SAM, Python, credentials)
└── README.md                      # Comprehensive project documentation
```

---

## 11. Database Design

QueueMesh uses **Amazon DynamoDB** with a Single-Table Design pattern.

### 11.1 Single-Table Entity Layout

| Entity Type | PK | SK | Attribute Schema |
| :--- | :--- | :--- | :--- |
| **Queue Config** | `QUEUE#<QueueUUID>` | `METADATA` | `{ QueueArn, TargetEsmUuid, BaselineMaxConcurrency: 100, MinConcurrency: 2, Beta: 0.5, Alpha: 5 }` |
| **Active State** | `QUEUE#<QueueUUID>` | `STATE#CURRENT` | `{ CurrentState: "THROTTLED", CurrentMaxConcurrency: 10, ConsecutiveBreaches: 2 }` |
| **Metric Audit** | `QUEUE#<QueueUUID>` | `AUDIT#<Timestamp>`| `{ EventType: "CONCURRENCY_REDUCED", PreviousValue: 20, NewValue: 10, Reason: "HTTP_429" }` |

---

## 12. API & Control Interface Specification

### 12.1 Public & Telemetry Ingestion (CloudWatch EMF Schema)
Worker Lambdas emit log lines to stdout formatted according to the CloudWatch Embedded Metric Format (EMF):

```json
{
  "_aws": {
    "Timestamp": 1773964800000,
    "CloudWatchMetrics": [
      {
        "Namespace": "QueueMesh/Telemetry",
        "Dimensions": [["QueueName", "TargetVendor"]],
        "Metrics": [
          {"Name": "Downstream200Count", "Unit": "Count"},
          {"Name": "Downstream429Count", "Unit": "Count"}
        ]
      }
    ]
  },
  "QueueName": "StripePaymentProcessingQueue",
  "TargetVendor": "Stripe",
  "Downstream200Count": 0,
  "Downstream429Count": 1
}
```

### 12.2 Admin Control REST API (AWS API Gateway)

#### PUT `/api/v1/queues/{queueId}/override`
Allows an operator to manually override concurrency and freeze automation.

*Request Body:*
```json
{
  "overrideConcurrency": 15,
  "freezeDurationMinutes": 60,
  "reason": "Investigating Stripe incident manually"
}
```

---

## 13. External Service Integration & Downstream Throttling Design

When integrating with third-party APIs (Stripe, Twilio, Shopify), QueueMesh parses HTTP response headers extracted by worker Lambdas:

```text
┌────────────────────────────────────────────────────────────────────────┐
│                        WORKER LAMBDA (TELEMETRY)                       │
│  Extracts:                                                             │
│    • Retry-After: 30                                                   │
│    • X-RateLimit-Reset: 1773964830                                     │
└──────────────────────────────────┬─────────────────────────────────────┘
                                   │ (EMF Log)
                                   ▼ 
┌────────────────────────────────────────────────────────────────────────┐
│                        QUEUEMESH CONTROLLER                            │
│  Calculates Freeze Window:                                             │
│    Freeze Window = max(30s, X-RateLimit-Reset - CurrentTime)           │
│  Action:                                                               │
│    Sets MaximumConcurrency = C_min during Freeze Window                │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 14. Observability & Dashboard Design

QueueMesh provisions a custom Amazon CloudWatch Executive Dashboard showing real-time system performance.

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                      QUEUEMESH EXECUTIVE DASHBOARD                      │
├────────────────────────────────────────┬────────────────────────────────┤
│ WIDGET 1: Downstream HTTP Codes        │ WIDGET 2: Active Concurrency   │
│ [Line Chart]                           │ [Gauge Chart]                  │
│ ──── 200 Successes (Green)             │ Target ESM Concurrency: 10     │
│ ──── 429 Throttles  (Red)              │ Baseline Ceiling:       100    │
├────────────────────────────────────────┼────────────────────────────────┤
│ WIDGET 3: SQS Queue Depth & Age        │ WIDGET 4: System State Ledger  │
│ [Bar Chart]                            │ [Status Badge]                 │
│ Visible Messages: 4,210                │ Current State:  [ THROTTLED ]  │
│ Age of Oldest Msg: 12 sec              │ Last Reaction:  -1.2 seconds   │
└────────────────────────────────────────┴────────────────────────────────┘
```

---

## 15. Security Requirements

### 15.1 IAM Least Privilege Policy
The QueueMesh Controller Lambda operates under a strict, dedicated IAM Role:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "AllowSQSEventSourceMappingUpdate",
      "Effect": "Allow",
      "Action": [
        "lambda:GetEventSourceMapping",
        "lambda:UpdateEventSourceMapping"
      ],
      "Resource": "arn:aws:lambda:us-east-1:123456789012:event-source-mapping:ce1fded8-dc54-4f5e-99be-56d5d8ff4547"
    }
  ]
}
```

---

## 16. Operational & Non-Functional Considerations

### 16.1 Control Loop Fault Tolerance & State Resilience
If the QueueMesh Controller Lambda encounters downstream or AWS API contention:
1. **Fallback Safety:** AWS SQS Event Source Mapping retains its last applied `MaximumConcurrency` value.
2. **Circuit Breaker Timeout:** If no control updates occur for > 5 minutes, a fallback EventBridge rule resets `MaximumConcurrency` to a safe baseline default.
3. **AWS ESM Lifecycle Resilience (`ResourceInUseException`):** When updating SQS Event Source Mappings, AWS Lambda places the ESM into an `Updating` state for 1 to 5 seconds. If subsequent control events trigger during this transition, AWS raises a `ResourceInUseException`. QueueMesh implements an exponential retry backoff loop with jitter (`max_retries=5`, `base_delay=1.0s`) and graceful fallback, ensuring zero dropped control events and seamless convergence without unhandled exceptions.

---

## 17. Deployment & Infrastructure Plan

Deployment is fully automated using the AWS Serverless Application Model (AWS SAM).

```yaml
# AWS SAM Template Excerpt (infrastructure/template.yaml)
AWSTemplateFormatVersion: '2010-09-09'
Transform: AWS::Serverless-2016-10-31

Globals:
  Function:
    Runtime: python3.11
    Architectures:
      - arm64

Resources:
  QueueMeshStateLedger:
    Type: AWS::DynamoDB::Table
    Properties:
      TableName: !Sub "QueueMeshStateLedger-${Environment}"
      BillingMode: PAY_PER_REQUEST
      AttributeDefinitions:
        - AttributeName: PK
          AttributeType: S
        - AttributeName: SK
          AttributeType: S
      KeySchema:
        - AttributeName: PK
          KeyType: HASH
        - AttributeName: SK
          KeyType: RANGE

  WorkerSQSEventSourceMapping:
    Type: AWS::Lambda::EventSourceMapping
    Properties:
      EventSourceArn: !GetAtt TargetSQSQueue.Arn
      FunctionName: !GetAtt WorkerLambda.Arn
      BatchSize: 10
      ScalingConfig:
        MaximumConcurrency: !Ref BaselineMaxConcurrency
      FunctionResponseTypes:
        - ReportBatchItemFailures

  QueueMeshControllerFunction:
    Type: AWS::Serverless::Function
    Properties:
      CodeUri: ../src/controller/
      Handler: index.lambda_handler
      Timeout: 10
      Policies:
        - DynamoDBCrudPolicy:
            TableName: !Ref QueueMeshStateLedger
        - Version: "2012-10-17"
          Statement:
            - Sid: AllowESMReadUpdate
              Effect: Allow
              Action:
                - lambda:GetEventSourceMapping
                - lambda:UpdateEventSourceMapping
              Resource: !Sub "arn:aws:lambda:${AWS::Region}:${AWS::AccountId}:event-source-mapping:*"
```

---

## 18. Software Development Life Cycle (SDLC)

```text
┌────────────────────────────────────────────────────────────────────────┐
│                       GITHUB ACTIONS CI/CD PIPELINE                    │
└────────────────────────────────────────────────────────────────────────┘

  1. COMMIT           2. LINT & TEST      3. SAM BUILD        4. DEPLOY
┌───────────────┐   ┌───────────────┐   ┌───────────────┐   ┌───────────────┐
│ Git push to   │──►│ Flake8 linting│──►│ sam build     │──►│ sam deploy    │
│ main branch   │   │ & PyTest unit │   │ validates     │   │ executes AWS  │
│               │   │ tests execute.│   │ CloudFormation│   │ ChangeSet.    │
└───────────────┘   └───────────────┘   └───────────────┘   └───────────────┘
```

---


## 19. Change Control Process
To adjust control-loop parameters safely in production:
1. Modify parameter values in SSM Parameter Store (`/queuemesh/config/alpha`).
2. The Controller Lambda reads SSM parameters with a 60-second in-memory cache TTL.

---

## 20. Future Roadmap (Post-MVP)
* **Version 1.1:** Amazon Bedrock AI Anomaly Detection for custom vendor error strings.
* **Version 2.0:** Cross-Region Dynamic Routing for vendor outages.

---

## Appendices

### Appendix A: Sample Control Event Payload (JSON)
EventBridge payload triggering the QueueMesh Controller Lambda:


```json
{
  "version": "0",
  "id": "c2b3a4d5-e6f7-8a9b-0c1d-2e3f4a5b6c7d",
  "detail-type": "QueueMesh Metric Alarm State Change",
  "source": "aws.cloudwatch",
  "account": "123456789012",
  "time": "2026-09-19T23:20:00Z",
  "region": "us-east-1",
  "resources": [
    "arn:aws:cloudwatch:us-east-1:123456789012:alarm:QueueMesh-429-Breach-StripeQueue"
  ],
  "detail": {
    "alarmName": "QueueMesh-429-Breach-StripeQueue",
    "state": {
      "value": "ALARM",
      "reason": "Threshold Crossed: 1 datapoint (0.14) was greater than the threshold (0.05).",
      "timestamp": "2026-09-19T23:19:58.000+0000"
    },
    "configuration": {
      "metrics": [
        {
          "id": "m1",
          "metricStat": {
            "metric": {
              "namespace": "QueueMesh/Telemetry",
              "metricName": "Downstream429Count"
            },
            "period": 10,
            "stat": "Sum"
          }
        }
      ]
    }
  }
}

```

### Appendix B: Sample CLI Terminal Output (`queuemesh status`)

```text
================================================================================
                       QUEUEMESH DASHBOARD v1.0.0                              
================================================================================
Target Queue:      arn:aws:sqs:us-east-1:123456789012:StripePaymentProcessing
Event Source UUID: a1b2c3d4-e5f6-7a8b-9c0d-1e2f3a4b5c6d                        
System Health:     [ THROTTLED ] (Active Backpressure)                        
--------------------------------------------------------------------------------
CONCURRENCY METRICS
  Baseline Max Concurrency:  100 workers
  Current Target Limit:      10 workers  [▼ 90% Throttled]
  Minimum Floor Limit:       2 workers

TELEMETRY (Last 10 Seconds)
  HTTP 200 Successes:        860 [86.0%]
  HTTP 429 Throttles:        140 [14.0%]  ▲ (Threshold Breach > 5.0%)
  HTTP 503 Errors:           0   [ 0.0%]

CONTROL LOOP ACTION LOG
  EVENT: HTTP 429 Velocity Spike (14.0%).
  ACTION: Multiplicative Decrease Applied (Factor β = 0.5).
  AWS SDK: Updated EventSourceMapping MaximumConcurrency -> 10.
  RESULT: Concurrency reduced successfully in 1.18 seconds.
================================================================================
```

### Appendix C: Verified Live AWS Cloud Execution (Real Benchmark)

Real controller invocation results executed directly in `us-east-1` demonstrating sub-second turnaround:

#### Multiplicative Decrease (HTTP 429 Spike)
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

#### Consecutive Breach (Transition to `CRITICAL_BACKOFF`)
```json
{
  "statusCode": 200,
  "queue_id": "QueueMesh-TargetQueue-dev",
  "esm_uuid": "ce1fded8-dc54-4f5e-99be-56d5d8ff4547",
  "previous_concurrency": 50,
  "target_concurrency": 25,
  "action": "MULTIPLICATIVE_DECREASE",
  "state": "CRITICAL_BACKOFF",
  "reason": "HTTP 429 breach: Error rate 20.0% >= T1 (5.0%). Applied factor Beta=0.5. Scaled from 50 -> 25.",
  "esm_updated": true,
  "execution_time_ms": 355.81
}
```

#### Additive Increase (Downstream Recovery)
```json
{
  "statusCode": 200,
  "queue_id": "QueueMesh-TargetQueue-dev",
  "esm_uuid": "ce1fded8-dc54-4f5e-99be-56d5d8ff4547",
  "previous_concurrency": 15,
  "target_concurrency": 20,
  "action": "ADDITIVE_INCREASE",
  "state": "RECOVERING",
  "reason": "Downstream healthy: Success rate 99.0% >= T2 (98.0%) and Error rate 1.0% < T1 (5.0%). Added Alpha=+5. Scaled from 15 -> 20.",
  "esm_updated": true,
  "execution_time_ms": 619.74
}
```