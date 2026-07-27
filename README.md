# Edge → Fog → Cloud Sensor Failover

A deployable reference project for a Fog & Edge Computing module.

## What it demonstrates

- 5 mock sensors: temperature, humidity, pressure, light, vibration
- Configurable sensor generation frequency and Fog dispatch interval
- Virtual Edge worker with deliberately constrained CPU/memory
- Virtual Fog node acting as the control plane / manager
- Edge → Fog → Cloud workload failover
- Hysteresis: failover at 90%, recovery at 50%
- Python + FastAPI cloud backend
- SQS queue + Lambda consumer for scalable cloud processing
- DynamoDB persistence
- Responsive dashboard
- Docker Compose local deployment
- Terraform AWS deployment for the cloud backend
- 1,000-task stress-test script and graph generation

## Architecture

```text
Mock Sensors
    |
    v
Virtual Edge Worker  <---- limited CPU/RAM
    ^
    | metrics / work
    |
Virtual Fog Manager  <----  control plane
    |
    +-- Edge healthy ----------> Edge
    |
    +-- Edge overloaded -------> Fog local worker
    |
    +-- Fog overloaded ---------> FastAPI Cloud Backend
                                      |
                                      v
                                     SQS
                                      |
                                      v
                                    Lambda
                                      |
                                      v
                                  DynamoDB
                                      |
                                      v
                                  Dashboard
```

The Fog is intentionally a manager/control plane. It monitors Edge and Fog capacity, makes routing decisions, performs aggregation, and dispatches processed batches to the FastAPI backend.

## Prerequisites

### Local
- Docker Desktop / Docker Engine + Docker Compose
- Python 3.11+ for stress-test tooling (optional)
- 2+ CPU cores recommended

### AWS
- manually to create ECR, ECS, ALB, SQS, Lambda, DynamoDB, IAM and CloudWatch resources

## 1. Run locally

```bash
docker compose up --build
```

Open:

- Dashboard: http://localhost:8000
- API docs: http://localhost:8000/docs
- Health: http://localhost:8000/health

The local backend uses SQLite so the project works without AWS credentials.

The sensor simulator and Fog manager start automatically.

## 2. Increase load to trigger failover

The simulator starts at a modest rate. You can increase load with:

```bash
docker compose run --rm \
  -e SENSOR_MULTIPLIER=10 \
  sensors
```

Or edit `config.yaml` and change `load.multiplier`.

Watch the Fog logs:

```bash
docker compose logs -f fog
```

You should see transitions such as:

```text
EDGE_NORMAL -> FOG_ACTIVE
FOG_ACTIVE -> CLOUD_ACTIVE
CLOUD_ACTIVE -> FOG_ACTIVE
FOG_ACTIVE -> EDGE_NORMAL
```

The exact thresholds are configurable.

## 3. Run the 1,000-task stress test

Install the test dependencies:

```bash
python -m pip install -r load_test/requirements.txt
```

Run:

```bash
python load_test/stress_test.py --url http://localhost:8000 --tasks 1000
```

Generate graphs:

```bash
python load_test/analyse.py
```

Outputs are written to `results/`:

- `cpu_routing.png`
- `task_distribution.png`
- `latency_slo.png`
- `stress_results.csv`

## 4. AWS deployment

The AWS deployment hosts the scalable cloud backend. Edge/Fog remain virtual local nodes, which is a useful architectural distinction: Edge/Fog are outside the central cloud and dispatch to the AWS backend.


The FastAPI service exposes:

```text
POST /api/v1/sensor-data
GET  /api/v1/sensors
GET  /api/v1/sensors/{sensor_type}
GET  /api/v1/system/status
```



## 5. Point the Fog at AWS

Set the Fog environment variable:

```bash
BACKEND_URL=http://YOUR-AWS-ALB-DNS
```

For example:

```bash
docker compose run --rm \
  -e BACKEND_URL=http://YOUR-AWS-ALB-DNS \
  fog
```

The Fog will then dispatch batches to the AWS FastAPI backend.

## Control panal logic

The Fog manager uses hysteresis:

```text
Edge CPU >= 90%  -> stop assigning new work to Edge
                   route to Fog

Fog CPU >= 90%   -> route new work to Cloud

Fog CPU <= 50%   -> resume Fog processing

Edge CPU <= 50%  -> resume Edge processing
```

Existing Edge tasks are allowed to finish; failover stops *new* work from being assigned to the overloaded tier.

## SLO

The reference stress test uses:

```text
Target: 99% of tasks complete within 5 seconds
```

The stress test records:

- task ID
- submission time
- start/end time
- destination
- latency
- status
- Edge/Fog/Cloud CPU samples

Do not use example numbers in a report. Use the generated results from your own run.

## Project report evidence

Recommended screenshots/figures:

1. Architecture diagram
2. Dashboard with sensor cards
3. Fog logs showing `EDGE_NORMAL -> FOG_ACTIVE`
4. Fog logs showing `FOG_ACTIVE -> CLOUD_ACTIVE`
5. CPU graph with 90% failover threshold
6. Task distribution by processing tier
7. Latency graph with the 5-second SLO
8. Before/after comparison with failover disabled/enabled
9. AWS ECS/SQS/Lambda/DynamoDB deployment
10. CloudWatch monitoring

## Important note

The cloud is not literally unlimited. The project models a higher-capacity cloud tier by giving it scalable managed services and by moving work into SQS/Lambda. This is more accurate than claiming infinite resources.
