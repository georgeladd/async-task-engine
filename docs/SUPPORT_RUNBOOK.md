# Technical Support Runbook & Operations Playbook

🌐 **[English](SUPPORT_RUNBOOK.md)** • **[Русский](SUPPORT_RUNBOOK_RU.md)**

Welcome to the operational runbook for **Async Task Engine**. This document is designed for L2/L3 Technical Support and Operations Engineers to diagnose incidents rapidly, understand system behavior, and resolve production failures with minimal MTTR (Mean Time to Resolution)

---

## 1. Fast Onboarding (System Map in 5 Minutes)

### 1.1. Core Components & Access Points
| Component | Default Port | Internal Role | Web UI / Dashboard |
|---|---|---|---|
| **Operations Web Console** | `8000` | Primary L2 triage: live charts, 1-click lock release, DLQ replay | [http://localhost:8000/dashboard](http://localhost:8000/dashboard) ([User Guide](WEB_CONSOLE_GUIDE.md)) |
| **API Producer** | `8000` | Ingests tasks, assigns UUIDs, updates Redis status | [http://localhost:8000/docs](http://localhost:8000/docs) |
| **Prometheus Telemetry** | `8000` | Real-time scrape endpoint for Grafana | [http://localhost:8000/metrics](http://localhost:8000/metrics) |
| **RabbitMQ** | `5672` / `15672` | Topic exchange (`tasks.topic`), primary queue (`tasks_primary`), Alternate Exchange (`tasks.ae` -> `tasks_unrouted`), and DLQ (`tasks_dead_letter`) | [http://localhost:15672](http://localhost:15672) (`guest` / `guest`) |
| **Redis** | `6379` | Task statuses (`task:status:*`) and resource locks (`lock:resource:*`) | Access via `redis-cli` or CLI |
| **Worker** | Background | Consumes tasks, acquires lock, streams data chunks | Monitored via container logs |
| **Operations CLI** | Terminal | Incident triage, task submission, and lock management | `async-engine --help` (or `python -m src.cli --help`) |

### 1.2. Key System Invariants
- **Task Lifecycle:** `PENDING` -> `RUNNING` -> `COMPLETED` (or `FAILED` / `DEAD_LETTERED`)
- **Resource Locking:** Only **one** worker can process tasks for a given `resource_id` at the same time. Other tasks with the same `resource_id` will be rejected back to the queue until the active lock expires or releases
- **Dead-Letter Queue:** If a task fails more than `MAX_TASK_RETRIES` (default: 3) or encounters an unrecoverable error (e.g. unknown task type), it is rejected into `tasks_dead_letter`. Preserved task context in `task:data:{task_id}` enables seamless one-click replay without loss of items

---

## 2. Emergency 30-Second Triage Checklist

When a ticket arrives stating *"Tasks are not completing"* or *"Data is not updated"*, perform triage in order:

```bash
# 0. Open Operations Web Console in browser for instant visual triage:
# http://localhost:8000/dashboard (Inspect KPIs, Active Locks & DLQ items)

# 1. Check API service health and connectivity via CLI
async-engine health

# 2. Check for stuck resource locks in Redis
async-engine locks

# 3. Check container runtime status
docker-compose ps

# 4. Check if RabbitMQ has active consumers and queue backlog
docker exec async-engine-rabbitmq rabbitmqctl list_queues name messages messages_unacknowledged consumers

# 5. Check worker logs for unhandled errors
docker logs --tail 100 async-engine-worker
```

---

## 3. Incident Diagnostic Matrix & Playbooks

---

### Incident A: Tasks Accumulating in `PENDING` (Queue Backlog Growing)

#### Symptoms
- Clients report tasks accepted with HTTP 202, but `GET /api/v1/tasks/{task_id}` stays `pending` for minutes
- RabbitMQ queue `tasks_primary` shows high `messages` count and `consumers = 0`

#### Root Cause
1. Worker service has crashed or exited
2. Influx of tasks exceeds single-worker processing capacity

#### Diagnostic Commands
```bash
# Check worker status
docker-compose ps worker

# Inspect why worker died
docker logs --tail 50 async-engine-worker
```

#### Remediation & Fix
1. If the worker container is stopped, restart it immediately:
   ```bash
   docker-compose restart worker
   ```
2. If traffic surged, scale the worker pool horizontally:
   ```bash
   docker-compose up -d --scale worker=4 --no-recreate
   ```
3. Verify that new workers connected and `consumers` count increased:
   ```bash
   docker exec async-engine-rabbitmq rabbitmqctl list_queues name consumers
   ```

---

### Incident B: Recurring "Resource is locked" & Tasks Repeatedly Requeued

#### Symptoms
- Worker logs contain continuous warnings:
  `WARNING: Resource 'customer_tenant_42' is locked by another task. Requeuing task ...`
- Tasks for a specific customer or resource are stalled, while other tasks process normally

#### Root Cause
A previous task on the same `resource_id` terminated abruptly without executing its `finally: await lock.release()` block, leaving an orphaned lock key in Redis

#### Diagnostic Commands
```bash
# Check the remaining TTL on the locked resource
docker exec async-engine-redis redis-cli ttl "lock:resource:<resource_id>"

# Inspect the lock owner token
docker exec async-engine-redis redis-cli get "lock:resource:<resource_id>"
```

#### Remediation & Fix
1. Check if an active task is genuinely still executing in the worker logs:
   ```bash
   docker logs async-engine-worker | grep "<resource_id>"
   ```
2. If no active processing is detected and the lock is stale, manually evict the lock using either:
   - **Web Console (Recommended):** Open [http://localhost:8000/dashboard](http://localhost:8000/dashboard), find the resource row, and click **"Force Unlock"**
   - **Operations CLI:** `async-engine unlock "<resource_id>"` (or `python -m src.cli unlock "<resource_id>"`)
   - **Raw Redis CLI:** `docker exec async-engine-redis redis-cli del "lock:resource:<resource_id>"`
3. The worker will automatically acquire the lock and resume task execution on the next queue pass

---

### Incident C: Messages Diverted to Dead-Letter Queue (`tasks_dead_letter`)

#### Symptoms
- RabbitMQ queue `tasks_dead_letter` has a non-zero message count
- Task status in API returns `"status": "dead_lettered"`
- Operations Console displays red badge on Dead-Letter Queue card

#### Root Cause
The task encountered fatal business errors (e.g. external database rejected batch, corrupt record structure) and exceeded `MAX_TASK_RETRIES` (3 attempts)

#### Remediation & Fast Recovery
- **One-Click Replay via Web Console:** Open [http://localhost:8000/dashboard](http://localhost:8000/dashboard), inspect failure reason in Dead-Letter Queue table, and click **"Replay"** to re-enqueue message back into primary queue
- **Escalation to Engineering:** If the issue requires code fix, click **"Escalate"** to automatically compile an Incident Dossier with stack trace and parameters for developers

#### Diagnostic Commands
```bash
# Peek at the dead-lettered message body without consuming it
docker exec async-engine-rabbitmq rabbitmqctl list_queues name messages | grep dead_letter

# Search worker logs for failure traceback
docker logs async-engine-worker | grep "Routing to Dead-Letter Queue" -B 5 -A 2
```

#### Remediation & Fix
1. Identify the root cause from the error log (e.g., target table missing column, external API down)
2. Once external dependency is repaired:
   - Extract the payload from DLQ (or replay via `POST /api/v1/tasks`)
   - Purge dead-letter queue if messages were corrupt test data:
     ```bash
     docker exec async-engine-rabbitmq rabbitmqctl purge_queue tasks_dead_letter
     ```

---

### Incident D: Worker Killed with Exit Code 137 (OOMKilled)

#### Symptoms
- Docker daemon reports `async-engine-worker exited with code 137`
- Worker crashes midway through huge batch payloads

#### Root Cause
Memory overload caused by an excessively large `BATCH_CHUNK_SIZE` setting combined with huge individual record sizes

#### Diagnostic Commands
```bash
# Check container memory consumption
docker stats --no-stream async-engine-worker
```

#### Remediation & Fix
1. Lower the chunk size in `.env`:
   ```bash
   # In .env:
   BATCH_CHUNK_SIZE=25
   ```
2. Re-apply configuration to containers:
   ```bash
   docker-compose up -d worker
   ```

### Incident E: External API 429 Rate Limits or Database Connection Exhaustion

#### Symptoms
- Worker logs show `HTTP 429 Too Many Requests` from third-party APIs
- Database alerts report connection pool starvation during large batch jobs

#### Root Cause
Worker processing throughput is overwhelming downstream services faster than their rate limits permit

#### Remediation & Fix
1. Throttle worker cadence by setting `RATE_LIMIT_PER_SECOND` in `.env`:
   ```bash
   # In .env:
   RATE_LIMIT_PER_SECOND=20.0
   ```
2. Hot-reload worker container:
   ```bash
   docker-compose up -d worker
   ```

---

## 4. Useful Operational Cheatsheet

### Task Status Query
```bash
# Inspect task status and metrics
curl -s "http://localhost:8000/api/v1/tasks/<TASK_UUID>" | jq .
```

### Redis Quick Commands
```bash
# Inspect all registered task statuses
docker exec async-engine-redis redis-cli keys "task:status:*"

# Check raw task result JSON
docker exec async-engine-redis redis-cli get "task:result:<TASK_UUID>"

# Inspect active idempotency tokens and mapped task UUIDs
docker exec async-engine-redis redis-cli keys "idempotency:*"
docker exec async-engine-redis redis-cli get "idempotency:<KEY>"
```

### RabbitMQ Reset & Diagnostic
```bash
# View unacknowledged messages currently in flight across workers
docker exec async-engine-rabbitmq rabbitmqctl list_queues name messages_unacknowledged
```

### Structured JSON Log Filtering (ELK / jq)
```bash
# Filter logs for a specific correlation_id across streaming output
docker logs -f async-engine-worker | jq -R 'fromjson? | select(.correlation_id == "<TASK_UUID>")'

# Show recent error events with stack traces
docker logs --tail 200 async-engine-worker | jq -R 'fromjson? | select(.level == "ERROR" or .level == "CRITICAL")'

# Inspect webhook delivery attempts and responses
docker logs async-engine-worker | jq -R 'fromjson? | select(.message | contains("Webhook"))'
```

### Prometheus Alert Rules (Grafana / Alertmanager)
| Alert Name | PromQL Expression | Severity | Immediate Action |
|---|---|---|---|
| **TasksInDeadLetterQueue** | `rate(dead_letter_tasks_total[5m]) > 0` | Warning | Check DLQ via `rabbitmqctl` and inspect failure reasons |
| **WorkerPoolStalled** | `active_worker_tasks == 0 and rabbitmq_queue_messages > 10` | Critical | Worker process died; restart via `docker-compose restart worker` |
| **HighTaskDurationP95** | `histogram_quantile(0.95, sum(rate(task_duration_seconds_bucket[5m])) by (le)) > 30` | Warning | Inspect database query times or decrease `BATCH_CHUNK_SIZE` |

---

## 5. Escalation Guidelines (When to Page L3 / Backend Developers)

Escalate to the core development team if:
- RabbitMQ crashes repeatedly with disk alarm or memory alarm (`rabbitmqctl status`)
- Multiple dead-lettered tasks originate from valid schema requests, indicating an unhandled upstream API contract change
- Distributed lock contention is caused by an infinite loop inside task business logic
- Stalled tasks remain in `RUNNING` status exceeding worker heartbeat timeouts

### How Support Generates an Incident Dossier

Support engineers can compile a complete diagnostic package for engineering using three workflows:

1. **Global Header Button (Web Console):**
   - Open `http://localhost:8000/dashboard`
   - Click the red **`🚨 Create Incident`** button in the top navigation bar
   - Enter the customer's `Task UUID` and operator observations
   - Click **Generate Incident Dossier**, then **📋 Copy Dossier to Clipboard**
   - Paste directly into the Jira issue or Slack engineering incident channel
2. **From Dead-Letter Queue Table:**
   - In the bottom DLQ table, click **`🚨 Escalate`** next to any failed task
   - The modal automatically pre-fills the target UUID and queries the root cause from Redis
3. **Via Operations API:**
   ```bash
   curl -X POST http://localhost:8000/api/v1/ops/escalate \
     -H "Content-Type: application/json" \
     -H "X-Ops-Token: ops-dev-secret" \
     -d '{
       "task_id": "550e8400-e29b-41d4-a716-446655440000",
       "operator_comment": "Customer report: batch export frozen at 85%"
     }'
   ```
