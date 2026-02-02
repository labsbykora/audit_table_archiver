# One-Shot vs Persistent Services Explained

## Quick Answer

**One-Shot**: Container runs a task, completes, and exits (like a script)  
**Persistent**: Container stays running continuously (like a web server)

The archiver is configured as **one-shot** because it's a batch job that runs, completes archival, and exits.

---

## Detailed Explanation

### What is "One-Shot"?

**One-shot** means the container runs a task, completes it, and then **exits**. It's like running a shell script:

```bash
# One-shot: Runs, completes, exits
python -m archiver.main --config config.yaml
# Process exits when done
```

**In Docker Compose**:
```yaml
restart: "no"  # Don't restart when it exits
```

**Behavior**:
- Container starts
- Runs archival task
- Completes (success or failure)
- Container exits
- Container stays stopped (doesn't restart)

### What is "Persistent"?

**Persistent** means the container stays running continuously, waiting for work:

```bash
# Persistent: Stays running, waits for requests
python -m flask run
# Process stays alive, handling requests
```

**In Docker Compose**:
```yaml
restart: "always"  # Restart if it exits
# or
command: ["tail", "-f", "/dev/null"]  # Keep container alive
```

**Behavior**:
- Container starts
- Stays running
- Handles requests/tasks continuously
- Only stops if explicitly stopped

---

## Why Archiver is One-Shot

The archiver is a **batch job**, not a service:

1. **It runs a task**: Archive tables → S3
2. **It completes**: All eligible records archived
3. **It exits**: Task done, no need to stay running

```python
# In archiver.main:
stats = asyncio.run(archiver.archive())  # Runs, completes, exits
print_summary(stats)  # Shows results
sys.exit(0)  # Process exits
```

### Why Not Persistent?

You might think: "Why not keep it running and schedule tasks internally?"

**Problems with persistent approach**:
- ❌ Wastes resources (idle container)
- ❌ Complex task scheduling inside container
- ❌ Harder to monitor (is it running? idle? stuck?)
- ❌ Harder to scale (how many containers?)

**Benefits of one-shot**:
- ✅ Efficient (only runs when needed)
- ✅ Simple (runs task, exits)
- ✅ Easy to monitor (container status = task status)
- ✅ Easy to schedule (use external scheduler)

---

## How to Run One-Shot Tasks

### Option 1: Manual Run (Development)

```bash
# Run once, exits when done
docker-compose run --rm archiver \
  python -m archiver.main --config /app/config.yaml

# --rm: Remove container after it exits
```

**What happens**:
1. Container starts
2. Runs archiver
3. Archival completes
4. Container exits
5. Container is removed (--rm)

### Option 2: Scheduled with CronJob (Kubernetes)

For **scheduled runs**, use Kubernetes CronJob:

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: archiver-daily
spec:
  schedule: "0 2 * * *"  # 2 AM daily
  jobTemplate:
    spec:
      template:
        spec:
          containers:
          - name: archiver
            image: audit-archiver:1.0.0
            command:
            - python
            - -m
            - archiver.main
            - --config
            - /app/config.yaml
          restartPolicy: OnFailure
```

**What happens**:
1. Kubernetes creates a Job at 2 AM
2. Job starts a pod
3. Pod runs archiver (one-shot)
4. Archiver completes
5. Pod exits
6. Job marks as complete
7. Next day, repeat

### Option 3: External Scheduler (cron, systemd)

**Using systemd timer** (on host):
```ini
# /etc/systemd/system/archiver.service
[Unit]
Description=Audit Table Archiver

[Service]
Type=oneshot
ExecStart=/usr/bin/docker run --rm \
  -v /path/to/config.yaml:/app/config.yaml:ro \
  audit-archiver:1.0.0 \
  python -m archiver.main --config /app/config.yaml
```

```ini
# /etc/systemd/system/archiver.timer
[Unit]
Description=Run archiver daily at 2 AM

[Timer]
OnCalendar=daily
OnCalendar=*-*-* 02:00:00

[Install]
WantedBy=timers.target
```

**Using cron** (on host):
```bash
# Run daily at 2 AM
0 2 * * * docker run --rm -v /path/to/config.yaml:/app/config.yaml:ro audit-archiver:1.0.0 python -m archiver.main --config /app/config.yaml
```

**Using Docker Compose with cron**:
```yaml
services:
  archiver-scheduler:
    image: alpine:latest
    volumes:
      - ../config.yaml:/app/config.yaml:ro
      - /var/run/docker.sock:/var/run/docker.sock
    command: |
      sh -c "
        apk add --no-cache dcron &&
        echo '0 2 * * * docker run --rm -v /app/config.yaml:/app/config.yaml:ro audit-archiver:1.0.0 python -m archiver.main --config /app/config.yaml' | crontab - &&
        crond -f
      "
    restart: always
```

---

## Comparison: One-Shot vs Persistent

### One-Shot (Current Setup)

```yaml
services:
  archiver:
    build: .
    restart: "no"  # Don't restart when it exits
    # No command = use default from Dockerfile
    # Default: Runs archiver, exits
```

**Usage**:
```bash
# Run on demand
docker-compose run --rm archiver

# Or with command
docker-compose run --rm archiver \
  python -m archiver.main --config /app/config.yaml
```

**Pros**:
- ✅ Efficient (only runs when needed)
- ✅ Simple (task completes, container exits)
- ✅ Resource-friendly (no idle containers)
- ✅ Easy to schedule (external scheduler)

**Cons**:
- ❌ Need external scheduler for automation
- ❌ Container must be started manually (or via scheduler)

### Persistent (If We Wanted It)

```yaml
services:
  archiver:
    build: .
    restart: "always"
    command: ["tail", "-f", "/dev/null"]  # Keep container alive
    # Or run a scheduler inside container
```

**Usage**:
```bash
# Container stays running
docker-compose up -d archiver

# Execute task inside running container
docker-compose exec archiver python -m archiver.main --config /app/config.yaml
```

**Pros**:
- ✅ Container always available
- ✅ Can run tasks on-demand inside container

**Cons**:
- ❌ Wastes resources (idle container)
- ❌ Complex (need to schedule inside container)
- ❌ Harder to monitor (is it idle or stuck?)

---

## Recommended Approach

### For Development

**Manual one-shot runs**:
```bash
docker-compose run --rm archiver \
  python -m archiver.main --config /app/config.yaml --dry-run
```

### For Production

**Kubernetes CronJob** (best for Kubernetes):
```yaml
# Runs daily at 2 AM
schedule: "0 2 * * *"
```

**Or external scheduler** (cron, systemd, etc.)

**Or Docker Compose with external trigger**:
- Host cron triggers: `docker-compose run --rm archiver`
- CI/CD pipeline triggers: `docker-compose run --rm archiver`
- Manual trigger: `docker-compose run --rm archiver`

---

## Summary

| Aspect | One-Shot | Persistent |
|--------|----------|------------|
| **Runtime** | Runs task, exits | Stays running |
| **Resource Usage** | Only when running | Always (even idle) |
| **Scheduling** | External (cron, K8s) | Internal or external |
| **Monitoring** | Container status = task status | Check if idle/busy |
| **Use Case** | Batch jobs, scripts | Web servers, daemons |
| **Archiver** | ✅ Perfect fit | ❌ Overkill |

**The archiver is one-shot because**:
- It's a batch job (runs, completes, exits)
- Scheduled runs use external scheduler (Kubernetes CronJob, cron, etc.)
- More efficient and simpler

---

## Quick Examples

### Run Once (One-Shot)
```bash
# Starts, runs, exits, removed
docker-compose run --rm archiver
```

### Keep Running (If Needed)
```bash
# Starts and stays running (for debugging)
docker-compose up -d archiver

# Run task inside running container
docker-compose exec archiver python -m archiver.main --config /app/config.yaml
```

### Schedule (Production)
```bash
# Add to crontab (runs daily at 2 AM)
0 2 * * * cd /path/to/project && docker-compose run --rm archiver
```

