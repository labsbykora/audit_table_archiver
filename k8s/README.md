# Kubernetes Deployment Manifests

Complete Kubernetes manifests for deploying the Audit Table Archiver.

## Files

- `namespace.yaml` - Namespace for archiver resources
- `serviceaccount.yaml` - ServiceAccount for IAM integration
- `configmap.yaml` - Configuration (sanitized, no secrets)
- `secret.yaml` - Sensitive credentials (database passwords, S3 keys)
- `cronjob-archiver.yaml` - CronJob for scheduled archival operations
- `cronjob-restore.yaml` - CronJob for restore operations (disabled by default)
- `kustomization.yaml` - Kustomize configuration for easy deployment

## Quick Start

### 1. Update Configuration

**Update `configmap.yaml`:**
- Set S3 bucket name
- Set database host and connection details
- Update table configurations

**Update `secret.yaml`:**
- Set database password
- Set S3 credentials (if not using IAM roles)

**Update `cronjob-archiver.yaml` and `cronjob-restore.yaml`:**
- Set container image (default: `ghcr.io/labsbykora/audit_table_archiver:latest`)
- Adjust schedule if needed
- Update resource limits if needed

### 2. Deploy

**Option A: Using Kustomize (Recommended)**
```bash
kubectl apply -k k8s/
```

**Option B: Deploy individually**
```bash
kubectl apply -f k8s/namespace.yaml
kubectl apply -f k8s/serviceaccount.yaml
kubectl apply -f k8s/configmap.yaml
kubectl apply -f k8s/secret.yaml
kubectl apply -f k8s/cronjob-archiver.yaml
kubectl apply -f k8s/cronjob-restore.yaml
```

### 3. Verify Deployment

```bash
# Check namespace
kubectl get namespace audit-archiver

# Check ConfigMap
kubectl get configmap archiver-config -n audit-archiver

# Check Secret (values will be hidden)
kubectl get secret archiver-secrets -n audit-archiver

# Check CronJobs
kubectl get cronjobs -n audit-archiver

# Check next scheduled run
kubectl get cronjob archiver-cronjob -n audit-archiver -o jsonpath='{.status.lastScheduleTime}'
```

## Configuration

### CronJob Schedule

The archiver CronJob runs daily at 2:00 AM UTC by default. To change:

Edit `cronjob-archiver.yaml`:
```yaml
spec:
  schedule: "0 2 * * *"  # minute hour day month day-of-week
```

Examples:
- `"0 2 * * *"` - Daily at 2 AM
- `"0 */6 * * *"` - Every 6 hours
- `"0 2 * * 0"` - Every Sunday at 2 AM
- `"0 2 1 * *"` - First day of month at 2 AM

### Resource Limits

Default resources:
- Requests: 512Mi memory, 500m CPU
- Limits: 2Gi memory, 2000m CPU

Adjust in `cronjob-archiver.yaml` based on your workload.

### IAM Integration (AWS EKS)

For AWS S3 access via IAM roles:

1. Create IAM role with S3 permissions
2. Update `serviceaccount.yaml`:
```yaml
annotations:
  eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT_ID:role/archiver-role
```

3. Remove S3 credentials from Secret (not needed with IAM)

### IAM Integration (GKE)

For GCP Cloud Storage via Workload Identity:

1. Create GCP service account
2. Update `serviceaccount.yaml`:
```yaml
annotations:
  iam.gke.io/gcp-service-account: archiver@PROJECT_ID.iam.gserviceaccount.com
```

## Monitoring

### View CronJob Status

```bash
# List CronJobs
kubectl get cronjobs -n audit-archiver

# View CronJob details
kubectl describe cronjob archiver-cronjob -n audit-archiver

# View job history
kubectl get jobs -n audit-archiver -l app=audit-archiver
```

### View Job Logs

```bash
# Get latest job
JOB=$(kubectl get jobs -n audit-archiver -l component=archiver --sort-by=.metadata.creationTimestamp -o jsonpath='{.items[-1].metadata.name}')

# Get pod for job
POD=$(kubectl get pods -n audit-archiver -l job-name=$JOB -o jsonpath='{.items[0].metadata.name}')

# View logs
kubectl logs $POD -n audit-archiver

# Follow logs
kubectl logs -f $POD -n audit-archiver
```

### Check Job Status

```bash
# View all jobs
kubectl get jobs -n audit-archiver

# View job details
kubectl describe job <job-name> -n audit-archiver

# View pod status
kubectl get pods -n audit-archiver -l component=archiver
```

## Manual Execution

### Run Archiver Manually

```bash
# Create one-time job from CronJob
kubectl create job --from=cronjob/archiver-cronjob archiver-manual-$(date +%s) -n audit-archiver

# Or run with custom args
kubectl run archiver-manual \
  --image=ghcr.io/labsbykora/audit_table_archiver:latest \
  --restart=Never \
  --rm -it \
  -n audit-archiver \
  --env="DB_PASSWORD=$(kubectl get secret archiver-secrets -n audit-archiver -o jsonpath='{.data.DB_PASSWORD}' | base64 -d)" \
  -- python -m archiver.main --config /app/config/config.yaml --verbose
```

### Run Restore Manually

```bash
# Enable restore CronJob temporarily
kubectl patch cronjob archiver-restore-cronjob -n audit-archiver -p '{"spec":{"suspend":false}}'

# Create one-time job
kubectl create job --from=cronjob/archiver-restore-cronjob restore-manual-$(date +%s) -n audit-archiver

# Disable after use
kubectl patch cronjob archiver-restore-cronjob -n audit-archiver -p '{"spec":{"suspend":true}}'
```

## Troubleshooting

### CronJob Not Running

```bash
# Check CronJob status
kubectl describe cronjob archiver-cronjob -n audit-archiver

# Check if suspended
kubectl get cronjob archiver-cronjob -n audit-archiver -o jsonpath='{.spec.suspend}'

# Check schedule
kubectl get cronjob archiver-cronjob -n audit-archiver -o jsonpath='{.spec.schedule}'
```

### Job Failing

```bash
# Get failed jobs
kubectl get jobs -n audit-archiver -l component=archiver --field-selector status.successful!=1

# View job events
kubectl describe job <job-name> -n audit-archiver

# View pod logs
kubectl logs <pod-name> -n audit-archiver
```

### Configuration Issues

```bash
# View ConfigMap
kubectl get configmap archiver-config -n audit-archiver -o yaml

# Verify Secret exists
kubectl get secret archiver-secrets -n audit-archiver

# Test config syntax
kubectl get configmap archiver-config -n audit-archiver -o jsonpath='{.data.config\.yaml}' | python -m yaml
```

## Updating Configuration

### Update ConfigMap

```bash
# Edit ConfigMap
kubectl edit configmap archiver-config -n audit-archiver

# Or apply updated file
kubectl apply -f k8s/configmap.yaml
```

**Note**: CronJobs use the ConfigMap at job start time. Changes take effect on the next scheduled run.

### Update Secret

```bash
# Edit Secret
kubectl edit secret archiver-secrets -n audit-archiver

# Or update specific key
kubectl create secret generic archiver-secrets \
  --from-literal=DB_PASSWORD='new-password' \
  --dry-run=client -o yaml | kubectl apply -f - -n audit-archiver
```

### Update Image

```bash
# Update CronJob image
kubectl set image cronjob/archiver-cronjob archiver=ghcr.io/labsbykora/audit_table_archiver:v1.0.1 -n audit-archiver
```

## Cleanup

```bash
# Delete all resources
kubectl delete -k k8s/

# Or delete individually
kubectl delete cronjob archiver-cronjob archiver-restore-cronjob -n audit-archiver
kubectl delete configmap archiver-config -n audit-archiver
kubectl delete secret archiver-secrets -n audit-archiver
kubectl delete serviceaccount archiver-serviceaccount -n audit-archiver
kubectl delete namespace audit-archiver
```

## Security Best Practices

1. **Never commit secrets** - Use `secret.yaml` with placeholders, set values via `kubectl`
2. **Use IAM roles** - Prefer ServiceAccount with IAM integration over access keys
3. **Limit RBAC** - ServiceAccount only needs read access to ConfigMap/Secret
4. **Network policies** - Restrict pod-to-pod communication if needed
5. **Image scanning** - Scan container images for vulnerabilities
6. **Resource limits** - Always set resource requests and limits

## Production Checklist

- [ ] Updated ConfigMap with production values
- [ ] Set Secret values (not placeholders)
- [ ] Updated container image to production tag
- [ ] Configured IAM roles (if using AWS/GCP)
- [ ] Set appropriate resource limits
- [ ] Configured monitoring/alerting
- [ ] Tested manual job execution
- [ ] Verified CronJob schedule
- [ ] Set up log aggregation
- [ ] Documented runbook for operations team

