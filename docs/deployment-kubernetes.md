# Kubernetes Deployment Guide

Complete guide for deploying the Audit Table Archiver on Kubernetes.

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Deployment Architecture](#deployment-architecture)
3. [Configuration](#configuration)
4. [Deployment Manifests](#deployment-manifests)
5. [Service Accounts & RBAC](#service-accounts--rbac)
6. [Secrets Management](#secrets-management)
7. [Monitoring & Observability](#monitoring--observability)
8. [Scaling & High Availability](#scaling--high-availability)
9. [Troubleshooting](#troubleshooting)

## Prerequisites

- Kubernetes cluster (1.20+)
- kubectl configured
- Helm 3.x (optional, for easier management)
- PostgreSQL database accessible from cluster
- S3-compatible storage (AWS S3, MinIO, etc.)

## Deployment Architecture

### Recommended Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                    │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │              CronJob (Scheduled)                  │  │
│  │  - Runs daily at 2 AM                            │  │
│  │  - One pod per run                               │  │
│  │  - Auto-cleanup after completion                 │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │         ConfigMap (Configuration)                 │  │
│  │  - config.yaml (sanitized, no secrets)            │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │         Secret (Credentials)                     │  │
│  │  - Database passwords                            │  │
│  │  - S3 credentials (if not using IAM)             │  │
│  └──────────────────────────────────────────────────┘  │
│                                                          │
│  ┌──────────────────────────────────────────────────┐  │
│  │    ServiceAccount (IAM Integration)              │  │
│  │  - IRSA for AWS (if using EKS)                   │  │
│  │  - Workload Identity for GKE                     │  │
│  └──────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

## Configuration

### ConfigMap

Create a ConfigMap with your configuration (sanitized, no secrets):

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: archiver-config
  namespace: audit-archiver
data:
  config.yaml: |
    version: "2.0"
    
    s3:
      endpoint: null  # null for AWS S3
      bucket: audit-archives
      prefix: archives/
      region: us-east-1
      storage_class: STANDARD_IA
    
    defaults:
      retention_days: 90
      batch_size: 10000
    
    databases:
      - name: production_db
        host: db.example.com
        port: 5432
        user: archiver
        password_env: DB_PASSWORD  # From Secret
        tables:
          - name: audit_logs
            schema: public
            timestamp_column: created_at
            primary_key: id
            retention_days: 90
```

### Secret

Create a Secret with database passwords:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: archiver-secrets
  namespace: audit-archiver
type: Opaque
stringData:
  DB_PASSWORD: "your-secure-password"
  # Add more passwords if multiple databases
  DB_PASSWORD_PROD: "prod-password"
```

**⚠️ Security Best Practice**: Use external secret management (AWS Secrets Manager, HashiCorp Vault) with operators instead of Kubernetes Secrets for production.

## Deployment Manifests

### CronJob Deployment

```yaml
apiVersion: batch/v1
kind: CronJob
metadata:
  name: audit-archiver
  namespace: audit-archiver
spec:
  schedule: "0 2 * * *"  # Daily at 2 AM UTC
  concurrencyPolicy: Forbid  # Prevent concurrent runs
  successfulJobsHistoryLimit: 3
  failedJobsHistoryLimit: 3
  jobTemplate:
    spec:
      backoffLimit: 0  # Don't retry on failure (use manual intervention)
      activeDeadlineSeconds: 86400  # 24 hours max
      template:
        metadata:
          labels:
            app: audit-archiver
        spec:
          serviceAccountName: archiver-sa
          restartPolicy: Never
          containers:
          - name: archiver
            image: audit-archiver:1.0.0
            imagePullPolicy: IfNotPresent
            command:
            - python
            - -m
            - archiver.main
            - --config
            - /etc/archiver/config.yaml
            - --log-format
            - json
            env:
            - name: DB_PASSWORD
              valueFrom:
                secretKeyRef:
                  name: archiver-secrets
                  key: DB_PASSWORD
            - name: AWS_REGION
              value: "us-east-1"
            # For AWS S3, use IRSA instead of access keys
            # - name: AWS_ACCESS_KEY_ID
            #   valueFrom:
            #     secretKeyRef:
            #       name: archiver-secrets
            #       key: AWS_ACCESS_KEY_ID
            volumeMounts:
            - name: config
              mountPath: /etc/archiver
              readOnly: true
            resources:
              requests:
                memory: "512Mi"
                cpu: "500m"
              limits:
                memory: "2Gi"
                cpu: "2000m"
          volumes:
          - name: config
            configMap:
              name: archiver-config
```

### Manual Job (One-Time Run)

For manual runs or testing:

```yaml
apiVersion: batch/v1
kind: Job
metadata:
  name: audit-archiver-manual
  namespace: audit-archiver
spec:
  backoffLimit: 0
  activeDeadlineSeconds: 86400
  template:
    metadata:
      labels:
        app: audit-archiver
    spec:
      serviceAccountName: archiver-sa
      restartPolicy: Never
      containers:
      - name: archiver
        image: audit-archiver:1.0.0
        imagePullPolicy: IfNotPresent
        command:
        - python
        - -m
        - archiver.main
        - --config
        - /etc/archiver/config.yaml
        - --database
        - production_db
        - --table
        - audit_logs
        env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: archiver-secrets
              key: DB_PASSWORD
        volumeMounts:
        - name: config
          mountPath: /etc/archiver
          readOnly: true
        resources:
          requests:
            memory: "512Mi"
            cpu: "500m"
          limits:
            memory: "2Gi"
            cpu: "2000m"
      volumes:
      - name: config
        configMap:
          name: archiver-config
```

## Service Accounts & RBAC

### Service Account

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: archiver-sa
  namespace: audit-archiver
```

### AWS IRSA (EKS)

If using AWS S3 with EKS, set up IRSA:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: archiver-sa
  namespace: audit-archiver
  annotations:
    eks.amazonaws.com/role-arn: arn:aws:iam::ACCOUNT_ID:role/archiver-s3-role
```

IAM Role Policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:PutObject",
        "s3:GetObject",
        "s3:DeleteObject",
        "s3:ListBucket"
      ],
      "Resource": [
        "arn:aws:s3:::audit-archives",
        "arn:aws:s3:::audit-archives/*"
      ]
    }
  ]
}
```

### GKE Workload Identity

If using GKE with Google Cloud Storage:

```yaml
apiVersion: v1
kind: ServiceAccount
metadata:
  name: archiver-sa
  namespace: audit-archiver
  annotations:
    iam.gke.io/gcp-service-account: archiver-sa@PROJECT_ID.iam.gserviceaccount.com
```

## Secrets Management

### Option 1: Kubernetes Secrets (Simple)

```bash
kubectl create secret generic archiver-secrets \
  --from-literal=DB_PASSWORD='your-password' \
  -n audit-archiver
```

### Option 2: External Secrets Operator (Recommended)

Use External Secrets Operator to sync from AWS Secrets Manager:

```yaml
apiVersion: external-secrets.io/v1beta1
kind: ExternalSecret
metadata:
  name: archiver-secrets
  namespace: audit-archiver
spec:
  secretStoreRef:
    name: aws-secrets-manager
    kind: SecretStore
  target:
    name: archiver-secrets
    creationPolicy: Owner
  data:
  - secretKey: DB_PASSWORD
    remoteRef:
      key: archiver/db-password
```

## Monitoring & Observability

### Prometheus ServiceMonitor

```yaml
apiVersion: monitoring.coreos.com/v1
kind: ServiceMonitor
metadata:
  name: archiver-metrics
  namespace: audit-archiver
spec:
  selector:
    matchLabels:
      app: audit-archiver
  endpoints:
  - port: metrics
    path: /metrics
    interval: 30s
```

### Logging

Logs are output to stdout in JSON format. Use a log collector (Fluentd, Fluent Bit, etc.) to forward to your logging system.

Example Fluent Bit configuration:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: fluent-bit-config
  namespace: audit-archiver
data:
  fluent-bit.conf: |
    [INPUT]
        Name              tail
        Path              /var/log/containers/audit-archiver*.log
        Parser            json
        Tag               archiver.*
    
    [OUTPUT]
        Name              es
        Match             archiver.*
        Host              elasticsearch.logging.svc.cluster.local
        Port              9200
        Index             archiver-logs
```

## Scaling & High Availability

### Multi-Database Parallel Processing

The archiver supports multiple databases. To process them in parallel:

1. **Option 1**: Run separate CronJobs per database
2. **Option 2**: Use a single CronJob with parallel processing (built-in)

### Resource Limits

Adjust based on your workload:

```yaml
resources:
  requests:
    memory: "512Mi"  # Increase for large batches
    cpu: "500m"
  limits:
    memory: "2Gi"    # Increase for large datasets
    cpu: "2000m"
```

### Node Affinity

Pin to specific node pools if needed:

```yaml
affinity:
  nodeAffinity:
    requiredDuringSchedulingIgnoredDuringExecution:
      nodeSelectorTerms:
      - matchExpressions:
        - key: workload-type
          operator: In
          values:
          - batch-processing
```

## Troubleshooting

### View Logs

```bash
# View CronJob logs
kubectl logs -n audit-archiver -l app=audit-archiver --tail=100

# View specific job
kubectl logs -n audit-archiver job/audit-archiver-1234567890
```

### Check Job Status

```bash
# List jobs
kubectl get jobs -n audit-archiver

# Describe job
kubectl describe job -n audit-archiver audit-archiver-1234567890
```

### Debug Failed Jobs

```bash
# Get pod name
kubectl get pods -n audit-archiver -l app=audit-archiver

# Exec into pod (if still running)
kubectl exec -it -n audit-archiver <pod-name> -- /bin/sh

# Check events
kubectl get events -n audit-archiver --sort-by='.lastTimestamp'
```

### Common Issues

1. **Job Timeout**: Increase `activeDeadlineSeconds`
2. **Memory Issues**: Increase memory limits or reduce batch size
3. **Database Connection**: Verify network policies and credentials
4. **S3 Access**: Verify IAM role/permissions

## Production Checklist

- [ ] ConfigMap created with sanitized config
- [ ] Secrets created (or External Secrets configured)
- [ ] ServiceAccount created with proper IAM role (if using IRSA)
- [ ] CronJob schedule configured
- [ ] Resource limits set appropriately
- [ ] Monitoring configured (Prometheus, logging)
- [ ] Network policies configured (if using)
- [ ] Backup/restore procedures documented
- [ ] Runbook created for common issues

## Next Steps

1. Deploy to staging environment first
2. Run manual job to verify configuration
3. Monitor first few scheduled runs
4. Adjust resource limits based on actual usage
5. Set up alerts for failures
6. Document environment-specific procedures

---

**See Also**:
- [Operations Manual](operations-manual.md)
- [Troubleshooting Guide](troubleshooting.md)
- [Security & Credentials](security-credentials.md)

