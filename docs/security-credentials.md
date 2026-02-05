# Credential Management

## Overview

The archiver supports multiple methods for providing database passwords, in order of security preference:

1. **Environment Variables** (Recommended for production)
2. **AWS Secrets Manager** (Phase 2)
3. **HashiCorp Vault** (Phase 2)
4. **Configuration File** (Development only - use with caution)

## Why Environment Variables?

### Security Reasons

1. **Version Control Safety**: Config files are often committed to Git repositories. Passwords in config files risk being exposed in repository history, even if removed later.

2. **Multi-Environment Support**: The same configuration file can be used across development, staging, and production environments, with different passwords provided via environment variables per environment.

3. **Compliance**: Security standards (PCI-DSS, SOC 2, HIPAA) typically require that secrets not be stored in configuration files.

4. **Access Control**: Environment variables can be managed by infrastructure/deployment tools (Kubernetes Secrets, Docker secrets, systemd environment files) with proper access controls.

5. **Secrets Rotation**: Environment variables can be rotated without modifying configuration files.

### Why Not Usernames?

Usernames are **not secrets** - they are identifiers. Unlike passwords:
- Usernames don't provide access on their own
- They're often predictable or documented
- They're not sensitive information
- Many systems expose usernames publicly (e.g., in logs, URLs, APIs)

## Recommended Approach: Environment Variables

```yaml
databases:
  - name: production_db
    host: db.example.com
    user: archiver
    password_env: DB_PASSWORD  # Environment variable name
```

Set the environment variable:
```bash
# Linux/macOS
export DB_PASSWORD=your_secure_password

# Windows PowerShell
$env:DB_PASSWORD="your_secure_password"

# Windows Command Prompt
set DB_PASSWORD=your_secure_password
```

## Development: Config File (Use with Caution)

For local development and testing only, you can specify the password directly in the configuration file:

```yaml
databases:
  - name: local_db
    host: localhost
    user: archiver
    password: dev_password_123  # ⚠️ DEVELOPMENT ONLY
```

**⚠️ WARNING**: 
- Never commit config files with passwords to version control
- Add config files with passwords to `.gitignore`
- Only use this for local development
- Use environment variables for any shared or production environments

The archiver will log a security warning when passwords are provided via config file.

## Production Deployment

### Docker
```yaml
# docker-compose.yml
services:
  archiver:
    environment:
      - DB_PASSWORD=${DB_PASSWORD}  # From .env file or environment
```

### Kubernetes
```yaml
apiVersion: v1
kind: Secret
metadata:
  name: archiver-db-password
stringData:
  password: your-secure-password
---
apiVersion: batch/v1
kind: CronJob
spec:
  template:
    spec:
      containers:
      - name: archiver
        env:
        - name: DB_PASSWORD
          valueFrom:
            secretKeyRef:
              name: archiver-db-password
              key: password
```

### Systemd Service
```ini
[Service]
Environment=DB_PASSWORD=your-secure-password
# Or better: EnvironmentFile=/etc/archiver/secrets
```

## Security Best Practices

1. **Never commit secrets** to version control
2. **Use environment variables** or secret management services in production
3. **Rotate passwords regularly** - environment variables make this easier
4. **Use least privilege** - database user should only have SELECT and DELETE permissions
5. **Monitor access** - log and alert on authentication failures
6. **Use separate credentials** per environment (dev, staging, prod)

## S3 Credentials

Similar to database passwords, S3 credentials support multiple methods:

### Recommended: Environment Variables

```yaml
s3:
  bucket: my-bucket
  # Credentials from environment variables
```

Set environment variables:
```bash
# Linux/macOS
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key

# Windows PowerShell
$env:AWS_ACCESS_KEY_ID="your_access_key"
$env:AWS_SECRET_ACCESS_KEY="your_secret_key"
```

### Development: Config File (Use with Caution)

For local development only:

```yaml
s3:
  bucket: my-bucket
  access_key_id: minioadmin      # ⚠️ DEVELOPMENT ONLY
  secret_access_key: minioadmin  # ⚠️ DEVELOPMENT ONLY
```

**⚠️ WARNING**: 
- Never commit config files with credentials to version control
- Add config files with credentials to `.gitignore`
- Only use this for local development
- Use environment variables for any shared or production environments

The archiver will log a security warning when credentials are provided via config file.

### AWS IAM Roles (Production)

For AWS S3, the recommended approach is to use IAM roles:
- EC2: Attach IAM role to instance
- ECS: Use task execution role
- Lambda: Use execution role
- Kubernetes: Use IRSA (IAM Roles for Service Accounts)

No credentials needed in config or environment when using IAM roles.

## Future: Secret Management Services

Phase 2 will add support for:
- AWS Secrets Manager
- HashiCorp Vault
- Azure Key Vault

These provide:
- Automatic rotation
- Audit trails
- Fine-grained access control
- Integration with IAM/RBAC

