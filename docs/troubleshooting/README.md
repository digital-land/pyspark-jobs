# Troubleshooting Documentation

This directory contains troubleshooting guides and solutions for common issues encountered in the PySpark jobs project.

## Guides

- **[AWS Secrets Manager Troubleshooting](./TROUBLESHOOTING_SECRETS_MANAGER.md)** — Resolving `DataNotFoundError`, EMR-compatible secret retrieval, environment variable fallbacks

## 🚨 Most Common Issues

### 1. **AWS Secrets Manager - DataNotFoundError**
```
botocore.exceptions.DataNotFoundError: Unable to load data for: endpoints
```
**Quick Fix:** Use `get_secret_emr_compatible()` function with built-in fallbacks.

### 2. **JDBC Driver Not Found**
```
java.lang.ClassNotFoundException: org.postgresql.Driver
```
**Quick Fix:** Verify `--jars` parameter includes PostgreSQL JDBC driver in EMR configuration.

### 3. **Import Errors in EMR**
```
ModuleNotFoundError: No module named 'jobs'
```
**Quick Fix:** Check `--py-files` parameter includes your application wheel and dependencies.

## 🔧 Troubleshooting Strategy

### Step 1: Identify the Layer
```
Error Location Analysis:
├── Python Layer Issues
│   ├── Import errors → Check PYTHONPATH and --py-files
│   ├── Package conflicts → Review requirements files
│   └── Runtime errors → Check application logs
│
├── Java/Spark Layer Issues  
│   ├── JDBC errors → Verify --jars parameter
│   ├── Classpath issues → Check Spark configuration
│   └── Memory issues → Adjust Spark memory settings
│
└── Infrastructure Issues
    ├── AWS permissions → Check IAM roles
    ├── Network connectivity → Verify VPC/security groups
    └── Resource limits → Check EMR Serverless quotas
```

### Step 2: Check Common Solutions

| Issue Category | Common Causes | First Steps |
|----------------|---------------|-------------|
| **AWS Services** | Permissions, regions, endpoints | Check IAM roles, try environment variables |
| **Database** | Connectivity, drivers, VPC | Verify security groups, check JDBC configuration |
| **Build/Deploy** | Dependencies, paths, permissions | Rebuild package, check S3 upload |
| **Performance** | Memory, timeouts, batch sizes | Review optimization guides |

### Step 3: Enable Debug Logging
```python
# Add to your entry point command function
import logging

logging.basicConfig(level=logging.DEBUG)
```

## 📊 Error Categories & Solutions

### AWS Integration Errors
- **Secrets Manager**: Use EMR-compatible functions with fallbacks
- **S3 Access**: Check bucket permissions and IAM roles
- **Regional Issues**: Ensure resources are in the same region

### Database Connection Errors  
- **JDBC Driver**: Use --jars parameter for PostgreSQL driver
- **Network**: Configure VPC and security groups properly
- **Authentication**: Use Secrets Manager or environment variables

### Build & Deployment Errors
- **Dependencies**: Use correct requirements files for target environment
- **Packaging**: Follow build guide for proper artifact creation
- **Upload**: Verify AWS credentials and S3 permissions

### Performance Issues
- **Memory**: Adjust Spark driver/executor memory settings
- **Timeouts**: Increase connection and socket timeout values
- **Batch Sizes**: Use performance optimization recommendations

## 🔍 Diagnostic Commands

### Local Testing
```bash
# Verify dependencies
python -c "import pg8000; print('pg8000 available')"

# Run tests
make test
```

### EMR Debugging
```bash
# Check EMR logs
aws logs describe-log-groups --log-group-name-prefix /aws/emr-serverless

# Test AWS connectivity
aws secretsmanager get-secret-value --secret-id your-secret-name

# Verify S3 access
aws s3 ls s3://your-bucket/
```

## 📋 Before Asking for Help

1. **Check the specific troubleshooting guide** for your issue type
2. **Review logs** with DEBUG level enabled
3. **Verify basic connectivity** (AWS credentials, network access)
4. **Test with minimal example** to isolate the problem
5. **Document the exact error** including full stack trace

## 🔗 Related Documentation

- **[Database Documentation](../database/)** - For database connectivity issues
- **[Architecture Documentation](../architecture/)** - For understanding system design
- **[Deployment Documentation](../deployment/)** - For build and deployment issues
- **[Testing Documentation](../testing/)** - For local testing and validation

---

[← Back to Main Documentation](../README.md)
