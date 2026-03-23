# Architecture Documentation

This directory contains documentation about system architecture, design decisions, and infrastructure components.

## 📚 Available Guides

### 🏗️ **System Architecture**
- **[Adding Jobs and Pipelines](./ADDING_JOBS_AND_PIPELINES.md)**
  - How to add new pipelines, jobs, and entry points
  - End-to-end wiring: entry point → job → pipeline → transformer
  - Code examples and testing patterns

- **[JDBC Dependency Architecture](./JDBC_DEPENDENCY_ARCHITECTURE.md)**
  - Why JDBC drivers aren't included in Python dependencies package
  - Separation of Java vs Python runtime environments
  - EMR Serverless design principles and best practices

### 📝 **Logging & Monitoring**
- **[Logging Configuration](./LOGGING.md)**
  - Comprehensive logging setup and usage guide
  - Environment-aware configuration (dev vs production)
  - S3 integration, file rotation, and performance tracking

## 🎯 Quick Navigation

| Need | Start Here |
|------|------------|
| **Understanding JDBC Setup** | [JDBC Dependency Architecture](./JDBC_DEPENDENCY_ARCHITECTURE.md) |
| **Setting Up Logging** | [Logging Configuration](./LOGGING.md) |

## 🏗️ System Architecture Overview

### PySpark Application Layers
```
┌─────────────────────────────────────────────────────────────┐
│                    PySpark Application                      │
├─────────────────────┬─────────────────────┬─────────────────┤
│   Python Layer     │   Java/JVM Layer    │   Infrastructure │
│                     │                     │                 │
│ • Your ETL code     │ • Spark engine      │ • EMR Serverless │
│ • pg8000 driver     │ • JDBC drivers      │ • S3 storage     │
│ • Python packages   │ • JAR dependencies  │ • Secrets Mgr    │
│ • Configuration     │ • Spark runtime     │ • CloudWatch     │
└─────────────────────┴─────────────────────┴─────────────────┘
```

### Dependency Management Strategy
```
Python Dependencies (--py-files):
├── Application code (.whl)
├── Pure Python packages (.zip)
└── Configuration files

Java Dependencies (--jars):
├── JDBC drivers (.jar)
├── Spark extensions (.jar)
└── Java libraries (.jar)

Infrastructure (EMR/AWS):
├── Spark runtime (pre-installed)
├── Java runtime (pre-installed)
└── AWS SDK (pre-installed)
```

## 📊 Logging Architecture

### Multi-Destination Logging
```
Application Logs
├── Console Output (Development)
├── File Logging (Local/Testing)
├── S3 Storage (Production)
└── CloudWatch (EMR Integration)

Log Levels & Environments:
├── Development: DEBUG + detailed formatting
├── Testing: INFO + structured output
└── Production: WARNING + minimal formatting
```

### Logging Features
- **Environment-aware** configuration
- **Multiple output** destinations
- **S3 integration** with buffering
- **Automatic rotation** and cleanup
- **Performance tracking** with decorators
- **Context logging** for request tracing

## 🔧 Design Principles

### 1. **Separation of Concerns**
- **Python dependencies** for application logic
- **Java dependencies** for Spark engine needs
- **Clear boundaries** between layers

### 2. **EMR Serverless Optimization**
- **Leverage pre-installed** components
- **Minimize artifact size** for faster startup
- **Follow AWS best practices** for deployment

### 3. **Cross-Platform Compatibility**
- **Pure Python approach** for portability
- **No compiled binaries** in Python packages
- **Consistent behavior** across environments

### 4. **Performance & Reliability**
- **Efficient dependency loading** into appropriate runtimes
- **Graceful error handling** and fallbacks
- **Comprehensive logging** for debugging

## 📈 Performance Considerations

### Dependency Loading
| Approach | Load Time | Memory | Best For |
|----------|-----------|--------|----------|
| **Maven Central JARs** | Fast | Low | Development |
| **S3-Hosted JARs** | Medium | Low | Production |
| **Bundled JARs** | Slow | Medium | Isolated environments |

### Logging Performance
- **Buffered S3 writes** minimize network calls
- **Lazy evaluation** for debug messages
- **Async logging** for high-throughput applications
- **Log rotation** prevents disk space issues

## 🚨 Common Architecture Questions

### Q: Why separate JDBC from Python packages?
**A:** JDBC drivers are Java libraries that must be loaded into the JVM classpath, not the Python environment. This separation follows EMR best practices and ensures optimal performance.

### Q: Why pg8000 instead of psycopg2?
**A:** pg8000 is pure Python with no compiled binaries, making it compatible across all platforms including EMR Serverless. psycopg2 has platform-specific compiled components that cause deployment issues.

### Q: How does logging scale in production?
**A:** The logging system uses buffered S3 writes, automatic rotation, and environment-specific configurations to handle high-volume production logging efficiently.

### Q: What about dependency conflicts?
**A:** The clear separation between Python and Java dependencies eliminates most conflicts. Python packages are isolated in virtual environments, while Java dependencies are managed through Spark's classpath.

## 🔍 Related Documentation

- **[Database Connectivity](../database/DATABASE_CONNECTIVITY.md)** - pg8000 vs psycopg2 decision details
- **[Build Guide](../deployment/BUILD_GUIDE.md)** - How architecture affects build process
- **[Troubleshooting](../troubleshooting/)** - Common architecture-related issues

---

[← Back to Main Documentation](../README.md)
