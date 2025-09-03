# PySpark Jobs Documentation

Welcome to the PySpark Jobs documentation. This directory contains comprehensive guides for development, deployment, and troubleshooting.

## 📖 Documentation Structure

### 🗄️ [Database](./database/)
Database connectivity, performance optimization, and configuration guides:

- **[Aurora PostgreSQL Optimization](./database/AURORA_POSTGRESQL_OPTIMIZATION.md)** - AWS Aurora-specific optimizations and performance tuning
- **[Database Connectivity](./database/DATABASE_CONNECTIVITY.md)** - Why we use pg8000 instead of psycopg2-binary for EMR compatibility
- **[PostgreSQL JDBC Configuration](./database/POSTGRESQL_JDBC_CONFIGURATION.md)** - Setting up JDBC drivers for EMR Serverless
- **[PostgreSQL Performance Optimization](./database/POSTGRESQL_PERFORMANCE_OPTIMIZATION.md)** - Complete performance optimization guide with 3-10x speedup techniques
- **[Fix psycopg2 Issues](./database/FIX_PSYCOPG2.md)** - Historical context and solutions for psycopg2-binary problems

### 🚀 [Deployment](./deployment/)
Build processes, artifacts, and deployment guides:

- **[Build Guide](./deployment/BUILD_GUIDE.md)** - Step-by-step guide for building and deploying to AWS EMR Serverless
- **[Build Artifacts Guide](./deployment/BUILD_ARTIFACTS_GUIDE.md)** - Understanding the dual-artifact approach (dist/ vs build_output/)
- **[Parquet to SQLite Guide](./deployment/PARQUET_TO_SQLITE_GUIDE.md)** - Converting parquet files to SQLite databases for analysis and backup

### 🏗️ [Architecture](./architecture/)
System architecture, logging, and design decisions:

- **[JDBC Dependency Architecture](./architecture/JDBC_DEPENDENCY_ARCHITECTURE.md)** - Why JDBC drivers aren't included in Python dependencies
- **[Logging Configuration](./architecture/LOGGING.md)** - Comprehensive logging setup and usage guide

### 🧪 [Testing](./testing/)
Local testing setup and best practices:

- **[Local Testing Guide](./testing/LOCAL_TESTING_GUIDE.md)** - Comprehensive guide for setting up and running tests locally
- **[Testing Setup Summary](./testing/TESTING_SETUP_SUMMARY.md)** - Quick reference for the testing infrastructure

### 🔧 [Troubleshooting](./troubleshooting/)
Common issues and their solutions:

- **[AWS Secrets Manager Troubleshooting](./troubleshooting/TROUBLESHOOTING_SECRETS_MANAGER.md)** - Resolving Secrets Manager issues in EMR Serverless
- **[COPY Protocol Removal](./troubleshooting/COPY_PROTOCOL_REMOVAL.md)** - Why COPY protocol was removed and what replaced it

## 🎯 Quick Start Guides

### For New Developers
1. Start with **[Local Testing Guide](./testing/LOCAL_TESTING_GUIDE.md)** to set up your development environment
2. Review **[Database Connectivity](./database/DATABASE_CONNECTIVITY.md)** to understand database setup
3. Check **[Build Guide](./deployment/BUILD_GUIDE.md)** for deployment basics

### For Performance Optimization
1. **[PostgreSQL Performance Optimization](./database/POSTGRESQL_PERFORMANCE_OPTIMIZATION.md)** - 3-10x speedup techniques
2. **[Aurora PostgreSQL Optimization](./database/AURORA_POSTGRESQL_OPTIMIZATION.md)** - Aurora-specific optimizations

### For Deployment Issues
1. **[Troubleshooting](./troubleshooting/)** - Start here for common issues
2. **[Build Artifacts Guide](./deployment/BUILD_ARTIFACTS_GUIDE.md)** - Understanding build outputs
3. **[JDBC Configuration](./database/POSTGRESQL_JDBC_CONFIGURATION.md)** - Database connection issues

### For Architecture Understanding
1. **[JDBC Dependency Architecture](./architecture/JDBC_DEPENDENCY_ARCHITECTURE.md)** - System design decisions
2. **[Logging Guide](./architecture/LOGGING.md)** - Comprehensive logging setup

## 📊 Documentation Categories

| Category | Purpose | Key Documents |
|----------|---------|---------------|
| **Database** | Database connectivity and performance | PostgreSQL guides, Aurora optimization |
| **Deployment** | Building and deploying applications | Build guides, artifact management |
| **Architecture** | System design and logging | JDBC architecture, logging setup |
| **Testing** | Local development and testing | Test setup, best practices |
| **Troubleshooting** | Problem solving and fixes | Common issues, solutions |

## 🔍 Finding What You Need

### By Topic
- **Performance Issues** → [Database](./database/) + [Troubleshooting](./troubleshooting/)
- **Build/Deploy Problems** → [Deployment](./deployment/) + [Troubleshooting](./troubleshooting/)
- **Development Setup** → [Testing](./testing/) + [Architecture](./architecture/)
- **Understanding System** → [Architecture](./architecture/) + [Database](./database/)

### By Experience Level
- **Beginner** → Start with [Testing](./testing/) and [Build Guide](./deployment/BUILD_GUIDE.md)
- **Intermediate** → Focus on [Database](./database/) and [Deployment](./deployment/)
- **Advanced** → Deep dive into [Architecture](./architecture/) and [Troubleshooting](./troubleshooting/)

## 🤝 Contributing to Documentation

When adding new documentation:

1. **Choose the right category** based on the primary purpose
2. **Follow naming conventions** - use descriptive, consistent names
3. **Update this README** to include links to new documents
4. **Cross-reference related documents** to help users find connected information
5. **Include practical examples** and code snippets where helpful

## 📝 Documentation Standards

All documentation follows these standards:
- ✅ **Clear headings** with emoji indicators for easy scanning
- ✅ **Practical examples** with code snippets and commands
- ✅ **Step-by-step instructions** for complex procedures
- ✅ **Cross-references** to related documents
- ✅ **Troubleshooting sections** for common issues
- ✅ **Performance benchmarks** where applicable

---

**Need help?** Start with the most relevant category above, or check the [Testing Guide](./testing/LOCAL_TESTING_GUIDE.md) for development setup.
