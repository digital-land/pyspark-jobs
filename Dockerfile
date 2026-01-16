# EMR Serverless Spark 7.9.0 (Spark 3.5.x / Scala 2.12)
FROM public.ecr.aws/emr-serverless/spark/emr-7.9.0:latest

# Switch to root to install JARs and Python dependencies
USER root

# Download Apache Sedona and PostgreSQL JDBC driver
RUN set -eux; \
    mkdir -p /usr/lib/spark/jars && \
    curl -L -o /usr/lib/spark/jars/sedona-spark-shaded-3.5_2.12-1.8.0.jar https://repo1.maven.org/maven2/org/apache/sedona/sedona-spark-shaded-3.5_2.12/1.8.0/sedona-spark-shaded-3.5_2.12-1.8.0.jar && \
    curl -L -o /usr/lib/spark/jars/postgresql-42.7.4.jar https://jdbc.postgresql.org/download/postgresql-42.7.4.jar

# Install Python dependencies
RUN python3 -m pip install --no-cache-dir apache-sedona==1.8.0 pandas==2.2.3

# Set ownership and permissions for hadoop user
RUN chown -R hadoop:hadoop /usr/lib/spark/jars && chmod -R a+r /usr/lib/spark/jars

# Switch back to hadoop user for runtime
USER hadoop:hadoop