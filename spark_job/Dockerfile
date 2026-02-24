# =============================================================================
# Spark Image — Streaming Pipeline
# =============================================================================
# Base: official apache/spark:latest (Spark 4.1.1 / Scala 2.13 / Python 3.10)
#
# This single image is reused for three roles in docker-compose:
#   spark-master  → runs org.apache.spark.deploy.master.Master
#   spark-worker  → runs org.apache.spark.deploy.worker.Worker
#   spark-job     → runs spark-submit with the streaming script
#
# The Kafka connector JARs are downloaded at build time from Maven Central
# so the image is fully self-contained (no internet needed at runtime).
# =============================================================================
FROM apache/spark:latest

# Switch to root to install packages and download JARs
USER root

# ---------------------------------------------------------------------------
# Python dependencies for the Spark driver (spark-job container).
# Installing on all Spark nodes keeps the image consistent; only the driver
# (spark-job) actually imports these libraries.
# ---------------------------------------------------------------------------
RUN pip3 install --no-cache-dir \
    boto3==1.35.0 \
    pymongo==4.8.0 \
    prometheus-client==0.21.0 \
    python-dateutil==2.8.2

# ---------------------------------------------------------------------------
# Spark-Kafka connector JARs
# Required for Spark Structured Streaming to read from Kafka topics.
#
# Versions: Spark 4.1.1 / Scala 2.13 (apache/spark:latest = Spark 4.1.1)
#
# JARs downloaded:
#   1. spark-sql-kafka-0-10_2.13-4.1.1.jar     — main Kafka data source
#   2. spark-token-provider-kafka-0-10_2.13-4.1.1.jar — auth token support
#   3. kafka-clients-3.9.1.jar                  — Kafka Java client (Spark 4.1.1 dep)
#   4. commons-pool2-2.12.1.jar                 — connection pooling (Spark 4.1.1 dep)
# ---------------------------------------------------------------------------
ENV SPARK_JARS=/opt/spark/jars

RUN curl -fSL -o ${SPARK_JARS}/spark-sql-kafka-0-10_2.13-4.1.1.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.13/4.1.1/spark-sql-kafka-0-10_2.13-4.1.1.jar" && \
    curl -fSL -o ${SPARK_JARS}/spark-token-provider-kafka-0-10_2.13-4.1.1.jar \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.13/4.1.1/spark-token-provider-kafka-0-10_2.13-4.1.1.jar" && \
    curl -fSL -o ${SPARK_JARS}/kafka-clients-3.9.1.jar \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.9.1/kafka-clients-3.9.1.jar" && \
    curl -fSL -o ${SPARK_JARS}/commons-pool2-2.12.1.jar \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.1/commons-pool2-2.12.1.jar"

# Copy the streaming job script and give ownership to the spark user (uid 185)
COPY spark_streaming.py /opt/spark_streaming.py
RUN chown 185:185 /opt/spark_streaming.py

# Return to the default spark user (uid 185) — never run as root in production
USER spark
