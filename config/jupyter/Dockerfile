# ---------------------------------------------------------------------------
# Spark + JupyterLab image with Apache Iceberg wired in.
#
# The base image ships PySpark but no Iceberg, and Iceberg cannot talk to S3
# without its AWS bundle. Both jars are fetched at build time and dropped into
# $SPARK_HOME/jars, which Spark loads automatically -- no --jars flag needed in
# the notebooks.
# ---------------------------------------------------------------------------
FROM jupyter/pyspark-notebook:spark-3.5.0

ARG ICEBERG_VERSION=1.9.1
ARG MAVEN_CENTRAL=https://repo1.maven.org/maven2

USER root

RUN set -eux; \
    SPARK_JARS="${SPARK_HOME}/jars"; \
    curl -fsSL -o "${SPARK_JARS}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar" \
      "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar"; \
    curl -fsSL -o "${SPARK_JARS}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar" \
      "${MAVEN_CENTRAL}/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar"; \
    rm -rf /var/lib/apt/lists/*

# Do not let Spark bind the UI to every interface inside the container.
ENV SPARK_LOCAL_IP=0.0.0.0

# PySpark ships inside $SPARK_HOME rather than as a conda package, so put it on
# the path. Leave PYSPARK_PYTHON unset: the notebook kernel is conda's Python
# (3.11), and pointing Spark at /usr/bin/python3 (the OS's 3.10) makes the
# driver and workers disagree on the Python version.
ENV PYTHONPATH=${SPARK_HOME}/python:${SPARK_HOME}/python/lib/py4j-0.10.9.7-src.zip

USER jovyan
