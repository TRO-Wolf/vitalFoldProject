FROM apache/airflow:3.1.3

USER root

# ── System dependencies ─────────────────────────────────────────────
# gettext-base:          provides envsubst for spark-defaults.conf templating
# libgomp1:              existing (ML libs)
# openjdk-17-jre:        Spark driver JVM (must match cluster Java 17)
# procps:                spark-submit uses `ps` internally
# wget:                  for downloading Spark + JARs
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
        libgomp1 \
        openjdk-17-jre-headless \
        gettext-base \
        procps \
        wget && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# ── Spark 3.5.8 binary distribution ─────────────────────────────────
# Match the cluster exactly (apache/spark:3.5.8-scala2.12-java17-ubuntu)
ENV SPARK_VERSION=3.5.8
ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
# /opt/airflow: makes the top-level includes/ package importable
#               (moved from dags/includes/ to follow Astronomer layout)
ENV PYTHONPATH=/opt/airflow:$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

RUN wget -qO /tmp/spark.tgz \
        https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 ${SPARK_HOME} && \
    rm /tmp/spark.tgz && \
    chown -R airflow:root ${SPARK_HOME}

# ── Iceberg / AWS / S3 Tables JARs (exact version match with cluster) ─
ENV ICEBERG_VERSION=1.10.0 \
    HADOOP_VERSION=3.3.4 \
    SCALA_VERSION=2.12 \
    S3_TABLES_VERSION=0.1.8 \
    AWS_SDK_VERSION=2.29.38 \
    AWS_SDK_V1_VERSION=1.12.262

RUN wget -P ${SPARK_HOME}/jars/ \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_${SCALA_VERSION}/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_${SCALA_VERSION}-${ICEBERG_VERSION}.jar && \
    wget -P ${SPARK_HOME}/jars/ \
        https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar && \
    wget -P ${SPARK_HOME}/jars/ \
        https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/${AWS_SDK_VERSION}/bundle-${AWS_SDK_VERSION}.jar && \
    wget -P ${SPARK_HOME}/jars/ \
        https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    wget -P ${SPARK_HOME}/jars/ \
        https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar && \
    wget -P ${SPARK_HOME}/jars/ \
        https://repo1.maven.org/maven2/software/amazon/s3tables/s3-tables-catalog-for-iceberg-runtime/${S3_TABLES_VERSION}/s3-tables-catalog-for-iceberg-runtime-${S3_TABLES_VERSION}.jar

# ── Spark defaults (catalogs, S3, memory) must match the cluster ────
# Template is resolved at container startup by entrypoint.sh (envsubst)
COPY spark-defaults.conf.template ${SPARK_HOME}/conf/spark-defaults.conf.template

# spark-defaults.conf has spark.eventLog.enabled=true pointing at
# /tmp/spark-events. That directory is a named-volume mount on the Spark
# cluster containers; on the Airflow worker it has to exist as a regular
# local directory, otherwise SparkContext.__init__ fails before any job can
# run. World-writable so both the airflow user and the `default` user used
# by `docker compose exec` can write to it.
RUN mkdir -p /tmp/spark-events && chmod 1777 /tmp/spark-events

USER airflow

RUN pip install --no-cache-dir uv

COPY requirements.txt .
RUN uv pip install --no-cache-dir -r requirements.txt

# ── Entrypoint: envsubst resolves spark-defaults.conf.template at startup ─
COPY entrypoint.sh /opt/airflow/entrypoint.sh
RUN chmod +x /opt/airflow/entrypoint.sh
ENTRYPOINT ["/opt/airflow/entrypoint.sh"]
