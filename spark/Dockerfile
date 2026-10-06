# Image Spark commune (master, worker, spark-submit) avec Iceberg + accès S3/MinIO
ARG SPARK_IMAGE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
FROM ${SPARK_IMAGE}

ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262
ARG SPARK_VERSION=3.5.3
ARG KAFKA_CLIENTS_VERSION=3.4.1

USER root
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*; \
    M=https://repo1.maven.org/maven2; \
    cd /opt/spark/jars; \
    # Iceberg (catalogue REST + S3FileIO)
    curl -fsSLO $M/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fsSLO $M/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    # Lecture des CSV bruts en s3a://
    curl -fsSLO $M/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fsSLO $M/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar; \
    # Niveau 3 : source / sink Kafka pour Spark Structured Streaming
    curl -fsSLO $M/org/apache/spark/spark-sql-kafka-0-10_2.12/${SPARK_VERSION}/spark-sql-kafka-0-10_2.12-${SPARK_VERSION}.jar; \
    curl -fsSLO $M/org/apache/spark/spark-token-provider-kafka-0-10_2.12/${SPARK_VERSION}/spark-token-provider-kafka-0-10_2.12-${SPARK_VERSION}.jar; \
    curl -fsSLO $M/org/apache/kafka/kafka-clients/${KAFKA_CLIENTS_VERSION}/kafka-clients-${KAFKA_CLIENTS_VERSION}.jar; \
    curl -fsSLO $M/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar; \
    # Checkpoints du streaming (volume partagé driver / executors), appartenant à l'utilisateur spark
    mkdir -p /opt/spark-checkpoints && chown -R spark:spark /opt/spark-checkpoints

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY conf/spark-defaults.conf /opt/spark/conf/spark-defaults.conf
COPY jobs /opt/spark-apps

ENV PYTHONPATH=/opt/spark-apps \
    PYSPARK_PYTHON=python3
WORKDIR /opt/spark-apps
USER spark
