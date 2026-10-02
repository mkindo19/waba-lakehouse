# Image Airflow capable de soumettre des jobs au cluster Spark (SparkSubmitOperator).
# Étape 1 : on récupère une distribution Spark complète (JRE + Spark + jars Iceberg/S3)
#           depuis l'image Spark officielle, sans installer pyspark via pip.
ARG SPARK_IMAGE=apache/spark:3.5.3-scala2.12-java17-python3-ubuntu
ARG AIRFLOW_IMAGE=apache/airflow:2.10.5-python3.10

FROM ${SPARK_IMAGE} AS spark
ARG ICEBERG_VERSION=1.6.1
ARG HADOOP_AWS_VERSION=3.3.4
ARG AWS_SDK_V1_VERSION=1.12.262
USER root
RUN set -eux; \
    apt-get update && apt-get install -y --no-install-recommends curl && rm -rf /var/lib/apt/lists/*; \
    M=https://repo1.maven.org/maven2; cd /opt/spark/jars; \
    curl -fsSLO $M/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/${ICEBERG_VERSION}/iceberg-spark-runtime-3.5_2.12-${ICEBERG_VERSION}.jar; \
    curl -fsSLO $M/org/apache/iceberg/iceberg-aws-bundle/${ICEBERG_VERSION}/iceberg-aws-bundle-${ICEBERG_VERSION}.jar; \
    curl -fsSLO $M/org/apache/hadoop/hadoop-aws/${HADOOP_AWS_VERSION}/hadoop-aws-${HADOOP_AWS_VERSION}.jar; \
    curl -fsSLO $M/com/amazonaws/aws-java-sdk-bundle/${AWS_SDK_V1_VERSION}/aws-java-sdk-bundle-${AWS_SDK_V1_VERSION}.jar

# Étape 2 : Airflow officiel + Spark copié + provider Spark
FROM ${AIRFLOW_IMAGE}
USER root
COPY --from=spark /opt/java/openjdk /opt/java/openjdk
COPY --from=spark /opt/spark /opt/spark
ENV JAVA_HOME=/opt/java/openjdk \
    SPARK_HOME=/opt/spark \
    PATH=/opt/spark/bin:/opt/java/openjdk/bin:${PATH}
USER airflow
COPY requirements.txt /requirements.txt
# Versions alignées sur les "constraints" officielles d'Airflow (reproductibilité)
RUN pip install --no-cache-dir -r /requirements.txt \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.10.txt"
