FROM apache/airflow:2.8.1

USER root

# 🧹 Slim Java install (optional: skip if not needed in Airflow)
RUN apt-get update && \
    apt-get install -y openjdk-17-jdk-headless && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

# 🐎 COPY Spark instead of downloading every time
ENV SPARK_VERSION=3.5.0
COPY ./spark-dist/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark-${SPARK_VERSION}-bin-hadoop3
RUN ln -s /opt/spark-${SPARK_VERSION}-bin-hadoop3 /opt/spark

ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

USER airflow

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt && \
    pip install --no-cache-dir psycopg2-binary
