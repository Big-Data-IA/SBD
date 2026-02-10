FROM openjdk:17.0.2-slim-bullseye AS builder

# ------------------------------
# System + Python deps
# ------------------------------
RUN apt-get update && apt-get install -y \
    curl vim wget software-properties-common ssh ca-certificates \
    python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas \
    unzip maven git && \
    rm -rf /var/lib/apt/lists/*

RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1
RUN pip3 install pyspark

# ------------------------------
# Spark
# ------------------------------
ENV SPARK_VERSION=4.0.0 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

COPY resources/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz

RUN mkdir -p /opt/spark && \
    tar -xf /tmp/spark.tgz -C /opt/spark --strip-components=1 && \
    rm /tmp/spark.tgz

# ------------------------------
# Use Maven to pull Hadoop AWS + SDK v2 + Postgres JARs
# ------------------------------
WORKDIR /tmp/maven-build

# Copy the pom.xml from your project
COPY resources/pom.xml .

# Fetch all dependencies into /opt/spark/jars
RUN mvn dependency:copy-dependencies -DoutputDirectory=/opt/spark/jars

# ------------------------------
# Final image
# ------------------------------
FROM builder AS apache-spark

WORKDIR /opt/spark

ENV SPARK_MASTER_PORT=7077 \
    SPARK_MASTER_WEBUI_PORT=8080 \
    SPARK_LOG_DIR=/opt/spark/logs \
    SPARK_MASTER_LOG=/opt/spark/logs/spark-master.out \
    SPARK_WORKER_LOG=/opt/spark/logs/spark-worker.out \
    SPARK_WORKER_WEBUI_PORT=8080 \
    SPARK_WORKER_PORT=7000 \
    SPARK_MASTER="spark://spark-master:7077" \
    SPARK_WORKLOAD="master"

EXPOSE 8080 7077 7000

RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

COPY /scripts/start-spark.sh /
RUN sed -i 's/\r$//g' /start-spark.sh && chmod +x /start-spark.sh

CMD ["/bin/bash", "/start-spark.sh"]
