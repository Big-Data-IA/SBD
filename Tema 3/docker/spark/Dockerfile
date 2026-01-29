FROM openjdk:17.0.2-slim-bullseye AS builder

# Add Dependencies for PySpark 
RUN apt-get update && apt-get install -y \
    curl vim wget software-properties-common ssh ca-certificates \
    python3 python3-pip python3-numpy python3-matplotlib python3-scipy python3-pandas \
    unzip

# Set python3 as default
RUN update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1

# Install PySpark
RUN pip3 install pyspark

# Fix the value of PYTHONHASHSEED
ENV SPARK_VERSION=4.0.1 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

# Copy Spark binaries
COPY resources/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz /tmp/spark.tgz


RUN mkdir -p /opt/spark \
    && tar -xf /tmp/spark.tgz -C /opt/spark --strip-components=1 \
    && rm /tmp/spark.tgz

# ------------------------------
# Final image
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

# Spark logs to stdout
RUN mkdir -p $SPARK_LOG_DIR && \
    touch $SPARK_MASTER_LOG && \
    touch $SPARK_WORKER_LOG && \
    ln -sf /dev/stdout $SPARK_MASTER_LOG && \
    ln -sf /dev/stdout $SPARK_WORKER_LOG

# Copy start script
COPY /scripts/start-spark.sh /
RUN sed -i 's/\r$//g' /start-spark.sh
RUN chmod +x /start-spark.sh

# Set default CMD
CMD ["/bin/bash", "/start-spark.sh"]
