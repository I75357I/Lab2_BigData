FROM eclipse-temurin:17-jdk-jammy

RUN apt-get update && apt-get install -y \
    python3 \
    python3-pip \
    procps \
    && rm -rf /var/lib/apt/lists/* \
    && ln -sf /usr/bin/python3 /usr/bin/python

ENV JAVA_HOME=/opt/java/openjdk
ENV PYTHONUNBUFFERED=1

COPY requirements.txt /tmp/
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Suppress Spark/Hadoop WARN/INFO logs at JVM level (before setLogLevel)
COPY log4j.properties /opt/spark-log4j/log4j.properties
ENV SPARK_SUBMIT_OPTS="-Dlog4j.configuration=file:/opt/spark-log4j/log4j.properties"

WORKDIR /app
