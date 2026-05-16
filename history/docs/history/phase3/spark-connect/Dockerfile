FROM openjdk:17-jdk-slim

RUN apt-get update && \
    apt-get install -y curl unzip netcat-openbsd python3 python3-pip procps && \
    rm -rf /var/lib/apt/lists/*

ENV SPARK_VERSION=3.5.6
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin
ENV HOME=/tmp

# Spark 다운로드 (hadoop3)
RUN curl -fSL https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop3.tgz \
    -o /tmp/spark.tgz && \
    mkdir -p /opt && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark.tgz

# pyspark (클라이언트 라이브러리, 필요하면)
RUN pip3 install --no-cache-dir pyspark[connect]==3.5.6

# 사용자 생성
RUN useradd -u 1001 -m sparkuser || true

# 엔트리포인트 복사하고 권한 설정 -> yaml 에서 실행하도록 변경
# COPY entrypoint.sh /opt/entrypoint.sh
# RUN chmod +x /opt/entrypoint.sh

USER sparkuser
ENV HADOOP_USER_NAME=sparkuser

# ENTRYPOINT ["/opt/entrypoint.sh"]
