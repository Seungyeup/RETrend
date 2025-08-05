# Dockerfile.spark-connect
FROM apache/spark:3.5.6

# root 권한으로 필요한 툴 설치
USER root

RUN apt-get update && \
    apt-get install -y python3 python3-pip netcat-openbsd procps && \
    rm -rf /var/lib/apt/lists/*

# pyspark client (Spark Connect 포함)
RUN pip3 install --no-cache-dir pyspark[connect]==3.5.6

# Spark Connect Server JAR 미리 다운로드
RUN curl -sfL \
    https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.6/spark-connect_2.12-3.5.6.jar \
    -o /opt/spark/jars/spark-connect_2.12-3.5.6.jar

# 환경 변수 (필요한 경우)
ENV HOME=/tmp
ENV HADOOP_USER_NAME=sparkuser

# 비root 사용자로 돌아감; 공식 이미지 기본은 spark 유저
USER spark

# 기본 entrypoint/command는 공식 이미지가 제공하는 것 그대로 둠.
# Kubernetes 모드에서 executor가 "executor" 커맨드를 기대하므로
# entrypoint 분기 로직을 유지하려면 공식 이미지의 구조를 그대로 쓰는 게 중요함.