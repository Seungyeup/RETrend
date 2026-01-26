# Dockerfile.spark-connect (patched)
FROM apache/spark:3.5.6

USER root

# 1) 필요한 툴 + curl 포함
RUN apt-get update && \
    apt-get install -y python3 python3-pip curl netcat-openbsd procps && \
    rm -rf /var/lib/apt/lists/*

# 2) (선택) pyspark[connect] — 서버엔 필수 아님.
#    클라이언트 겸용이면 유지, 서버 전용이면 제거 가능
RUN pip3 install --no-cache-dir pyspark[connect]==3.5.6

# 3) Spark Connect Server JAR
RUN curl -sfL \
  https://repo1.maven.org/maven2/org/apache/spark/spark-connect_2.12/3.5.6/spark-connect_2.12-3.5.6.jar \
  -o /opt/spark/jars/spark-connect_2.12-3.5.6.jar

# 4) hadoop-aws (버전/파일명 일치시킴: 3.3.4 -> 3.3.4)
RUN curl -sfL \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
  -o /opt/spark/jars/hadoop-aws-3.3.4.jar

# 5) aws-java-sdk-bundle (버전 OK)
RUN curl -sfL \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.747/aws-java-sdk-bundle-1.12.747.jar \
  -o /opt/spark/jars/aws-java-sdk-bundle-1.12.747.jar

ENV HOME=/tmp
ENV HADOOP_USER_NAME=sparkuser

USER spark
# entrypoint/cmd — 기본값 유지 (중요)