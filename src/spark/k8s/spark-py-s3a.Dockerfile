FROM apache/spark-py:v3.3.1

USER root

RUN apt-get update && \
    apt-get install -y curl && \
    rm -rf /var/lib/apt/lists/*

# ✅ S3A + Iceberg 관련 jar를 /opt/spark/jars 에 추가
RUN mkdir -p /opt/spark/jars-extra && \
    curl -L -o /opt/spark/jars-extra/hadoop-aws-3.3.4.jar \
      https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar && \
    curl -L -o /opt/spark/jars-extra/aws-java-sdk-bundle-1.12.262.jar \
      https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar && \
    curl -L -o /opt/spark/jars-extra/iceberg-spark-runtime-3.3_2.12-1.5.2.jar \
      https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.3_2.12/1.5.2/iceberg-spark-runtime-3.3_2.12-1.5.2.jar && \
    cp /opt/spark/jars-extra/*.jar /opt/spark/jars/

USER 185
