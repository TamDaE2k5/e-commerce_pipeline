FROM apache/airflow:2.10.2-python3.8

# Cài đặt các package hệ thống cần thiết và làm sạch cache ngay trong cùng một layer
USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        openjdk-17-jre-headless \
        curl \
        ca-certificates \
        tar \
        procps \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Thiết lập các biến môi trường cho Spark và Java
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# Tải và cài đặt Spark Client
# nếu có file rồi thì ko cần kéo về
COPY ./package/spark-3.5.0-bin-hadoop3.tgz /tmp/spark.tgz
RUN mkdir -p $SPARK_HOME \
    && tar -xzf /tmp/spark.tgz -C $SPARK_HOME --strip-components=1 \
    && rm /tmp/spark.tgz
# nếu chưa thì chạy RUN này
# RUN curl -fL \
#     --retry 5 \
#     --retry-delay 10 \
#     --retry-connrefused \
#     -o /tmp/spark.tgz \
#     https://mirrors.huaweicloud.com/apache/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz \
#     && mkdir -p $SPARK_HOME \
#     && tar -xzf /tmp/spark.tgz -C $SPARK_HOME --strip-components=1 \
#     && rm /tmp/spark.tgz

COPY ./package/*.jar /opt/spark/jars/

# RUN curl -fL \
#       https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar \
#       -o $SPARK_HOME/jars/hadoop-aws-3.3.4.jar \
#  && curl -fL \
#       https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar \
#       -o $SPARK_HOME/jars/aws-java-sdk-bundle-1.12.262.jar

# Đảm bảo user airflow có thể thực thi lệnh spark-submit
RUN chmod -R 777 $SPARK_HOME

# Cập nhật PATH để hệ thống nhận diện được lệnh spark-submit
ENV PATH="${SPARK_HOME}/bin:${PATH}"

# Chuyển sang user airflow và cài đặt requirements
USER airflow
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt \
 && rm -f requirements.txt

