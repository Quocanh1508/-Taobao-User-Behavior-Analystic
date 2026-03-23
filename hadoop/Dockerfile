FROM eclipse-temurin:17-jdk

ENV HADOOP_HOME=/opt/hadoop
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$HADOOP_HOME/bin:$HADOOP_HOME/sbin:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Install dependencies
RUN apt-get update && apt-get install -y python3 python3-pip curl jq vim

# Copy the local offline Hadoop/Spark tar files
COPY hadoop-3.4.3.tar.gz /tmp/
COPY spark-4.1.1-bin-hadoop3.tgz /tmp/

# Extract the archives to /opt
RUN tar -xzf /tmp/hadoop-3.4.3.tar.gz -C /opt/ && \
    mv /opt/hadoop-3.4.3 $HADOOP_HOME && \
    rm /tmp/hadoop-3.4.3.tar.gz && \
    tar -xzf /tmp/spark-4.1.1-bin-hadoop3.tgz -C /opt/ && \
    mv /opt/spark-4.1.1-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark-4.1.1-bin-hadoop3.tgz

WORKDIR /app

# Ensure we have required pip packages
RUN pip3 install pandas pyspark --break-system-packages
