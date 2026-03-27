FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && \
    apt-get install -y --no-install-recommends openjdk-11-jdk-headless procps curl && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark binaries (Client only) for SparkSubmitOperator
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH="${SPARK_HOME}/bin:${PATH}"

RUN curl -L https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz | tar -xz -C /opt && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    chown -R airflow: /opt/spark

USER airflow
# Pin versions compatible with Airflow 2.7.x
RUN pip install --no-cache-dir \
    "apache-airflow-providers-apache-spark==4.3.0" \
    "apache-airflow-providers-google==10.11.0" \
    "apache-airflow-providers-slack==8.7.0"
