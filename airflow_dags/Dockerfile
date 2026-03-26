FROM apache/airflow:2.7.3-python3.10

USER root
RUN apt-get update && apt-get install -y procps && apt-get clean

USER airflow
# Install spark provider WITHOUT its pyspark dependency (we use external Spark cluster)
RUN pip install --no-cache-dir --no-deps "apache-airflow-providers-apache-spark==4.3.0"
# Install google and slack providers
RUN pip install --no-cache-dir \
    "apache-airflow-providers-google==10.11.0" \
    "apache-airflow-providers-slack==8.7.0"
