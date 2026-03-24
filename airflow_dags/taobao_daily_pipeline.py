import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor, GCSObjectsWithPrefixExistenceSensor
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
from airflow.operators.email import EmailOperator
from airflow.utils.trigger_rule import TriggerRule

# ----------------- #
# CONFIG & ALERTS
# ----------------- #
GCS_RAW_BUCKET = Variable.get("gcs_raw_bucket", default_var="taobao-datalake")
GCS_PROCESSED_BUCKET = Variable.get("gcs_processed_bucket", default_var="taobao-datalake")
BQ_PROJECT = Variable.get("bq_project", default_var="dwh-midterm-123456")
BQ_DATASET = Variable.get("bq_dataset", default_var="taobao_dwh")

def task_success_slack_alert(context):
    slack_msg = f"""
    :white_check_mark: Task Succeeded.
    *Task*: {context.get('task_instance').task_id}  
    *Dag*: {context.get('task_instance').dag_id} 
    *Execution Time*: {context.get('execution_date')}  
    """
    alert = SlackWebhookOperator(
        task_id='slack_success',
        slack_webhook_conn_id='slack_default',
        message=slack_msg,
        username='Airflow'
    )
    return alert.execute(context=context)

def task_fail_slack_alert(context):
    slack_msg = f"""
    :x: Task Failed.
    *Task*: {context.get('task_instance').task_id}  
    *Dag*: {context.get('task_instance').dag_id} 
    *Execution Time*: {context.get('execution_date')}  
    *Log Url*: {context.get('task_instance').log_url} 
    """
    alert = SlackWebhookOperator(
        task_id='slack_failed',
        slack_webhook_conn_id='slack_default',
        message=slack_msg,
        username='Airflow'
    )
    return alert.execute(context=context)

default_args = {
    'owner': 'quocanh',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['admin@example.com'],
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_fail_slack_alert,
}

with DAG(
    'taobao_daily_pipeline',
    default_args=default_args,
    description='Taobao daily ETL orchestration',
    schedule_interval='@daily',
    start_date=datetime(2017, 11, 24),
    catchup=False,
    max_active_runs=1,
    tags=['taobao', 'spark', 'bigquery']
) as dag:

    # 1. Wait for Raw Data to land in GCS Daily Partition
    wait_for_raw_data = GCSObjectsWithPrefixExistenceSensor(
        task_id='wait_for_raw_data',
        bucket=GCS_RAW_BUCKET,
        prefix="Raw_Zone/Taobao/ingestion_date={{ ds }}/",
        google_cloud_conn_id='google_cloud_default',
        poke_interval=600,
        timeout=3600 # wait for up to 1 hr
    )

    # 2. PySpark Cleansing
    # Here we map arguments to our existing Phase 2 scripts utilizing {{ ds }} dynamically.
    spark_cleanse = SparkSubmitOperator(
        task_id='spark_cleanse',
        application='/app/spark_jobs/taobao_cleanse.py',
        conn_id='spark_default',
        application_args=[
            '--input-path', f"gs://{GCS_RAW_BUCKET}/Raw_Zone/Taobao/ingestion_date={{{{ ds }}}}/hour=*/*.json",
            '--output-path', f"gs://{GCS_PROCESSED_BUCKET}/Processed_Zone/Taobao/Cleansed/event_date={{{{ ds }}}}/"
        ],
        conf={"spark.sql.parquet.compression.codec": "snappy"}
    )

    # 3. PySpark Transformation & Metrics extraction
    spark_transform_metrics = SparkSubmitOperator(
        task_id='spark_transform_metrics',
        application='/app/spark_jobs/taobao_transform_metrics.py',
        conn_id='spark_default',
        application_args=[
            '--input-path', f"gs://{GCS_PROCESSED_BUCKET}/Processed_Zone/Taobao/Cleansed/event_date={{{{ ds }}}}/",
            '--output-fact', f"gs://{GCS_PROCESSED_BUCKET}/Processed_Zone/Taobao/Fact_Events/",
            '--output-metrics', f"gs://{GCS_PROCESSED_BUCKET}/Processed_Zone/Taobao/Metrics_Daily/"
        ],
        conf={"spark.sql.parquet.compression.codec": "snappy"}
    )

    # 4a. Load Fact Behavior Parquet into BQ 
    bq_load_fact_behavior = GCSToBigQueryOperator(
        task_id='bq_load_fact_behavior',
        bucket=GCS_PROCESSED_BUCKET,
        source_objects=["Processed_Zone/Taobao/Fact_Events/event_date={{ ds }}/part-*"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.Fact_User_Behavior",
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        time_partitioning={'type': 'DAY', 'field': 'event_date'},
        cluster_fields=['user_id', 'item_id'],
        google_cloud_storage_conn_id='google_cloud_default',
        bigquery_conn_id='google_cloud_default'
    )

    # 4b. Load Daily Metrics into BQ
    bq_load_metrics_daily = GCSToBigQueryOperator(
        task_id='bq_load_metrics_daily',
        bucket=GCS_PROCESSED_BUCKET,
        source_objects=["Processed_Zone/Taobao/Metrics_Daily/event_date={{ ds }}/part-*"],
        destination_project_dataset_table=f"{BQ_PROJECT}.{BQ_DATASET}.Fact_Metrics_Daily",
        source_format='PARQUET',
        write_disposition='WRITE_APPEND',
        time_partitioning={'type': 'DAY', 'field': 'event_date'},
        # PySpark specific inference mapping via API options
        parquet_options={"enableListInference": True}, 
        google_cloud_storage_conn_id='google_cloud_default',
        bigquery_conn_id='google_cloud_default'
    )

    # 5. SCD Type 2 MERGE Execution for Dim_Item
    # Read our scd script from the dwh/scd dir via File or Inline SQL string mapping
    with open('/app/dwh/scd/scd_item_type2.sql', 'r') as f:
        scd2_sql = f.read()

    bq_scd2_dim_item = BigQueryInsertJobOperator(
        task_id='bq_scd2_dim_item',
        configuration={
            "query": {
                "query": scd2_sql,
                "useLegacySql": False,
                "queryParameters": [
                    {
                        "name": "run_date",
                        "parameterType": {"type": "DATE"},
                        "parameterValue": {"value": "{{ ds }}"}
                    }
                ]
            }
        },
        gcp_conn_id='google_cloud_default'
    )

    # 6. Success Alert Notification
    notify_success = SlackWebhookOperator(
        task_id='notify_success',
        slack_webhook_conn_id='slack_default',
        message="✅ Successfully processed Taobao ETL pipeline for {{ ds }}!",
        trigger_rule=TriggerRule.ALL_SUCCESS
    )

    # Workflow DAG Dependency Matrix
    wait_for_raw_data >> spark_cleanse >> spark_transform_metrics
    spark_transform_metrics >> [bq_load_fact_behavior, bq_load_metrics_daily]
    [bq_load_fact_behavior, bq_load_metrics_daily] >> bq_scd2_dim_item >> notify_success
