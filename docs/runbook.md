# Taobao Data Warehouse - Operations Runbook

This runbook covers operational procedures for the Taobao Pipeline built on Apache Airflow.

## 1. How to rerun a day
If a specific day failed or needs recalculation (e.g. data corrections):
1. **Clear the DAG run** in Airflow:
   - Navigate to the **Grid view** in the Airflow UI for `taobao_daily_pipeline`.
   - Click on the execution date square (column) you wish to re-run.
   - Click the **"Clear"** button to reset task states to null.
   - Airflow will automatically reschedule the tasks in dependency order.

## 2. How to backfill historical ranges
To process weeks or months of existing historical data linearly (if `catchup=False` inherently):
1. **Using Airflow CLI**:
   - `docker exec -it airflow-scheduler bash`
   - `airflow dags backfill taobao_daily_pipeline -s 2017-11-25 -e 2017-12-03`
2. This will orchestrate DAG executions for every logical day (`ds`) between your start and end bounds, concurrently honoring `max_active_runs`.

## 3. How to fix failed loads
If a specific `bq_load_*` or `spark_*` task crashes:
1. **Identify the Failure Source**:
   - Check the **Slack alerts** which provides immediate hyperlinks to the failed Airflow log.
   - Look at the stacktrace in Airflow Log viewer.
2. If it's a **Spark issue**:
   - For detailed Spark JVM logs (OOM errors, mapping failures), access the logs mounted at the root or within GCP Dataproc (if configured globally).
3. If it's a **BigQuery load issue (Schema Mismatch)**:
   - Confirm your dataset formats by running `bq show <Dataset>` or investigating `LoadJobConfig` in PySpark.
   - If Parquet structs differ due to PySpark, enforce `enableListInference=True` globally.
4. If it's a **Data/Network missing** (GCS Sink delay):
   - The pipeline features explicit `retry_delay=5` minutes configurations. Wait for the transient network error to clear. 
5. Once fixed, simply click **"Clear"** exclusively on the failed task directly from the UI, choosing "Downstream". Airflow resumes the pipeline without reprocessing earlier successful Spark nodes.
