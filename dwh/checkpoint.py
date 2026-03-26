import os
import glob
from google.cloud import bigquery

def initialize_dwh(project_id, credentials_path):
    print(f"Initializing DWH for project: {project_id}")
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    client = bigquery.Client(project=project_id)
    
    # 1. Create Dataset
    dataset_id = f"{project_id}.taobao_dwh"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    
    try:
        dataset = client.create_dataset(dataset, timeout=30)
        print(f"Created dataset {dataset_id}")
    except Exception as e:
        print(f"Dataset already exists or error: {e}")

    # 2. Execute all Schema DDL files
    schema_dir = os.path.join(os.path.dirname(__file__), 'schema')
    for sql_file in sorted(glob.glob(os.path.join(schema_dir, '*.sql'))):
        print(f"Executing {os.path.basename(sql_file)}...")
        with open(sql_file, 'r') as f:
            query = f.read()
            query_job = client.query(query)
            query_job.result()  # Wait for the job to complete
            print(f"-> Successfully executed {os.path.basename(sql_file)}")

def load_local_parquet_to_bq(project_id, local_pattern, table_id, partition_field=None, cluster_fields=None):
    client = bigquery.Client(project=project_id)
    
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND
    )
    
    parquet_options = bigquery.format_options.ParquetOptions()
    parquet_options.enable_list_inference = True
    job_config.parquet_options = parquet_options
    
    if partition_field:
        job_config.time_partitioning = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY,
            field=partition_field
        )
        
        # Ensure BQ doesn't null out columns stripped by PySpark Partitioning folders!
        hive_opts = bigquery.HivePartitioningOptions()
        hive_opts.mode = "AUTO"
        hive_opts.source_uri_prefix = local_pattern.split(f'{partition_field}=')[0]
        job_config.hive_partitioning = hive_opts
        
    if cluster_fields:
        job_config.clustering_fields = cluster_fields
        
        
    files = glob.glob(local_pattern)
    if not files:
        print(f"No files found matching {local_pattern}")
        return
        
    for filepath in files:
        print(f"Loading '{filepath}' into {table_id}...")
        with open(filepath, "rb") as source_file:
            load_job = client.load_table_from_file(
                source_file, table_id, job_config=job_config
            )
            load_job.result()  # Waits for the job to complete
    
    table = client.get_table(table_id)
    print(f"Loaded a total of {table.num_rows} rows into {table_id}.")

if __name__ == "__main__":
    project_id = "dwh-midterm-123456"
    credentials_path = r"c:\StudyZone\Project\Taobao\credentials.json"
    
    # 1. Initialize DWH
    initialize_dwh(project_id, credentials_path)
    
    # 2. Perform mock checkpoint load simulating load_jobs.py
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = credentials_path
    
    mock_base = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data_replayer", "gs_mock_bucket", "Processed_Zone", "Taobao"))
    ds = "2017-11-29"
    
    fact_pattern = os.path.join(mock_base, "Fact_Events", f"event_date={ds}", "part-*.parquet")
    metrics_pattern = os.path.join(mock_base, "Metrics_Daily", f"event_date={ds}", "part-*.parquet")
    
    load_local_parquet_to_bq(project_id, fact_pattern, f"{project_id}.taobao_dwh.Fact_User_Behavior", "event_date", ["user_id", "item_id"])
    load_local_parquet_to_bq(project_id, metrics_pattern, f"{project_id}.taobao_dwh.Fact_Metrics_Daily", "event_date")
