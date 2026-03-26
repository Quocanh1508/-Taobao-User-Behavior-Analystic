import argparse
from google.cloud import bigquery

def load_parquet_to_bq(client, source_uri, table_id, partition_field=None, cluster_fields=None):
    print(f"Loading '{source_uri}' into {table_id}...")
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
        
        hive_opts = bigquery.HivePartitioningOptions()
        hive_opts.mode = "AUTO"
        hive_opts.source_uri_prefix = source_uri.split(f'{partition_field}=')[0]
        job_config.hive_partitioning = hive_opts
    if cluster_fields:
        job_config.clustering_fields = cluster_fields

    load_job = client.load_table_from_uri(
        source_uri, table_id, job_config=job_config
    )
    
    load_job.result()  # Waits for the job to complete
    
    table = client.get_table(table_id)
    print(f"Loaded {table.num_rows} rows into {table_id}.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--ds", required=True, help="Execution date partition YYYY-MM-DD")
    parser.add_argument("--bucket", required=True, help="GCS Bucket containing your Processed Zone (e.g. gs://taobao-datalake)")
    parser.add_argument("--project-id", required=True, help="GCP Project ID")
    args = parser.parse_args()

    client = bigquery.Client(project=args.project_id)
    
    # Constructing URIs
    fact_uri = f"{args.bucket}/Processed_Zone/Taobao/Fact_Events/event_date={args.ds}/part-*"
    metrics_uri = f"{args.bucket}/Processed_Zone/Taobao/Metrics_Daily/event_date={args.ds}/part-*"
    
    dataset_ref = f"{args.project_id}.taobao_dwh"
    
    # Execute Load Jobs
    load_parquet_to_bq(client, fact_uri, f"{dataset_ref}.Fact_User_Behavior", partition_field="event_date", cluster_fields=["user_id", "item_id"])
    load_parquet_to_bq(client, metrics_uri, f"{dataset_ref}.Fact_Metrics_Daily", partition_field="event_date")
