import os
from google.cloud import storage

def list_and_upload(project_id, creds_path, local_mock_dir):
    os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = creds_path
    client = storage.Client(project=project_id)
    
    # Check for existing buckets
    buckets = list(client.list_buckets())
    bucket_name = None
    
    if buckets:
        # Use their existing bucket
        bucket_name = buckets[0].name
        print(f"Found existing bucket: {bucket_name}")
    else:
        # Create one if none exists
        bucket_name = f"taobao-datalake-{project_id}"
        print(f"No bucket found. Creating bucket: {bucket_name}")
        bucket = client.bucket(bucket_name)
        bucket.location = "US"
        client.create_bucket(bucket)
        
    bucket = client.bucket(bucket_name)
    
    # Walk the local gs_mock_bucket directory and upload
    print(f"Uploading files to gs://{bucket_name}/...")
    upload_count = 0
    
    for root, dirs, files in os.walk(local_mock_dir):
        for file in files:
            local_path = os.path.join(root, file)
            # Determine the relative path to use as Blob name (e.g. Raw_Zone/Taobao/...)
            blob_name = os.path.relpath(local_path, local_mock_dir).replace('\\', '/')
            
            blob = bucket.blob(blob_name)
            blob.upload_from_filename(local_path)
            upload_count += 1
            if upload_count % 10 == 0:
                print(f"Uploaded {upload_count} files...")
                
    print(f"Successfully uploaded all {upload_count} mock data files to the real GCS bucket!")

if __name__ == "__main__":
    project_id = "dwh-midterm-123456"
    creds_path = r"c:\StudyZone\Project\Taobao\credentials.json"
    local_mock_dir = r"c:\StudyZone\Project\Taobao\data_replayer\gs_mock_bucket"
    
    list_and_upload(project_id, creds_path, local_mock_dir)
