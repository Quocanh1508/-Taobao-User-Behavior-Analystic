import argparse
import yaml
import os
import time
import json
import pandas as pd
from datetime import datetime, timezone

def parse_args():
    parser = argparse.ArgumentParser(description='Simulate streaming of Taobao dataset.')
    parser.add_argument('--speed-factor', type=float, default=1000.0, help='Playback speed multiplier (e.g. 1000 means 1000x faster than real-time).')
    parser.add_argument('--target-bucket', type=str, default='local_gcs_bucket', help='Target bucket or local directory path for the Raw Zone.')
    parser.add_argument('--start-date', type=str, default='', help='ISO format start date to filter data (e.g., 2017-11-25).')
    parser.add_argument('--max-rows', type=int, default=0, help='Maximum number of rows to process (0 = infinite).')
    return parser.parse_args()

def main():
    args = parse_args()
    
    # Load config file
    config_path = os.path.join(os.path.dirname(__file__), 'config.yaml')
    with open(config_path, 'r') as f:
        config = yaml.safe_load(f)
        
    source_file = config.get('source_data', '../data/raw/UserBehavior.csv.zip')
    
    # Resolve relative path considering script location
    if not os.path.isabs(source_file):
        source_file = os.path.abspath(os.path.join(os.path.dirname(__file__), source_file))
        
    batch_size_mb = config.get('batch_size_mb', 50)
    target_bucket = args.target_bucket
    
    # Target batch size in bytes
    batch_size_bytes = batch_size_mb * 1024 * 1024
    
    start_ts = 0
    if args.start_date:
        start_ts = int(datetime.fromisoformat(args.start_date).timestamp())
        
    print(f"Starting replay from: {source_file}")
    print(f"Target Bucket: {target_bucket}")
    print(f"Speed Factor: {args.speed_factor}x")
    print(f"Batch Size: {batch_size_mb} MB")
    
    columns = ['user_id', 'item_id', 'category_id', 'behavior_type', 'event_ts']
    chunksize = 200000 
    
    buffer = []
    current_buffer_size = 0
    rows_processed = 0
    
    last_event_ts = None
    accumulated_sim_time = 0.0
    
    # Read CSV in chunks
    for chunk in pd.read_csv(source_file, header=None, names=columns, chunksize=chunksize, compression='zip'):
        # Filter by start date if provided
        if start_ts > 0:
            chunk = chunk[chunk['event_ts'] >= start_ts]
            if chunk.empty:
                continue
                
        # Fast row iteration
        records = chunk.to_dict('records')
        
        for row in records:
            if args.max_rows > 0 and rows_processed >= args.max_rows:
                break
                
            event_ts = int(row['event_ts'])
            
            # Batch sleep logic to simulate streaming speed
            if last_event_ts is not None and event_ts > last_event_ts:
                time_diff = event_ts - last_event_ts
                if time_diff < 86400 * 30: # ignore massive time jumps (e.g. bad data)
                    accumulated_sim_time += time_diff
                
                # Sleep if accumulated time corresponds to more than 0.1 real seconds
                real_sleep = accumulated_sim_time / args.speed_factor
                if real_sleep >= 0.1:
                    time.sleep(real_sleep)
                    accumulated_sim_time = 0.0
                    
            last_event_ts = event_ts
            
            # Format to JSON strings
            now_iso = datetime.now(timezone.utc).isoformat()
            record = {
                'user_id': int(row['user_id']),
                'item_id': int(row['item_id']),
                'category_id': int(row['category_id']),
                'behavior_type': row['behavior_type'],
                'event_ts': event_ts,
                'ingested_at': now_iso
            }
            
            json_str = json.dumps(record, separators=(',', ':')) + '\n'
            
            # Measure strictly ASCII string length as byte length for efficiency
            record_size = len(json_str)
            
            buffer.append(json_str)
            current_buffer_size += record_size
            rows_processed += 1
            
            # Flush batch
            if current_buffer_size >= batch_size_bytes:
                flush_buffer(buffer, target_bucket, event_ts)
                buffer = []
                current_buffer_size = 0
                
        if args.max_rows > 0 and rows_processed >= args.max_rows:
            print("Max rows reached. Stopping.")
            break

    # Final flush
    if buffer:
        flush_buffer(buffer, target_bucket, last_event_ts or int(time.time()))
        
    print(f"Replay finished. Total rows processed: {rows_processed}")

def flush_buffer(buffer, bucket, ts):
    dt = datetime.fromtimestamp(ts, timezone.utc)
    # Target partition: Raw_Zone/Taobao/ingestion_date=YYYY-MM-DD/hour=HH/
    date_str = dt.strftime('%Y-%m-%d')
    hour_str = dt.strftime('%H')
    
    partition_path = os.path.join(bucket, 'Raw_Zone', 'Taobao', f'ingestion_date={date_str}', f'hour={hour_str}')
    os.makedirs(partition_path, exist_ok=True)
    
    file_name = f"batch_{int(time.time() * 1000)}.json"
    file_path = os.path.join(partition_path, file_name)
    
    with open(file_path, 'w', encoding='utf-8') as f:
        f.writelines(buffer)
        
    size_mb = os.path.getsize(file_path) / (1024 * 1024)
    print(f"Flushed {len(buffer)} records to {file_path} ({size_mb:.2f} MB)")

if __name__ == '__main__':
    main()
