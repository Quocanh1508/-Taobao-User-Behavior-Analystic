import os
from google.cloud import bigquery

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"c:\StudyZone\Project\Taobao\credentials.json"
client = bigquery.Client(project="dwh-midterm-123456")

query = """
SELECT event_time, date_key, event_date, is_buy 
FROM taobao_dwh.Fact_User_Behavior 
WHERE event_time IS NOT NULL
LIMIT 10
"""
results = client.query(query).result()

for row in results:
    print(f"Time: {row.event_time}, DateKey: {row.date_key}, Date: {row.event_date}, Is_Buy: {row.is_buy}")
