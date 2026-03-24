CREATE TABLE IF NOT EXISTS `taobao_dwh.Fact_User_Behavior` (
    behavior_key STRING,
    user_key STRING,
    item_key STRING,
    date_key INT64,
    
    user_id INT64, 
    item_id INT64, 
    category_id INT64,

    behavior_type STRING,
    event_time TIMESTAMP,
    event_date DATE,
    session_id STRING,
    is_buy BOOLEAN
)
PARTITION BY event_date
CLUSTER BY user_id, item_id;
