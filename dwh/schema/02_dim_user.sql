CREATE TABLE IF NOT EXISTS `taobao_dwh.Dim_User` (
    user_key STRING NOT NULL,
    user_id INT64 NOT NULL,
    first_activity_date DATE,
    user_segment STRING
);
