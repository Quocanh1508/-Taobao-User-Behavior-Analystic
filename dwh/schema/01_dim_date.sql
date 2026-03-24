CREATE TABLE IF NOT EXISTS `taobao_dwh.Dim_Date` (
    date_key INT64 NOT NULL,
    full_date DATE NOT NULL,
    day_of_week INT64,
    day_name STRING,
    month_num INT64,
    month_name STRING,
    year_num INT64,
    is_weekend BOOLEAN
);
