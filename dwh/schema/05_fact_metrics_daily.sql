CREATE TABLE IF NOT EXISTS `taobao_dwh.Fact_Metrics_Daily` (
    event_date DATE,
    dau INT64,
    avg_pv_to_cart_sec FLOAT64,
    avg_cart_to_buy_sec FLOAT64,
    avg_pv_to_buy_sec FLOAT64,
    top_10_categories_bought ARRAY<INT64>
)
PARTITION BY event_date;
