CREATE TABLE IF NOT EXISTS `taobao_dwh.Dim_Item` (
    item_key STRING NOT NULL,
    item_id INT64 NOT NULL,
    category_id INT64,
    effective_from DATE,
    effective_to DATE,
    is_current BOOLEAN
);
