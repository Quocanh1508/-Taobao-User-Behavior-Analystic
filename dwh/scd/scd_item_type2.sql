-- 1. Create a staging table with distinct item/categories from today's load
CREATE TEMP TABLE stg_item AS
SELECT DISTINCT 
    item_id, 
    category_id, 
    event_date AS as_of_date
FROM `taobao_dwh.Fact_User_Behavior` 
WHERE event_date = @run_date;

-- 2. Execute BQ MERGE for SCD Type 2 logic
MERGE `taobao_dwh.Dim_Item` T
USING (
    -- The rows we need to UPDATE (expire old records)
    SELECT
      stg.item_id,
      target.category_id, -- old category
      stg.as_of_date,
      TRUE as is_expire_action
    FROM stg_item stg
    JOIN `taobao_dwh.Dim_Item` target 
      ON stg.item_id = target.item_id
      AND target.is_current = TRUE
      AND stg.category_id != target.category_id
      
    UNION ALL
    
    -- The rows we need to INSERT (new items or new versions of updated items)
    SELECT 
      stg.item_id,
      stg.category_id, -- new category
      stg.as_of_date,
      FALSE as is_expire_action
    FROM stg_item stg
    WHERE NOT EXISTS (
      SELECT 1 FROM `taobao_dwh.Dim_Item` existing
      WHERE existing.item_id = stg.item_id
        AND existing.category_id = stg.category_id
        AND existing.is_current = TRUE
    )
) S
ON T.item_id = S.item_id 
   AND T.category_id = S.category_id 
   AND T.is_current = TRUE
WHEN MATCHED AND S.is_expire_action = TRUE THEN
  UPDATE SET effective_to = S.as_of_date, is_current = FALSE
WHEN NOT MATCHED BY TARGET THEN
  INSERT (item_key, item_id, category_id, effective_from, effective_to, is_current)
  VALUES (GENERATE_UUID(), S.item_id, S.category_id, S.as_of_date, NULL, TRUE);
