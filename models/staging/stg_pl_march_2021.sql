SELECT
    TRIM(LOWER(Sku)) AS product_sku,
    TRIM(LOWER(Category)) AS product_category,
    TRY_CAST("TP 1" AS DECIMAL(10, 2)) AS cost_price_tp1,
    TRY_CAST("TP 2" AS DECIMAL(10, 2)) AS cost_price_tp2,
    TRY_CAST("Final MRP Old" AS DECIMAL(10, 2)) AS final_mrp_old,
    'pl_march_2021' AS _source_file_name
FROM read_csv_auto('/Users/shabhrishreddyuddehal/Downloads/dbt/data/raw/P  L March 2021.csv')
WHERE Sku IS NOT NULL