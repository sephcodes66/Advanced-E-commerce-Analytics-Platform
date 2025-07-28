SELECT
    MD5(CAST(SKU AS VARCHAR) || CAST(Date AS VARCHAR)) AS order_item_id,
    MD5(CAST(SKU AS VARCHAR) || CAST(Date AS VARCHAR)) AS order_id,
    try_strptime(CAST(Date AS VARCHAR), '%m-%d-%y') AS order_date,
    NULL AS order_status,
    NULL AS fulfilment_method,
    NULL AS product_category,
    TRIM(LOWER(SKU)) AS product_sku,
    NULL AS product_style,
    NULL AS product_asin,
    CAST(Pcs AS INTEGER) AS quantity,
    CAST("GROSS AMT" AS DECIMAL(10, 2)) AS amount,
    NULL AS currency,
    FALSE AS is_b2b_sale,
    'international_sale_report' AS _source_file_name
FROM read_csv_auto('/Users/shabhrishreddyuddehal/Downloads/dbt/data/raw/International sale Report.csv', header=true)
WHERE SKU IS NOT NULL AND try_cast("GROSS AMT" as decimal) is not null