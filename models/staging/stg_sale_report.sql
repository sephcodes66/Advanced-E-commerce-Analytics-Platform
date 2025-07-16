SELECT
    MD5(CAST("SKU Code" AS VARCHAR) || CAST("Design No." AS VARCHAR)) AS order_item_id,
    CAST("SKU Code" AS VARCHAR) AS order_id, -- Assuming SKU Code can act as order_id for this dataset
    NULL AS order_date, -- Date column not explicitly mentioned, will be null for now
    NULL AS order_status, -- Status column not explicitly mentioned, will be null for now
    NULL AS fulfilment_method, -- Fulfilment method not explicitly mentioned, will be null for now
    TRIM(LOWER(Category)) AS product_category,
    NULL AS product_style, -- Style not explicitly mentioned, will be null for now
    TRIM(LOWER("SKU Code")) AS product_sku,
    TRIM(LOWER("Design No.")) AS product_asin, -- Assuming Design No. can act as ASIN
    CAST(Stock AS INTEGER) AS quantity, -- Assuming Stock represents quantity
    NULL AS amount, -- Amount not explicitly mentioned, will be null for now
    NULL AS currency, -- Currency not explicitly mentioned, will be null for now
    FALSE AS is_b2b_sale, -- Assuming not a B2B sale by default
    'sale_report' AS _source_file_name
FROM read_csv_auto('/Users/shabhrishreddyuddehal/Downloads/dbt/data/raw/Sale Report.csv', header=true)
WHERE "SKU Code" IS NOT NULL