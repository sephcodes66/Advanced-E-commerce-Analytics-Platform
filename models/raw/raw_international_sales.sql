{{ config(
    materialized='table',
    tags=['raw', 'international', 'sales'],
    meta={
        'owner': 'data_engineering',
        'description': 'Raw international sales data with enhanced business logic',
        'freshness_threshold': '24 hours'
    }
) }}

WITH source_data AS (
    SELECT 
        -- Product and customer identifiers
        "SKU" as sku,
        "Style" as style_code,
        "CUSTOMER" as customer_name,
        
        -- Order date and time information
        TRY_CAST("DATE" as DATE) as order_date,
        "Months" as order_month_text,
        EXTRACT(YEAR FROM TRY_CAST("DATE" as DATE)) as order_year,
        EXTRACT(MONTH FROM TRY_CAST("DATE" as DATE)) as order_month,
        EXTRACT(DAY FROM TRY_CAST("DATE" as DATE)) as order_day,
        EXTRACT(dow FROM TRY_CAST("DATE" as DATE)) as day_of_week,
        
        -- Product size information
        "Size" as product_size,
        
        -- Order financial information
        TRY_CAST("PCS" as INTEGER) as quantity,
        TRY_CAST("RATE" as DECIMAL(10,2)) as unit_rate,
        TRY_CAST("GROSS AMT" as DECIMAL(10,2)) as gross_amount,
        
        -- Calculated unit price
        ROUND(TRY_CAST("GROSS AMT" as DECIMAL(10,2)) / NULLIF(TRY_CAST("PCS" as INTEGER), 0), 2) as calculated_unit_price,
        
        -- Data quality validation flags
        CASE 
            WHEN "SKU" IS NULL THEN 'MISSING_SKU'
            WHEN "DATE" IS NULL THEN 'MISSING_DATE'
            WHEN "GROSS AMT" IS NULL THEN 'MISSING_AMOUNT'
            WHEN "CUSTOMER" IS NULL THEN 'MISSING_CUSTOMER'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Timestamps and IDs for auditing
        CURRENT_TIMESTAMP as ingested_at,
        '{{ invocation_id }}' as ingestion_run_id,
        
        -- MD5 hash for detecting row changes
        MD5(CONCAT(
            COALESCE("SKU", ''),
            COALESCE("DATE", ''),
            COALESCE("GROSS AMT", ''),
            COALESCE("CUSTOMER", ''),
            COALESCE("PCS", '')
        )) as row_hash
        
    FROM read_csv_auto('../data/raw/International sale Report.csv', header=true)
    WHERE "SKU" IS NOT NULL
)

SELECT 
    *,
    -- Custom business logic
    CASE 
        WHEN gross_amount > 1000 THEN 'HIGH_VALUE'
        WHEN gross_amount > 500 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END as order_value_segment,
    
    -- Customer category based on name
    CASE 
        WHEN customer_name LIKE '%LOGANATHAN%' THEN 'PREMIUM_CUSTOMER'
        WHEN customer_name LIKE '%RETAIL%' THEN 'RETAIL_CUSTOMER'
        ELSE 'STANDARD_CUSTOMER'
    END as customer_segment,
    
    -- Product category derived from SKU
    CASE 
        WHEN sku LIKE 'MEN%' THEN 'MENS_WEAR'
        WHEN sku LIKE 'WOMEN%' THEN 'WOMENS_WEAR'
        WHEN sku LIKE 'KIDS%' THEN 'KIDS_WEAR'
        ELSE 'UNISEX'
    END as product_category,
    
    -- Standardized product size
    CASE 
        WHEN product_size = 'S' THEN 'SMALL'
        WHEN product_size = 'M' THEN 'MEDIUM'
        WHEN product_size = 'L' THEN 'LARGE'
        WHEN product_size = 'XL' THEN 'EXTRA_LARGE'
        WHEN product_size = 'XXL' THEN 'DOUBLE_EXTRA_LARGE'
        ELSE 'OTHER'
    END as standardized_size,
    
    -- Seasonal information for analysis
    CASE 
        WHEN order_month IN (11, 12, 1) THEN 'WINTER'
        WHEN order_month IN (2, 3, 4) THEN 'SPRING'
        WHEN order_month IN (5, 6, 7) THEN 'SUMMER'
        WHEN order_month IN (8, 9, 10) THEN 'FALL'
    END as season,
    
    -- Indicates if the order was placed on a weekend
    CASE 
        WHEN day_of_week IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END as is_weekend,
    
    -- Profitability status of the order
    CASE 
        WHEN unit_rate > calculated_unit_price THEN 'PROFITABLE'
        WHEN unit_rate = calculated_unit_price THEN 'BREAKEVEN'
        ELSE 'LOSS'
    END as profitability_flag

FROM source_data