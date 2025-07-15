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
        -- Identifiers
        "SKU" as sku,
        "Style" as style_code,
        "CUSTOMER" as customer_name,
        
        -- Temporal data
        TRY_CAST("DATE" as DATE) as order_date,
        "Months" as order_month_text,
        EXTRACT(YEAR FROM TRY_CAST("DATE" as DATE)) as order_year,
        EXTRACT(MONTH FROM TRY_CAST("DATE" as DATE)) as order_month,
        EXTRACT(DAY FROM TRY_CAST("DATE" as DATE)) as order_day,
        EXTRACT(dow FROM TRY_CAST("DATE" as DATE)) as day_of_week,
        
        -- Product details
        "Size" as product_size,
        
        -- Financial data
        TRY_CAST("PCS" as INTEGER) as quantity,
        TRY_CAST("RATE" as DECIMAL(10,2)) as unit_rate,
        TRY_CAST("GROSS AMT" as DECIMAL(10,2)) as gross_amount,
        
        -- Calculated fields
        ROUND(TRY_CAST("GROSS AMT" as DECIMAL(10,2)) / NULLIF(TRY_CAST("PCS" as INTEGER), 0), 2) as calculated_unit_price,
        
        -- Data quality indicators
        CASE 
            WHEN "SKU" IS NULL THEN 'MISSING_SKU'
            WHEN "DATE" IS NULL THEN 'MISSING_DATE'
            WHEN "GROSS AMT" IS NULL THEN 'MISSING_AMOUNT'
            WHEN "CUSTOMER" IS NULL THEN 'MISSING_CUSTOMER'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Audit fields
        CURRENT_TIMESTAMP as ingested_at,
        '{{ invocation_id }}' as ingestion_run_id,
        
        -- Hash for change detection
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
    -- Business-specific enhancements
    CASE 
        WHEN gross_amount > 1000 THEN 'HIGH_VALUE'
        WHEN gross_amount > 500 THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END as order_value_segment,
    
    -- Customer segmentation
    CASE 
        WHEN customer_name LIKE '%LOGANATHAN%' THEN 'PREMIUM_CUSTOMER'
        WHEN customer_name LIKE '%RETAIL%' THEN 'RETAIL_CUSTOMER'
        ELSE 'STANDARD_CUSTOMER'
    END as customer_segment,
    
    -- Product category extraction from SKU
    CASE 
        WHEN sku LIKE 'MEN%' THEN 'MENS_WEAR'
        WHEN sku LIKE 'WOMEN%' THEN 'WOMENS_WEAR'
        WHEN sku LIKE 'KIDS%' THEN 'KIDS_WEAR'
        ELSE 'UNISEX'
    END as product_category,
    
    -- Size standardization
    CASE 
        WHEN product_size = 'S' THEN 'SMALL'
        WHEN product_size = 'M' THEN 'MEDIUM'
        WHEN product_size = 'L' THEN 'LARGE'
        WHEN product_size = 'XL' THEN 'EXTRA_LARGE'
        WHEN product_size = 'XXL' THEN 'DOUBLE_EXTRA_LARGE'
        ELSE 'OTHER'
    END as standardized_size,
    
    -- Seasonality flags
    CASE 
        WHEN order_month IN (11, 12, 1) THEN 'WINTER'
        WHEN order_month IN (2, 3, 4) THEN 'SPRING'
        WHEN order_month IN (5, 6, 7) THEN 'SUMMER'
        WHEN order_month IN (8, 9, 10) THEN 'FALL'
    END as season,
    
    -- Weekend flag
    CASE 
        WHEN day_of_week IN (0, 6) THEN TRUE 
        ELSE FALSE 
    END as is_weekend,
    
    -- Margin analysis
    CASE 
        WHEN unit_rate > calculated_unit_price THEN 'PROFITABLE'
        WHEN unit_rate = calculated_unit_price THEN 'BREAKEVEN'
        ELSE 'LOSS'
    END as profitability_flag

FROM source_data