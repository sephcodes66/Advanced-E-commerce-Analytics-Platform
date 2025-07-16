{{ config(
    materialized='table',
    tags=['raw', 'amazon', 'sales'],
    meta={
        'owner': 'data_engineering',
        'description': 'Raw Amazon sales data with all original columns preserved',
        'freshness_threshold': '24 hours'
    }
) }}

WITH source_data AS (
    SELECT 
        -- Identifiers
        "Order ID" as order_id,
        "SKU" as sku,
        "ASIN" as asin,
        "Style" as style_code,
        
        -- Temporal data
        TRY_CAST("Date" as DATE) as order_date,
        EXTRACT(YEAR FROM TRY_CAST("Date" as DATE)) as order_year,
        EXTRACT(MONTH FROM TRY_CAST("Date" as DATE)) as order_month,
        EXTRACT(DAY FROM TRY_CAST("Date" as DATE)) as order_day,
        EXTRACT(dow FROM TRY_CAST("Date" as DATE)) as day_of_week,
        
        -- Order details
        "Status" as order_status,
        "Fulfilment" as fulfillment_type,
        "Sales Channel" as sales_channel,
        "ship-service-level" as shipping_service_level,
        "Courier Status" as courier_status,
        
        -- Product details
        "Category" as product_category,
        "Size" as product_size,
        
        -- Financial data
        "currency" as currency_code,
        TRY_CAST("Amount" as DECIMAL(10,2)) as order_amount,
        TRY_CAST("Qty" as INTEGER) as quantity,
        ROUND(TRY_CAST("Amount" as DECIMAL(10,2)) / NULLIF(TRY_CAST("Qty" as INTEGER), 0), 2) as unit_price,
        
        -- Geographic data
        "ship-city" as shipping_city,
        "ship-state" as shipping_state,
        "ship-postal-code" as shipping_postal_code,
        "ship-country" as shipping_country,
        
        -- Business flags
        TRY_CAST("B2B" as BOOLEAN) as is_b2b,
        "fulfilled-by" as fulfilled_by,
        "promotion-ids" as promotion_ids,
        CASE 
            WHEN "promotion-ids" IS NOT NULL AND "promotion-ids" != '' THEN TRUE 
            ELSE FALSE 
        END as has_promotion,
        
        -- Data quality indicators
        CASE 
            WHEN "Order ID" IS NULL THEN 'MISSING_ORDER_ID'
            WHEN "Date" IS NULL THEN 'MISSING_DATE'
            WHEN "Amount" IS NULL THEN 'MISSING_AMOUNT'
            WHEN "Status" IS NULL THEN 'MISSING_STATUS'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Audit fields
        CURRENT_TIMESTAMP as ingested_at,
        '{{ invocation_id }}' as ingestion_run_id,
        
        -- Hash for change detection
        MD5(CONCAT(
            COALESCE("Order ID", ''),
            COALESCE("Date", ''),
            COALESCE(CAST("Amount" as VARCHAR), ''),
            COALESCE("Status", ''),
            COALESCE(CAST("Qty" as VARCHAR), '')
        )) as row_hash
        
    FROM read_csv_auto('../data/raw/Amazon Sale Report.csv', header=true)
    WHERE "Order ID" IS NOT NULL
)

SELECT 
    *,
    -- Business-specific calculations
    CASE 
        WHEN order_status = 'Shipped - Delivered to Buyer' THEN 'DELIVERED'
        WHEN order_status = 'Shipped' THEN 'SHIPPED'
        WHEN order_status = 'Cancelled' THEN 'CANCELLED'
        ELSE 'OTHER'
    END as order_status_category,
    
    CASE 
        WHEN order_amount > {{ var('high_value_order_threshold') }} THEN 'HIGH_VALUE'
        WHEN order_amount > {{ var('high_value_order_threshold') * 0.5 }} THEN 'MEDIUM_VALUE'
        ELSE 'LOW_VALUE'
    END as order_value_segment,
    
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
    END as is_weekend

FROM source_data