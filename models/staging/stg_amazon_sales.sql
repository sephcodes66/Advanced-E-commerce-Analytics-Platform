{{ config(
    materialized='view',
    tags=['staging', 'amazon', 'sales'],
    meta={
        'owner': 'data_engineering',
        'description': 'Standardized Amazon sales data with business rules applied'
    }
) }}

WITH cleansed_data AS (
    SELECT 
        -- Primary keys and identifiers
        order_id,
        sku,
        asin,
        style_code,
        
        -- Temporal dimensions
        order_date,
        order_year,
        order_month,
        order_day,
        day_of_week,
        is_weekend,
        season,
        
        -- Order attributes
        order_status,
        order_status_category,
        fulfillment_type,
        sales_channel,
        shipping_service_level,
        courier_status,
        
        -- Product attributes
        product_category,
        product_size,
        
        -- Financial metrics
        currency_code,
        order_amount,
        quantity,
        unit_price,
        order_value_segment,
        
        -- Geographic information
        shipping_city,
        shipping_state,
        shipping_postal_code,
        shipping_country,
        
        -- Business flags
        is_b2b,
        fulfilled_by,
        has_promotion,
        promotion_ids,
        
        -- Data quality
        data_quality_flag,
        ingested_at,
        row_hash
        
    FROM {{ ref('raw_amazon_sales') }}
    WHERE data_quality_flag = 'VALID'
),

enhanced_metrics AS (
    SELECT 
        *,
        -- Advanced financial calculations
        ROUND(order_amount * 0.18, 2) as estimated_gst_amount,
        ROUND(order_amount * 0.82, 2) as net_order_amount,
        
        -- Product performance scoring
        CASE 
            WHEN order_status_category = 'DELIVERED' AND order_amount > 500 THEN 100
            WHEN order_status_category = 'DELIVERED' AND order_amount > 250 THEN 80
            WHEN order_status_category = 'DELIVERED' THEN 60
            WHEN order_status_category = 'SHIPPED' THEN 40
            WHEN order_status_category = 'CANCELLED' THEN 0
            ELSE 20
        END as order_performance_score,
        
        -- Customer segment inference
        CASE 
            WHEN is_b2b = TRUE THEN 'B2B_CUSTOMER'
            WHEN order_amount > 1000 THEN 'HIGH_VALUE_B2C'
            WHEN order_amount > 500 THEN 'MEDIUM_VALUE_B2C'
            ELSE 'LOW_VALUE_B2C'
        END as customer_segment,
        
        -- Fulfillment analysis
        CASE 
            WHEN fulfillment_type = 'Amazon' THEN 'FBA'
            WHEN fulfillment_type = 'Merchant' THEN 'FBM'
            ELSE 'OTHER'
        END as fulfillment_model,
        
        -- Geographic tier
        CASE 
            WHEN UPPER(shipping_city) IN ('MUMBAI', 'DELHI', 'BANGALORE', 'CHENNAI', 'HYDERABAD', 'KOLKATA') THEN 'TIER_1'
            WHEN UPPER(shipping_city) IN ('PUNE', 'AHMEDABAD', 'JAIPUR', 'LUCKNOW', 'KANPUR', 'NAGPUR') THEN 'TIER_2'
            ELSE 'TIER_3'
        END as city_tier,
        
        -- Shipping efficiency
        CASE 
            WHEN shipping_service_level = 'Expedited' THEN 'FAST'
            WHEN shipping_service_level = 'Standard' THEN 'STANDARD'
            ELSE 'SLOW'
        END as shipping_speed_category
        
    FROM cleansed_data
)

SELECT 
    -- Generate surrogate key
    MD5(CONCAT(order_id, '-', sku)) as order_item_key,
    
    -- All existing columns
    *,
    
    -- Advanced business metrics
    ROUND(
        (order_performance_score * 0.4 + 
         CASE WHEN fulfillment_model = 'FBA' THEN 80 ELSE 60 END * 0.3 + 
         CASE WHEN city_tier = 'TIER_1' THEN 90 WHEN city_tier = 'TIER_2' THEN 70 ELSE 50 END * 0.3)
    ) as composite_performance_score,
    
    -- Revenue attribution
    CASE 
        WHEN order_status_category = 'DELIVERED' THEN order_amount
        ELSE 0
    END as recognized_revenue,
    
    -- Cohort assignment
    CONCAT(
        order_year, 
        '-', 
        LPAD(CAST(order_month AS VARCHAR), 2, '0')
    ) as order_cohort,
    
    -- Data freshness indicator
    DATEDIFF('day', order_date, CURRENT_DATE) as days_since_order,
    
    -- Business rules validation
    CASE 
        WHEN order_amount < 0 THEN 'NEGATIVE_AMOUNT'
        WHEN quantity <= 0 THEN 'INVALID_QUANTITY'
        WHEN order_date > CURRENT_DATE THEN 'FUTURE_DATE'
        ELSE 'VALID'
    END as business_validation_status

FROM enhanced_metrics