{{ config(
    materialized='table',
    tags=['marts', 'core', 'unified_sales'],
    meta={
        'owner': 'analytics_team',
        'description': 'Unified sales data across all channels with advanced analytics',
        'refresh_frequency': 'daily'
    }
) }}

WITH amazon_sales AS (
    SELECT 
        order_item_key,
        order_id,
        'AMAZON' as sales_channel,
        order_date,
        order_year,
        order_month,
        season,
        
        -- Product information
        sku,
        product_category,
        product_size,
        
        -- Customer information
        customer_segment,
        is_b2b,
        city_tier,
        
        -- Financial metrics
        order_amount,
        quantity,
        unit_price,
        recognized_revenue,
        estimated_gst_amount,
        net_order_amount,
        
        -- Performance metrics
        order_performance_score,
        composite_performance_score,
        
        -- Geographic
        shipping_city,
        shipping_state,
        shipping_country,
        
        -- Business attributes
        fulfillment_model,
        order_status_category,
        has_promotion,
        
        -- Data lineage
        ingested_at
        
    FROM {{ ref('stg_amazon_sales') }}
    WHERE business_validation_status = 'VALID'
),

international_sales AS (
    SELECT 
        MD5(CONCAT(sku, '-', order_date, '-', customer_name)) as order_item_key,
        CONCAT('INTL-', ROW_NUMBER() OVER (ORDER BY order_date)) as order_id,
        'INTERNATIONAL' as sales_channel,
        order_date,
        order_year,
        order_month,
        season,
        
        -- Product information
        sku,
        product_category,
        product_size,
        
        -- Customer information
        customer_segment,
        FALSE as is_b2b,
        'INTERNATIONAL' as city_tier,
        
        -- Financial metrics
        gross_amount as order_amount,
        quantity,
        unit_rate as unit_price,
        gross_amount as recognized_revenue,
        ROUND(gross_amount * 0.18, 2) as estimated_gst_amount,
        ROUND(gross_amount * 0.82, 2) as net_order_amount,
        
        -- Performance metrics
        85 as order_performance_score,
        90 as composite_performance_score,
        
        -- Geographic
        'INTERNATIONAL' as shipping_city,
        'INTERNATIONAL' as shipping_state,
        'INTERNATIONAL' as shipping_country,
        
        -- Business attributes
        'DIRECT' as fulfillment_model,
        'DELIVERED' as order_status_category,
        FALSE as has_promotion,
        
        -- Data lineage
        ingested_at
        
    FROM {{ ref('raw_international_sales') }}
    WHERE data_quality_flag = 'VALID'
),

unified_sales AS (
    SELECT * FROM amazon_sales
    UNION ALL
    SELECT * FROM international_sales
),

enhanced_analytics AS (
    SELECT 
        *,
        -- Advanced time-based metrics
        LAG(order_amount, 1) OVER (
            PARTITION BY sales_channel, sku 
            ORDER BY order_date
        ) as prev_order_amount,
        
        -- Rolling metrics
        AVG(order_amount) OVER (
            PARTITION BY sales_channel 
            ORDER BY order_date 
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) as rolling_30d_avg_order_value,
        
        SUM(recognized_revenue) OVER (
            PARTITION BY sales_channel, sku 
            ORDER BY order_date 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) as rolling_7d_revenue,
        
        -- Rank metrics
        RANK() OVER (
            PARTITION BY sales_channel, order_date 
            ORDER BY order_amount DESC
        ) as daily_order_rank,
        
        ROW_NUMBER() OVER (
            PARTITION BY sku 
            ORDER BY order_date
        ) as product_order_sequence,
        
        -- Growth metrics
        CASE 
            WHEN LAG(order_amount, 1) OVER (
                PARTITION BY sales_channel, sku 
                ORDER BY order_date
            ) IS NOT NULL THEN
                ROUND(
                    ((order_amount - LAG(order_amount, 1) OVER (
                        PARTITION BY sales_channel, sku 
                        ORDER BY order_date
                    )) / LAG(order_amount, 1) OVER (
                        PARTITION BY sales_channel, sku 
                        ORDER BY order_date
                    )) * 100, 2
                )
            ELSE NULL
        END as order_value_growth_rate,
        
        -- Cohort analysis
        CONCAT(order_year, '-', LPAD(CAST(order_month AS VARCHAR), 2, '0')) as order_cohort,
        
        -- Customer lifetime value components
        COUNT(*) OVER (
            PARTITION BY sales_channel, customer_segment 
            ORDER BY order_date 
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) as customer_segment_order_count,
        
        -- Product performance
        DENSE_RANK() OVER (
            PARTITION BY product_category 
            ORDER BY composite_performance_score DESC
        ) as category_performance_rank,
        
        -- Seasonality analysis
        AVG(order_amount) OVER (
            PARTITION BY season, product_category
        ) as seasonal_avg_order_value,
        
        -- Channel performance
        PERCENT_RANK() OVER (
            PARTITION BY sales_channel 
            ORDER BY composite_performance_score
        ) as channel_performance_percentile

    FROM unified_sales
)

SELECT 
    -- Primary key
    order_item_key,
    
    -- Identifiers
    order_id,
    sales_channel,
    sku,
    
    -- Temporal dimensions
    order_date,
    order_year,
    order_month,
    season,
    order_cohort,
    
    -- Product dimensions
    product_category,
    product_size,
    
    -- Customer dimensions
    customer_segment,
    is_b2b,
    city_tier,
    
    -- Financial metrics
    order_amount,
    quantity,
    unit_price,
    recognized_revenue,
    estimated_gst_amount,
    net_order_amount,
    
    -- Performance metrics
    order_performance_score,
    composite_performance_score,
    daily_order_rank,
    category_performance_rank,
    channel_performance_percentile,
    
    -- Growth and trend metrics
    prev_order_amount,
    order_value_growth_rate,
    rolling_30d_avg_order_value,
    rolling_7d_revenue,
    seasonal_avg_order_value,
    
    -- Sequence metrics
    product_order_sequence,
    customer_segment_order_count,
    
    -- Geographic
    shipping_city,
    shipping_state,
    shipping_country,
    
    -- Business attributes
    fulfillment_model,
    order_status_category,
    has_promotion,
    
    -- Data lineage
    ingested_at,
    CURRENT_TIMESTAMP as processed_at

FROM enhanced_analytics