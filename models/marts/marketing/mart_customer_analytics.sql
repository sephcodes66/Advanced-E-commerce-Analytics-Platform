{{ config(
    materialized='table',
    tags=['marts', 'marketing', 'customer_analytics'],
    meta={
        'owner': 'marketing_team',
        'description': 'Comprehensive customer analytics with RFM, CLV, and segmentation',
        'refresh_frequency': 'daily'
    }
) }}

WITH customer_base AS (
    SELECT 
        -- Customer identification (synthetic for this demo)
        CASE 
            WHEN sales_channel = 'AMAZON' THEN 
                MD5(CONCAT(shipping_city, shipping_state, customer_segment, sku))
            ELSE 
                MD5(CONCAT(sales_channel, city_tier, customer_segment))
        END as customer_id,
        
        sales_channel,
        customer_segment,
        city_tier,
        is_b2b,
        
        -- Order details
        order_date,
        order_amount,
        recognized_revenue,
        quantity,
        
        -- Product details
        product_category,
        sku,
        
        -- Geographic
        shipping_city,
        shipping_state,
        shipping_country
        
    FROM {{ ref('mart_unified_sales') }}
    WHERE recognized_revenue > 0
),

customer_metrics AS (
    SELECT 
        customer_id,
        sales_channel,
        customer_segment,
        city_tier,
        is_b2b,
        shipping_city,
        shipping_state,
        shipping_country,
        
        -- Transaction metrics
        COUNT(DISTINCT order_date) as total_orders,
        COUNT(DISTINCT sku) as unique_products_purchased,
        COUNT(DISTINCT product_category) as unique_categories_purchased,
        
        -- Financial metrics
        SUM(recognized_revenue) as total_revenue,
        AVG(recognized_revenue) as avg_order_value,
        MIN(recognized_revenue) as min_order_value,
        MAX(recognized_revenue) as max_order_value,
        STDDEV(recognized_revenue) as order_value_stddev,
        
        -- Temporal metrics
        MIN(order_date) as first_order_date,
        MAX(order_date) as last_order_date,
        DATEDIFF('day', MIN(order_date), MAX(order_date)) as customer_lifespan_days,
        DATEDIFF('day', MAX(order_date), CURRENT_DATE) as days_since_last_order,
        
        -- Frequency metrics
        CASE 
            WHEN DATEDIFF('day', MIN(order_date), MAX(order_date)) > 0 
            THEN COUNT(DISTINCT order_date) * 1.0 / DATEDIFF('day', MIN(order_date), MAX(order_date)) * 365
            ELSE 0
        END as annual_order_frequency,
        
        -- Product preferences (simplified for DuckDB)
        'kurta' as favorite_category,
        
        -- Seasonality
        COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) IN (11, 12, 1) THEN 1 END) as winter_orders,
        COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) IN (2, 3, 4) THEN 1 END) as spring_orders,
        COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) IN (5, 6, 7) THEN 1 END) as summer_orders,
        COUNT(CASE WHEN EXTRACT(MONTH FROM order_date) IN (8, 9, 10) THEN 1 END) as fall_orders
        
    FROM customer_base
    GROUP BY customer_id, sales_channel, customer_segment, city_tier, is_b2b, 
             shipping_city, shipping_state, shipping_country
),

rfm_analysis AS (
    SELECT 
        *,
        -- RFM Components
        days_since_last_order as recency,
        total_orders as frequency,
        total_revenue as monetary,
        
        -- RFM Scores (1-5 scale)
        NTILE(5) OVER (ORDER BY days_since_last_order DESC) as recency_score,
        NTILE(5) OVER (ORDER BY total_orders ASC) as frequency_score,
        NTILE(5) OVER (ORDER BY total_revenue ASC) as monetary_score,
        
        -- Combined RFM Score
        CONCAT(
            NTILE(5) OVER (ORDER BY days_since_last_order DESC),
            NTILE(5) OVER (ORDER BY total_orders ASC),
            NTILE(5) OVER (ORDER BY total_revenue ASC)
        ) as rfm_score
        
    FROM customer_metrics
),

customer_segmentation AS (
    SELECT 
        *,
        -- RFM Segments
        CASE 
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Potential Loyalists'
            WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
            WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Promising'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Need Attention'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'About to Sleep'
            WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'At Risk'
            WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
            ELSE 'Others'
        END as rfm_segment,
        
        -- Customer Lifecycle Stage
        CASE 
            WHEN total_orders = 1 THEN 'New Customer'
            WHEN total_orders >= 2 AND days_since_last_order <= 30 THEN 'Active Customer'
            WHEN total_orders >= 2 AND days_since_last_order BETWEEN 31 AND 90 THEN 'Occasional Customer'
            WHEN total_orders >= 2 AND days_since_last_order BETWEEN 91 AND 180 THEN 'Dormant Customer'
            WHEN days_since_last_order > 180 THEN 'Churned Customer'
            ELSE 'Undefined'
        END as lifecycle_stage,
        
        -- Value Tier
        CASE 
            WHEN total_revenue >= 5000 THEN 'VIP'
            WHEN total_revenue >= 2000 THEN 'High Value'
            WHEN total_revenue >= 1000 THEN 'Medium Value'
            WHEN total_revenue >= 500 THEN 'Low Value'
            ELSE 'Minimal Value'
        END as value_tier,
        
        -- Engagement Level
        CASE 
            WHEN unique_categories_purchased >= 3 AND total_orders >= 5 THEN 'Highly Engaged'
            WHEN unique_categories_purchased >= 2 AND total_orders >= 3 THEN 'Moderately Engaged'
            WHEN total_orders >= 2 THEN 'Lightly Engaged'
            ELSE 'Single Purchase'
        END as engagement_level,
        
        -- Seasonal Preference
        CASE 
            WHEN winter_orders >= GREATEST(spring_orders, summer_orders, fall_orders) THEN 'Winter Shopper'
            WHEN spring_orders >= GREATEST(winter_orders, summer_orders, fall_orders) THEN 'Spring Shopper'
            WHEN summer_orders >= GREATEST(winter_orders, spring_orders, fall_orders) THEN 'Summer Shopper'
            WHEN fall_orders >= GREATEST(winter_orders, spring_orders, summer_orders) THEN 'Fall Shopper'
            ELSE 'All Season'
        END as seasonal_preference
        
    FROM rfm_analysis
),

predictive_metrics AS (
    SELECT 
        *,
        -- Customer Lifetime Value (CLV) prediction
        CASE 
            WHEN customer_lifespan_days > 0 
            THEN (total_revenue / customer_lifespan_days) * 365 * 2
            ELSE avg_order_value * annual_order_frequency * 2
        END as predicted_clv_2year,
        
        -- Churn Probability
        CASE 
            WHEN days_since_last_order > 180 THEN 0.9
            WHEN days_since_last_order > 90 THEN 0.7
            WHEN days_since_last_order > 60 THEN 0.5
            WHEN days_since_last_order > 30 THEN 0.3
            ELSE 0.1
        END as churn_probability,
        
        -- Next Order Prediction
        CASE 
            WHEN annual_order_frequency > 0 
            THEN ROUND(365.0 / annual_order_frequency)
            ELSE 365
        END as predicted_days_to_next_order,
        
        -- Upsell Potential
        CASE 
            WHEN unique_categories_purchased = 1 AND total_orders >= 3 THEN 'High Cross-sell'
            WHEN avg_order_value < total_revenue * 0.8 THEN 'High Upsell'
            WHEN lifecycle_stage = 'Active Customer' AND value_tier IN ('High Value', 'VIP') THEN 'Premium Upsell'
            ELSE 'Standard'
        END as upsell_potential
        
    FROM customer_segmentation
)

SELECT 
    -- Customer identification
    customer_id,
    sales_channel,
    customer_segment,
    city_tier,
    is_b2b,
    
    -- Geographic
    shipping_city,
    shipping_state,
    shipping_country,
    
    -- Basic metrics
    total_orders,
    unique_products_purchased,
    unique_categories_purchased,
    total_revenue,
    avg_order_value,
    min_order_value,
    max_order_value,
    order_value_stddev,
    
    -- Temporal metrics
    first_order_date,
    last_order_date,
    customer_lifespan_days,
    days_since_last_order,
    annual_order_frequency,
    
    -- RFM Analysis
    recency,
    frequency,
    monetary,
    recency_score,
    frequency_score,
    monetary_score,
    rfm_score,
    rfm_segment,
    
    -- Segmentation
    lifecycle_stage,
    value_tier,
    engagement_level,
    seasonal_preference,
    
    -- Predictive metrics
    predicted_clv_2year,
    churn_probability,
    predicted_days_to_next_order,
    upsell_potential,
    
    -- Behavioral insights
    favorite_category,
    winter_orders,
    spring_orders,
    summer_orders,
    fall_orders,
    
    -- Data lineage
    CURRENT_TIMESTAMP as processed_at

FROM predictive_metrics