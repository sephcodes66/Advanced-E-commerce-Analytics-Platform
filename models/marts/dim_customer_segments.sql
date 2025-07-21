{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'customer_segmentation'],
        description='Customer segmentation dimension with advanced analytics and behavioral insights'
    )
}}

with customer_base as (
    select 
        customer_segment,
        partner_channel,
        
        -- Key customer metrics
        count(distinct order_id) as total_orders,
        sum(revenue) as total_revenue,
        sum(quantity) as total_units_purchased,
        count(distinct product_sku) as unique_products_purchased,
        count(distinct order_date) as active_days,
        
        -- Customer timeline metrics
        min(order_date) as first_order_date,
        max(order_date) as last_order_date,
        datediff('day', min(order_date), max(order_date)) as customer_lifespan_days,
        
        -- Customer order patterns
        avg(revenue) as avg_order_value,
        avg(quantity) as avg_units_per_order,
        stddev(revenue) as revenue_volatility,
        
        -- Customer behavior indicators
        count(case when order_value_tier = 'high_value' then 1 end) as high_value_orders,
        count(case when is_b2b then 1 end) as b2b_orders,
        count(case when is_business_day then 1 end) as business_day_orders,
        
        -- Customer channel preferences
        count(distinct case when partner_channel = 'amazon' then order_date end) as amazon_active_days,
        count(distinct case when partner_channel = 'merchant_website' then order_date end) as merchant_active_days,
        count(distinct case when partner_channel = 'international' then order_date end) as international_active_days,
        
        -- Quarterly order patterns
        count(case when quarter = 1 then 1 end) as q1_orders,
        count(case when quarter = 2 then 1 end) as q2_orders,
        count(case when quarter = 3 then 1 end) as q3_orders,
        count(case when quarter = 4 then 1 end) as q4_orders
        
    from {{ ref('stg_partner_performance') }}
    group by customer_segment, partner_channel
),

customer_analytics as (
    select 
        *,
        -- Estimated annual customer value
        case 
            when customer_lifespan_days > 0 then
                (total_revenue / customer_lifespan_days) * 365
            else total_revenue
        end as estimated_annual_value,
        
        -- Monthly purchase frequency
        case 
            when customer_lifespan_days > 0 then
                total_orders * 1.0 / (customer_lifespan_days / 30.0)
            else total_orders
        end as orders_per_month,
        
        -- Customer loyalty level
        case 
            when total_orders >= 10 then 'highly_loyal'
            when total_orders >= 5 then 'loyal'
            when total_orders >= 2 then 'repeat_customer'
            else 'one_time_buyer'
        end as loyalty_tier,
        
        -- Customer value category
        case 
            when total_revenue >= 5000 then 'high_value'
            when total_revenue >= 2000 then 'medium_value'
            when total_revenue >= 500 then 'low_value'
            else 'minimal_value'
        end as value_tier,
        
        -- Customer engagement level
        case 
            when active_days >= 30 then 'highly_engaged'
            when active_days >= 15 then 'engaged'
            when active_days >= 7 then 'moderately_engaged'
            else 'low_engagement'
        end as engagement_level,
        
        -- Dominant shopping channel
        case 
            when amazon_active_days >= merchant_active_days and amazon_active_days >= international_active_days then 'amazon_preferred'
            when merchant_active_days >= international_active_days then 'merchant_preferred'
            when international_active_days > 0 then 'international_preferred'
            else 'no_clear_preference'
        end as channel_preference,
        
        -- Peak shopping season
        case 
            when q4_orders >= greatest(q1_orders, q2_orders, q3_orders) then 'q4_seasonal'
            when q3_orders >= greatest(q1_orders, q2_orders, q4_orders) then 'q3_seasonal'
            when q2_orders >= greatest(q1_orders, q3_orders, q4_orders) then 'q2_seasonal'
            when q1_orders >= greatest(q2_orders, q3_orders, q4_orders) then 'q1_seasonal'
            else 'no_seasonal_pattern'
        end as seasonal_pattern,
        
        -- Customer churn risk
        case 
            when last_order_date < current_date - interval '90 days' then 'high_churn_risk'
            when last_order_date < current_date - interval '60 days' then 'medium_churn_risk'
            when last_order_date < current_date - interval '30 days' then 'low_churn_risk'
            else 'active'
        end as churn_risk_level,
        
        -- Business customer classification
        case 
            when b2b_orders * 1.0 / total_orders >= 0.8 then 'primarily_b2b'
            when b2b_orders * 1.0 / total_orders >= 0.5 then 'mixed_b2b_b2c'
            when b2b_orders > 0 then 'occasional_b2b'
            else 'b2c_only'
        end as business_customer_type,
        
        -- Customer spending consistency
        case 
            when revenue_volatility / avg_order_value > 1.5 then 'variable_spender'
            when revenue_volatility / avg_order_value > 0.8 then 'moderate_spender'
            else 'consistent_spender'
        end as spending_pattern,
        
        -- Likelihood of placing high-value orders
        case 
            when high_value_orders * 1.0 / total_orders >= 0.3 then 'high_value_buyer'
            when high_value_orders * 1.0 / total_orders >= 0.1 then 'occasional_high_value'
            else 'standard_buyer'
        end as high_value_propensity
        
    from customer_base
),

advanced_segmentation as (
    select 
        *,
        -- Recency, Frequency, Monetary (RFM) scores
        case 
            when last_order_date >= current_date - interval '30 days' then 5
            when last_order_date >= current_date - interval '60 days' then 4
            when last_order_date >= current_date - interval '90 days' then 3
            when last_order_date >= current_date - interval '180 days' then 2
            else 1
        end as recency_score,
        
        case 
            when orders_per_month >= 5 then 5
            when orders_per_month >= 3 then 4
            when orders_per_month >= 1 then 3
            when orders_per_month >= 0.5 then 2
            else 1
        end as frequency_score,
        
        case 
            when total_revenue >= 5000 then 5
            when total_revenue >= 2000 then 4
            when total_revenue >= 1000 then 3
            when total_revenue >= 500 then 2
            else 1
        end as monetary_score,
        
        -- Strategic customer segment
        case 
            when customer_segment = 'b2b' and total_revenue >= 10000 then 'enterprise_partner'
            when customer_segment = 'b2b' and total_revenue >= 5000 then 'business_partner'
            when customer_segment = 'b2b' then 'small_business'
            when customer_segment = 'b2c' and total_revenue >= 2000 then 'vip_consumer'
            when customer_segment = 'b2c' and total_orders >= 10 then 'loyal_consumer'
            when customer_segment = 'b2c' and total_orders >= 2 then 'repeat_consumer'
            else 'new_consumer'
        end as strategic_segment,
        
        -- Customer marketing persona
        case 
            when channel_preference = 'amazon_preferred' and high_value_propensity = 'high_value_buyer' then 'amazon_premium_shopper'
            when channel_preference = 'merchant_preferred' and loyalty_tier = 'highly_loyal' then 'brand_loyalist'
            when channel_preference = 'international_preferred' then 'global_customer'
            when business_customer_type = 'primarily_b2b' then 'business_procurement'
            when seasonal_pattern != 'no_seasonal_pattern' then 'seasonal_buyer'
            when spending_pattern = 'variable_spender' then 'opportunistic_buyer'
            else 'standard_customer'
        end as marketing_persona
        
    from customer_analytics
)

select 
    {{ generate_surrogate_key(['customer_segment', 'partner_channel']) }} as customer_segment_key,
    *,
    
    -- Final RFM score
    (recency_score + frequency_score + monetary_score) as rfm_total_score,
    
    -- Customer LTV category
    case 
        when estimated_annual_value >= 10000 then 'platinum'
        when estimated_annual_value >= 5000 then 'gold'
        when estimated_annual_value >= 2000 then 'silver'
        when estimated_annual_value >= 500 then 'bronze'
        else 'basic'
    end as clv_tier,
    
    -- Recommended marketing action
    case 
        when churn_risk_level = 'high_churn_risk' and value_tier in ('high_value', 'medium_value') then 'retention_campaign'
        when loyalty_tier = 'one_time_buyer' and avg_order_value >= 100 then 'repeat_purchase_incentive'
        when strategic_segment = 'enterprise_partner' then 'account_management'
        when marketing_persona = 'amazon_premium_shopper' then 'premium_product_recommendation'
        when seasonal_pattern != 'no_seasonal_pattern' then 'seasonal_marketing'
        else 'standard_marketing'
    end as recommended_action,
    
    -- Customer investment priority
    case 
        when strategic_segment in ('enterprise_partner', 'vip_consumer') then 'high_priority'
        when strategic_segment in ('business_partner', 'loyal_consumer') then 'medium_priority'
        else 'low_priority'
    end as investment_priority,
    
    current_timestamp as segment_created_at
    
from advanced_segmentation
order by estimated_annual_value desc, total_revenue desc