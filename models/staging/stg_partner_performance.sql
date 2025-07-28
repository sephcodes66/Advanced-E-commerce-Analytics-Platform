{{
    config(
        materialized='view',
        tags=['staging', 'partner_analytics'],
        description='Staging model for partner performance metrics'
    )
}}

with source_data as (
    select 
        'amazon' as partner_channel,
        order_id,
        order_date,
        order_status,
        fulfillment_type,
        coalesce(product_category, 'Unknown') as product_category,
        sku as product_sku,
        style_code as product_style,
        asin as product_asin,
        quantity,
        order_amount as revenue,
        currency_code as currency,
        is_b2b,
        'amazon_sales' as _source_file_name
    from {{ ref('stg_amazon_sales') }}
    
    union all
    
    select 
        'merchant_website' as partner_channel,
        order_id,
        order_date,
        order_status,
        fulfilment_method as fulfillment_type,
        coalesce(product_category, 'Unknown') as product_category,
        product_sku,
        NULL as product_style,
        product_asin,
        quantity,
        amount as revenue,
        currency,
        is_b2b_sale as is_b2b,
        _source_file_name
    from {{ ref('stg_sale_report') }}
    
    union all
    
    select 
        'international' as partner_channel,
        order_id,
        order_date,
        order_status,
        fulfilment_method as fulfillment_type,
        coalesce(product_category, 'Unknown') as product_category,
        product_sku,
        product_style,
        product_asin,
        quantity,
        amount as revenue,
        currency,
        is_b2b_sale as is_b2b,
        _source_file_name
    from {{ ref('stg_international_sale_report') }}
),

enriched_data as (
    select 
        *,
        -- Order value classification
        case 
            when revenue >= {{ var('high_value_order_threshold') }} then 'high_value'
            when revenue >= 100 then 'medium_value'
            else 'low_value'
        end as order_value_tier,
        
        -- Customer type (B2B or B2C)
        case 
            when is_b2b then 'b2b'
            else 'b2c'
        end as customer_segment,
        
        -- Fulfillment efficiency rating
        case 
            when fulfillment_type = 'amazon' then 95
            when fulfillment_type = 'merchant' then 85
            else 80
        end as fulfilment_efficiency_score,
        
        -- Revenue per item sold
        case 
            when quantity > 0 then revenue / quantity
            else 0
        end as revenue_per_unit,
        
        -- Estimated order processing time (demo data)
        case 
            when order_status = 'shipped' then 2
            when order_status = 'pending' then 5
            else 10
        end as estimated_processing_days,
        
        -- Data validation flags
        case 
            when order_id is null then 'missing_order_id'
            when revenue <= 0 then 'invalid_revenue'
            when quantity <= 0 then 'invalid_quantity'
            else 'valid'
        end as data_quality_flag,
        
        -- Time-based features for analysis
        extract(quarter from order_date) as quarter,
        extract(month from order_date) as month,
        extract(dow from order_date) as day_of_week,
        
        -- Identifies sales on business days
        case 
            when extract(dow from order_date) in (0, 6) then false
            else true
        end as is_business_day,
        
        -- Primary and secondary sales channels
        case 
            when partner_channel in ('amazon', 'merchant_website') then 'primary_channels'
            else 'secondary_channels'
        end as channel_group
        
    from source_data
)

select 
    {{ generate_surrogate_key(['partner_channel', 'order_id', 'product_sku']) }} as partner_performance_key,
    *
from enriched_data
where data_quality_flag = 'valid'
  and order_date >= '2021-01-01'  -- Filter out old or invalid records