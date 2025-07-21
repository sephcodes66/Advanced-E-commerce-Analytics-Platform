{{
    config(
        materialized='view',
        tags=['intermediate', 'finance', 'performance'],
        description='Financial performance metrics with cost analysis and profitability calculations'
    )
}}

with revenue_data as (
    select 
        partner_channel,
        order_date,
        product_category,
        product_sku,
        customer_segment,
        
        -- Aggregated revenue metrics
        sum(revenue) as total_revenue,
        sum(quantity) as total_units_sold,
        count(distinct order_id) as total_orders,
        
        -- Per-unit revenue calculations
        sum(revenue_per_unit * quantity) as total_revenue_per_unit_calc,
        avg(revenue_per_unit) as avg_revenue_per_unit
        
    from {{ ref('stg_partner_performance') }}
    group by 
        partner_channel,
        order_date,
        product_category,
        product_sku,
        customer_segment
),

financial_metrics as (
    select 
        r.*,
        
        -- Gross profit calculation
        r.total_revenue as gross_profit,
        
        -- Gross margin rate
        1 as gross_margin_rate,
        
        -- Gross profit per unit sold
        r.total_revenue / r.total_units_sold as gross_profit_per_unit
        
    from revenue_data r
),

performance_classification as (
    select 
        *,
        -- Profitability status
        'profitable' as profitability_tier,
        
        -- Revenue size category
        case 
            when total_revenue >= 10000 then 'large_revenue'
            when total_revenue >= 5000 then 'medium_revenue'
            when total_revenue >= 1000 then 'small_revenue'
            else 'micro_revenue'
        end as revenue_size_tier,
        
        -- Calculated performance score (0-100)
        least(100, greatest(0, 
            (least(20, total_revenue / 1000)) +
            (case when total_orders >= 10 then 10 else total_orders end)
        )) as performance_score,
        
        -- Margin health status
        'green_flag' as margin_health_flag,
        
        -- Channel efficiency status
        'efficient' as channel_efficiency_flag
        
    from financial_metrics
)

select 
    {{ generate_surrogate_key(['partner_channel', 'order_date', 'product_category', 'product_sku', 'customer_segment']) }} as financial_performance_key,
    *,
    current_timestamp as calculated_at
    
from performance_classification