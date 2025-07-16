{{
    config(
        materialized='view',
        tags=['intermediate', 'partner_analytics', 'daily'],
        description='Daily aggregated metrics for partner performance analysis'
    )
}}

with daily_partner_metrics as (
    select 
        partner_channel,
        order_date,
        channel_group,
        customer_segment,
        
        -- Volume metrics
        count(distinct order_id) as daily_orders,
        sum(quantity) as daily_units_sold,
        count(distinct product_sku) as daily_unique_products,
        
        -- Revenue metrics
        sum(revenue) as daily_revenue,
        avg(revenue) as daily_avg_order_value,
        sum(revenue_per_unit * quantity) as daily_revenue_per_unit,
        
        -- Performance metrics
        avg(fulfilment_efficiency_score) as avg_fulfilment_efficiency,
        avg(estimated_processing_days) as avg_processing_days,
        
        -- Quality metrics
        count(*) as total_records,
        sum(case when data_quality_flag = 'valid' then 1 else 0 end) as valid_records,
        
        -- Business day adjustments
        case when is_business_day then 1.0 else 0.7 end as business_day_multiplier,
        
        -- Seasonal indicators
        quarter,
        month,
        day_of_week,
        
        -- High value order indicators
        sum(case when order_value_tier = 'high_value' then 1 else 0 end) as high_value_orders,
        sum(case when order_value_tier = 'medium_value' then 1 else 0 end) as medium_value_orders,
        sum(case when order_value_tier = 'low_value' then 1 else 0 end) as low_value_orders

    from {{ ref('stg_partner_performance') }}
    group by 
        partner_channel,
        order_date,
        channel_group,
        customer_segment,
        quarter,
        month,
        day_of_week,
        is_business_day
),

enhanced_metrics as (
    select 
        *,
        -- Data quality score
        {{ safe_divide('valid_records', 'total_records') }} as data_quality_score,
        
        -- Efficiency calculations
        daily_revenue * business_day_multiplier as business_day_adjusted_revenue,
        {{ safe_divide('daily_revenue', 'daily_orders') }} as revenue_per_order,
        {{ safe_divide('daily_units_sold', 'daily_orders') }} as units_per_order,
        
        -- Performance indicators
        case 
            when daily_orders >= 100 then 'high_volume'
            when daily_orders >= 50 then 'medium_volume'
            else 'low_volume'
        end as volume_tier,
        
        -- Revenue concentration
        {{ safe_divide('high_value_orders', 'daily_orders') }} as high_value_order_rate,
        
        -- Day type classification
        case 
            when day_of_week = 1 then 'monday'
            when day_of_week = 2 then 'tuesday'
            when day_of_week = 3 then 'wednesday'
            when day_of_week = 4 then 'thursday'
            when day_of_week = 5 then 'friday'
            when day_of_week = 6 then 'saturday'
            when day_of_week = 0 then 'sunday'
        end as day_name,
        
        -- Rolling calculations (simulated with window functions)
        lag(daily_revenue, 1) over (
            partition by partner_channel, customer_segment 
            order by order_date
        ) as previous_day_revenue,
        
        avg(daily_revenue) over (
            partition by partner_channel, customer_segment 
            order by order_date 
            rows between 6 preceding and current row
        ) as rolling_7_day_avg_revenue,
        
        avg(daily_orders) over (
            partition by partner_channel, customer_segment 
            order by order_date 
            rows between 29 preceding and current row
        ) as rolling_30_day_avg_orders
        
    from daily_partner_metrics
),

final_metrics as (
    select 
        {{ generate_surrogate_key(['partner_channel', 'order_date', 'customer_segment']) }} as daily_metrics_key,
        *,
        
        -- Growth calculations
        case 
            when previous_day_revenue > 0 then 
                (daily_revenue - previous_day_revenue) / previous_day_revenue
            else null
        end as daily_revenue_growth_rate,
        
        -- Performance vs moving average
        case 
            when rolling_7_day_avg_revenue > 0 then 
                daily_revenue / rolling_7_day_avg_revenue
            else 1.0
        end as performance_vs_7_day_avg,
        
        -- Trend indicators
        case 
            when daily_revenue > rolling_7_day_avg_revenue * 1.1 then 'above_trend'
            when daily_revenue < rolling_7_day_avg_revenue * 0.9 then 'below_trend'
            else 'on_trend'
        end as revenue_trend_indicator,
        
        current_timestamp as processed_at
        
    from enhanced_metrics
)

select * from final_metrics
where order_date >= current_date - interval '{{ var("partner_performance_lookback_days") }}' DAY