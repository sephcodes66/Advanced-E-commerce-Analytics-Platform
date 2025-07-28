{{
    config(
        materialized='table',
        tags=['marts', 'partner_analytics', 'dashboard'],
        description='Comprehensive partner performance dashboard with KPIs and insights for ZMS team'
    )
}}

with partner_summary as (
    select 
        partner_channel,
        channel_group,
        customer_segment,
        
        -- Performance in the last 30 days
        sum(case when order_date >= current_date - interval '30 days' then daily_revenue else 0 end) as revenue_30d,
        sum(case when order_date >= current_date - interval '30 days' then daily_orders else 0 end) as orders_30d,
        sum(case when order_date >= current_date - interval '30 days' then daily_units_sold else 0 end) as units_30d,
        
        -- Performance in the previous 30-day period
        sum(case when order_date between current_date - interval '60 days' and current_date - interval '31 days' then daily_revenue else 0 end) as revenue_30d_prev,
        sum(case when order_date between current_date - interval '60 days' and current_date - interval '31 days' then daily_orders else 0 end) as orders_30d_prev,
        
        -- Quarter-to-date (QTD) performance
        sum(case when extract(quarter from order_date) = extract(quarter from current_date) 
                  and extract(year from order_date) = extract(year from current_date) 
                  then daily_revenue else 0 end) as revenue_qtd,
        sum(case when extract(quarter from order_date) = extract(quarter from current_date) 
                  and extract(year from order_date) = extract(year from current_date) 
                  then daily_orders else 0 end) as orders_qtd,
        
        -- Year-to-date (YTD) performance
        sum(case when extract(year from order_date) = extract(year from current_date) 
                  then daily_revenue else 0 end) as revenue_ytd,
        sum(case when extract(year from order_date) = extract(year from current_date) 
                  then daily_orders else 0 end) as orders_ytd,
        
        -- Key performance indicators (KPIs)
        avg(case when order_date >= current_date - interval '30 days' then daily_avg_order_value else null end) as avg_order_value_30d,
        avg(case when order_date >= current_date - interval '30 days' then avg_fulfilment_efficiency else null end) as avg_fulfilment_efficiency_30d,
        avg(case when order_date >= current_date - interval '30 days' then data_quality_score else null end) as avg_data_quality_30d,
        
        -- Revenue trend indicators
        count(case when order_date >= current_date - interval '30 days' and revenue_trend_indicator = 'above_trend' then 1 else null end) as above_trend_days,
        count(case when order_date >= current_date - interval '30 days' and revenue_trend_indicator = 'below_trend' then 1 else null end) as below_trend_days,
        
        -- High-value order distribution
        avg(case when order_date >= current_date - interval '30 days' then high_value_order_rate else null end) as high_value_order_rate_30d,
        
        -- Operational efficiency metrics
        avg(case when order_date >= current_date - interval '30 days' then avg_processing_days else null end) as avg_processing_days_30d
        
    from {{ ref('int_partner_daily_metrics') }}
    group by partner_channel, channel_group, customer_segment
),

financial_summary as (
    select 
        partner_channel,
        customer_segment,
        
        -- Profitability in the last 30 days
        sum(case when order_date >= current_date - interval '30 days' then gross_profit else 0 end) as gross_profit_30d,
        
        -- Gross margin metrics
        avg(case when order_date >= current_date - interval '30 days' then gross_margin_rate else null end) as avg_gross_margin_30d,
        
        -- Profit per unit
        avg(case when order_date >= current_date - interval '30 days' then gross_profit_per_unit else null end) as avg_gross_profit_per_unit_30d,
        
        -- Dominant profitability category
        mode() within group (order by case when order_date >= current_date - interval '30 days' then profitability_tier else null end) as dominant_profitability_tier,
        avg(case when order_date >= current_date - interval '30 days' then performance_score else null end) as avg_performance_score_30d,
        
        -- Margin health and efficiency
        count(case when order_date >= current_date - interval '30 days' and margin_health_flag = 'red_flag' then 1 else null end) as red_flag_days,
        count(case when order_date >= current_date - interval '30 days' and channel_efficiency_flag = 'efficient' then 1 else null end) as efficient_days
        
    from {{ ref('int_financial_performance') }}
    group by partner_channel, customer_segment
),

dashboard_metrics as (
    select 
        p.partner_channel,
        p.channel_group,
        p.customer_segment,
        
        -- Revenue performance and growth
        p.revenue_30d,
        p.revenue_30d_prev,
        p.revenue_qtd,
        p.revenue_ytd,
        
        -- 30-day revenue growth
        case 
            when p.revenue_30d_prev > 0 then (p.revenue_30d - p.revenue_30d_prev) / p.revenue_30d_prev
            else null
        end as revenue_growth_30d,
        
        -- Order volume and growth
        p.orders_30d,
        p.orders_30d_prev,
        p.orders_qtd,
        p.orders_ytd,
        
        -- 30-day order growth
        case 
            when p.orders_30d_prev > 0 then (p.orders_30d - p.orders_30d_prev) / p.orders_30d_prev
            else null
        end as order_growth_30d,
        
        -- Sales volume and average order value
        p.units_30d,
        p.avg_order_value_30d,
        
        -- Gross profit and margin
        coalesce(f.gross_profit_30d, 0) as gross_profit_30d,
        coalesce(f.avg_gross_margin_30d, 0) as avg_gross_margin_30d,
        
        -- Gross profit per unit
        coalesce(f.avg_gross_profit_per_unit_30d, 0) as avg_gross_profit_per_unit_30d,
        
        -- Operational performance indicators
        p.avg_fulfilment_efficiency_30d,
        p.avg_data_quality_30d,
        p.avg_processing_days_30d,
        coalesce(f.avg_performance_score_30d, 0) as avg_performance_score_30d,
        
        -- Overall revenue trend
        p.above_trend_days,
        p.below_trend_days,
        case 
            when p.above_trend_days > p.below_trend_days then 'positive_trend'
            when p.below_trend_days > p.above_trend_days then 'negative_trend'
            else 'neutral_trend'
        end as overall_trend_direction,
        
        -- Data and profitability quality
        p.high_value_order_rate_30d,
        coalesce(f.dominant_profitability_tier, 'unknown') as dominant_profitability_tier,
        coalesce(f.red_flag_days, 0) as red_flag_days,
        coalesce(f.efficient_days, 0) as efficient_days,
        
        -- Monthly revenue target status
        case 
            when p.revenue_30d >= {{ var('revenue_target_monthly') }} then 'achieved'
            when p.revenue_30d >= {{ var('revenue_target_monthly') }} * 0.8 then 'near_target'
            else 'below_target'
        end as monthly_target_status,
        
        -- Partner channel efficiency
        case 
            when coalesce(f.efficient_days, 0) >= 20 then 'highly_efficient'
            when coalesce(f.efficient_days, 0) >= 15 then 'efficient'
            when coalesce(f.efficient_days, 0) >= 10 then 'moderately_efficient'
            else 'needs_improvement'
        end as efficiency_rating,
        
        -- Composite partner health score
        greatest(0, least(100, 
            (case when p.revenue_30d > 0 then least(25, p.revenue_30d / {{ var('revenue_target_monthly') }} * 25) else 0 end) +
            (case when coalesce(f.avg_gross_margin_30d, 0) > 0 then coalesce(f.avg_gross_margin_30d, 0) * 25 else 0 end) +
            (case when p.avg_data_quality_30d > 0 then p.avg_data_quality_30d * 25 else 0 end) +
            (case when p.avg_fulfilment_efficiency_30d > 0 then p.avg_fulfilment_efficiency_30d / 100 * 25 else 0 end)
        )) as overall_health_score
        
    from partner_summary p
    left join financial_summary f 
        on p.partner_channel = f.partner_channel 
        and p.customer_segment = f.customer_segment
)

select 
    {{ generate_surrogate_key(['partner_channel', 'customer_segment']) }} as dashboard_key,
    *,
    
    -- Recommended actions based on performance
    case 
        when overall_health_score >= 80 then 'maintain_current_strategy'
        when overall_health_score >= 60 then 'optimize_operations'
        when overall_health_score >= 40 then 'strategic_review_needed'
        else 'immediate_action_required'
    end as recommended_action,
    
    -- Partner priority based on health and revenue
    row_number() over (order by overall_health_score desc, revenue_30d desc) as partner_priority_rank,
    
    current_timestamp as dashboard_updated_at
    
from dashboard_metrics
order by overall_health_score desc, revenue_30d desc