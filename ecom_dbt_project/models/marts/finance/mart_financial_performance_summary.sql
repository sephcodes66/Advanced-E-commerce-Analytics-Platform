{{
    config(
        materialized='incremental',
        unique_key='financial_summary_key',
        tags=['marts', 'finance', 'incremental'],
        description='Incremental financial performance summary for executive reporting and strategic decision-making'
    )
}}

with financial_base as (
    select 
        order_date,
        partner_channel,
        product_category,
        customer_segment,
        
        -- Revenue metrics
        sum(total_revenue) as daily_revenue,
        sum(total_units_sold) as daily_units_sold,
        sum(total_orders) as daily_orders,
        
        -- Cost and profitability
        sum(gross_profit) as daily_gross_profit,
        
        -- Margin calculations
        case 
            when sum(total_revenue) > 0 then sum(gross_profit) / sum(total_revenue)
            else 0
        end as daily_gross_margin_rate,
        
        -- Unit economics
        case 
            when sum(total_units_sold) > 0 then sum(gross_profit) / sum(total_units_sold)
            else 0
        end as daily_gross_profit_per_unit,
        
        -- Performance metrics
        avg(performance_score) as daily_avg_performance_score,
        
        -- Health indicators
        count(case when profitability_tier = 'highly_profitable' then 1 end) as highly_profitable_skus,
        count(case when profitability_tier = 'loss_making' then 1 end) as loss_making_skus,
        count(case when margin_health_flag = 'red_flag' then 1 end) as red_flag_skus,
        count(case when channel_efficiency_flag = 'efficient' then 1 end) as efficient_skus,
        count(*) as total_skus
        
    from {{ ref('int_financial_performance') }}
    group by order_date, partner_channel, product_category, customer_segment
),

financial_trends as (
    select 
        *,
        -- Moving averages for trend analysis
        avg(daily_revenue) over (
            partition by partner_channel, product_category, customer_segment
            order by order_date
            rows between 6 preceding and current row
        ) as revenue_7_day_ma,
        
        avg(daily_gross_margin_rate) over (
            partition by partner_channel, product_category, customer_segment
            order by order_date
            rows between 6 preceding and current row
        ) as gross_margin_7_day_ma,
        
        -- Growth calculations
        lag(daily_revenue, 1) over (
            partition by partner_channel, product_category, customer_segment
            order by order_date
        ) as prev_day_revenue,
        
        lag(daily_gross_profit, 1) over (
            partition by partner_channel, product_category, customer_segment
            order by order_date
        ) as prev_day_gross_profit,
        
        -- Monthly comparisons
        lag(daily_revenue, 30) over (
            partition by partner_channel, product_category, customer_segment
            order by order_date
        ) as revenue_30_days_ago,
        
        -- Year-over-year comparisons
        lag(daily_revenue, 365) over (
            partition by partner_channel, product_category, customer_segment
            order by order_date
        ) as revenue_1_year_ago
        
    from financial_base
),

financial_insights as (
    select 
        *,
        -- Growth rates
        case 
            when prev_day_revenue > 0 then (daily_revenue - prev_day_revenue) / prev_day_revenue
            else null
        end as daily_revenue_growth_rate,
        
        case 
            when prev_day_gross_profit > 0 then (daily_gross_profit - prev_day_gross_profit) / prev_day_gross_profit
            else null
        end as daily_gross_profit_growth_rate,
        
        case 
            when revenue_30_days_ago > 0 then (daily_revenue - revenue_30_days_ago) / revenue_30_days_ago
            else null
        end as revenue_growth_30d,
        
        case 
            when revenue_1_year_ago > 0 then (daily_revenue - revenue_1_year_ago) / revenue_1_year_ago
            else null
        end as revenue_growth_yoy,
        
        -- Trend indicators
        case 
            when daily_revenue > revenue_7_day_ma * 1.1 then 'strong_growth'
            when daily_revenue > revenue_7_day_ma * 1.05 then 'moderate_growth'
            when daily_revenue < revenue_7_day_ma * 0.95 then 'declining'
            when daily_revenue < revenue_7_day_ma * 0.9 then 'strong_decline'
            else 'stable'
        end as revenue_trend_indicator,
        
        case 
            when daily_gross_margin_rate > gross_margin_7_day_ma * 1.05 then 'improving_margins'
            when daily_gross_margin_rate < gross_margin_7_day_ma * 0.95 then 'declining_margins'
            else 'stable_margins'
        end as margin_trend_indicator,
        
        -- Performance classification
        case 
            when daily_avg_performance_score >= 80 then 'excellent'
            when daily_avg_performance_score >= 60 then 'good'
            when daily_avg_performance_score >= 40 then 'needs_improvement'
            else 'poor'
        end as performance_classification,
        
        -- Health scores
        case 
            when total_skus > 0 then highly_profitable_skus * 1.0 / total_skus
            else 0
        end as highly_profitable_sku_rate,
        
        case 
            when total_skus > 0 then loss_making_skus * 1.0 / total_skus
            else 0
        end as loss_making_sku_rate,
        
        case 
            when total_skus > 0 then red_flag_skus * 1.0 / total_skus
            else 0
        end as red_flag_sku_rate,
        
        case 
            when total_skus > 0 then efficient_skus * 1.0 / total_skus
            else 0
        end as efficient_sku_rate,
        
        -- Strategic indicators
        case 
            when daily_revenue >= {{ var('revenue_target_monthly') }} / 30 then 'on_target'
            when daily_revenue >= {{ var('revenue_target_monthly') }} / 30 * 0.8 then 'near_target'
            else 'below_target'
        end as daily_target_status,
        
        -- Risk assessment
        case 
            when red_flag_sku_rate > 0.3 then 'high_risk'
            when red_flag_sku_rate > 0.15 then 'medium_risk'
            when red_flag_sku_rate > 0.05 then 'low_risk'
            else 'minimal_risk'
        end as financial_risk_level,
        
        -- Channel efficiency
        case 
            when efficient_sku_rate > 0.8 then 'highly_efficient'
            when efficient_sku_rate > 0.6 then 'efficient'
            when efficient_sku_rate > 0.4 then 'moderately_efficient'
            else 'inefficient'
        end as channel_efficiency_level
        
    from financial_trends
),

executive_summary as (
    select 
        {{ generate_surrogate_key(['order_date', 'partner_channel', 'product_category', 'customer_segment']) }} as financial_summary_key,
        *,
        
        -- Executive alerts
        case 
            when revenue_trend_indicator = 'strong_decline' and daily_revenue > 1000 then 'revenue_alert'
            when margin_trend_indicator = 'declining_margins' and daily_gross_margin_rate < 0.2 then 'margin_alert'
            when red_flag_sku_rate > 0.25 then 'quality_alert'
            when financial_risk_level = 'high_risk' then 'risk_alert'
            else 'normal'
        end as executive_alert_level,
        
        -- Action recommendations
        case 
            when performance_classification = 'poor' and daily_revenue > 500 then 'immediate_review_required'
            when loss_making_sku_rate > 0.2 then 'product_portfolio_review'
            when channel_efficiency_level = 'inefficient' then 'channel_optimization'
            when revenue_trend_indicator = 'strong_growth' then 'scale_opportunity'
            else 'monitor'
        end as recommended_action,
        
        -- Priority scoring (0-100)
        least(100, greatest(0,
            (daily_revenue / ({{ var('revenue_target_monthly') }} / 30) * 30) +
            (daily_gross_margin_rate * 25) +
            (efficient_sku_rate * 20) +
            (case when revenue_trend_indicator in ('strong_growth', 'moderate_growth') then 25 else 0 end)
        )) as priority_score,
        
        current_timestamp as summary_created_at
        
    from financial_insights
)

select * from executive_summary