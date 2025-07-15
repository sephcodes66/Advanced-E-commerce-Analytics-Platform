{{
    config(
        materialized='table',
        tags=['marts', 'finance', 'cohort_analysis', 'customer_insights'],
        description='Advanced cohort analysis for customer lifecycle understanding and revenue forecasting'
    )
}}

with customer_first_orders as (
    select 
        order_id,
        customer_segment,
        partner_channel,
        min(order_date) as first_order_date,
        date_trunc('month', min(order_date)) as cohort_month,
        sum(revenue) as first_order_value,
        sum(quantity) as first_order_quantity
    from {{ ref('stg_partner_performance') }}
    group by order_id, customer_segment, partner_channel
),

customer_order_history as (
    select 
        p.order_id,
        p.customer_segment,
        p.partner_channel,
        p.order_date,
        p.revenue,
        p.quantity,
        p.product_category,
        p.order_value_tier,
        f.cohort_month,
        f.first_order_date,
        f.first_order_value,
        
        -- Calculate period number (months since first order)
        datediff('month', f.first_order_date, p.order_date) as period_number,
        
        -- Cumulative metrics
        sum(p.revenue) over (
            partition by p.order_id, p.customer_segment, p.partner_channel
            order by p.order_date
            rows unbounded preceding
        ) as cumulative_revenue,
        
        sum(p.quantity) over (
            partition by p.order_id, p.customer_segment, p.partner_channel
            order by p.order_date
            rows unbounded preceding
        ) as cumulative_quantity,
        
        -- Order sequence
        row_number() over (
            partition by p.order_id, p.customer_segment, p.partner_channel
            order by p.order_date
        ) as order_sequence
        
    from {{ ref('stg_partner_performance') }} p
    inner join customer_first_orders f 
        on p.order_id = f.order_id
        and p.customer_segment = f.customer_segment
        and p.partner_channel = f.partner_channel
),

cohort_metrics as (
    select 
        cohort_month,
        customer_segment,
        partner_channel,
        period_number,
        
        -- Customer metrics
        count(distinct order_id) as active_customers,
        sum(revenue) as period_revenue,
        sum(quantity) as period_quantity,
        
        -- Revenue metrics
        avg(revenue) as avg_revenue_per_customer,
        median(revenue) as median_revenue_per_customer,
        stddev(revenue) as revenue_stddev,
        
        -- Order behavior metrics
        avg(order_sequence) as avg_order_sequence,
        count(distinct case when order_value_tier = 'high_value' then order_id end) as high_value_customers,
        count(distinct case when order_sequence = 1 then order_id end) as first_time_customers,
        count(distinct case when order_sequence > 1 then order_id end) as repeat_customers,
        
        -- Product diversity
        count(distinct product_category) as unique_categories_purchased,
        
        -- First order cohort comparison
        avg(first_order_value) as avg_first_order_value,
        avg(case when period_number = 0 then revenue end) as avg_first_period_revenue,
        
        -- Cumulative metrics
        avg(cumulative_revenue) as avg_cumulative_revenue,
        avg(cumulative_quantity) as avg_cumulative_quantity
        
    from customer_order_history
    group by cohort_month, customer_segment, partner_channel, period_number
),

cohort_sizes as (
    select 
        cohort_month,
        customer_segment,
        partner_channel,
        active_customers as cohort_size,
        period_revenue as cohort_initial_revenue,
        avg_revenue_per_customer as cohort_initial_avg_revenue
    from cohort_metrics
    where period_number = 0
),

cohort_analysis as (
    select 
        c.cohort_month,
        c.customer_segment,
        c.partner_channel,
        c.period_number,
        c.active_customers,
        c.period_revenue,
        c.period_quantity,
        c.avg_revenue_per_customer,
        c.median_revenue_per_customer,
        c.avg_cumulative_revenue,
        c.high_value_customers,
        c.repeat_customers,
        c.unique_categories_purchased,
        
        -- Cohort size and initial metrics
        s.cohort_size,
        s.cohort_initial_revenue,
        s.cohort_initial_avg_revenue,
        
        -- Retention calculations
        c.active_customers * 1.0 / s.cohort_size as retention_rate,
        
        -- Revenue calculations
        c.period_revenue * 1.0 / s.cohort_initial_revenue as revenue_retention_rate,
        c.avg_revenue_per_customer / s.cohort_initial_avg_revenue as revenue_per_customer_ratio,
        
        -- Customer behavior insights
        c.high_value_customers * 1.0 / c.active_customers as high_value_customer_rate,
        c.repeat_customers * 1.0 / c.active_customers as repeat_customer_rate,
        
        -- Growth metrics
        lag(c.active_customers, 1) over (
            partition by c.cohort_month, c.customer_segment, c.partner_channel
            order by c.period_number
        ) as prev_period_customers,
        
        lag(c.period_revenue, 1) over (
            partition by c.cohort_month, c.customer_segment, c.partner_channel
            order by c.period_number
        ) as prev_period_revenue,
        
        -- Lifetime value progression
        c.avg_cumulative_revenue as ltv_at_period,
        
        -- Cohort quality indicators
        case 
            when c.period_number = 0 then 'initial'
            when c.period_number <= 3 then 'early_lifecycle'
            when c.period_number <= 12 then 'mature_lifecycle'
            else 'extended_lifecycle'
        end as lifecycle_stage,
        
        -- Cohort classification
        case 
            when c.period_number = 0 then s.cohort_size
            when c.period_number = 1 then c.active_customers * 1.0 / s.cohort_size
            else null
        end as month_1_retention_rate,
        
        case 
            when c.period_number = 0 then s.cohort_initial_revenue
            when c.period_number = 1 then c.period_revenue * 1.0 / s.cohort_initial_revenue
            else null
        end as month_1_revenue_retention_rate
        
    from cohort_metrics c
    inner join cohort_sizes s 
        on c.cohort_month = s.cohort_month
        and c.customer_segment = s.customer_segment
        and c.partner_channel = s.partner_channel
),

cohort_insights as (
    select 
        *,
        -- Customer churn analysis
        case 
            when prev_period_customers is not null and prev_period_customers > 0 then
                (prev_period_customers - active_customers) * 1.0 / prev_period_customers
            else 0
        end as churn_rate,
        
        -- Revenue growth analysis
        case 
            when prev_period_revenue is not null and prev_period_revenue > 0 then
                (period_revenue - prev_period_revenue) / prev_period_revenue
            else 0
        end as revenue_growth_rate,
        
        -- Cohort performance classification
        case 
            when retention_rate >= 0.8 then 'high_retention'
            when retention_rate >= 0.6 then 'medium_retention'
            when retention_rate >= 0.4 then 'low_retention'
            else 'high_churn'
        end as retention_classification,
        
        case 
            when revenue_retention_rate >= 1.2 then 'expansion_revenue'
            when revenue_retention_rate >= 0.8 then 'stable_revenue'
            when revenue_retention_rate >= 0.5 then 'declining_revenue'
            else 'significant_decline'
        end as revenue_classification,
        
        -- Lifetime value prediction
        case 
            when period_number >= 12 then ltv_at_period
            when period_number >= 6 then ltv_at_period * 1.5
            when period_number >= 3 then ltv_at_period * 2.0
            else ltv_at_period * 3.0
        end as predicted_ltv,
        
        -- Cohort quality score (0-100)
        least(100, greatest(0,
            (retention_rate * 40) +
            (revenue_retention_rate * 30) +
            (high_value_customer_rate * 20) +
            (repeat_customer_rate * 10)
        )) as cohort_quality_score,
        
        -- Strategic insights
        case 
            when cohort_month >= date_trunc('month', current_date - interval '3 months') then 'recent_cohort'
            when cohort_month >= date_trunc('month', current_date - interval '12 months') then 'mature_cohort'
            else 'historical_cohort'
        end as cohort_maturity,
        
        -- Business value assessment
        case 
            when cohort_size >= 100 and retention_rate >= 0.7 then 'high_value_cohort'
            when cohort_size >= 50 and retention_rate >= 0.5 then 'medium_value_cohort'
            when cohort_size >= 20 and retention_rate >= 0.3 then 'low_value_cohort'
            else 'monitor_cohort'
        end as business_value_assessment,
        
        -- Optimization recommendations
        case 
            when retention_classification = 'high_churn' and period_number <= 3 then 'improve_onboarding'
            when revenue_classification = 'declining_revenue' then 'implement_retention_campaigns'
            when high_value_customer_rate < 0.2 then 'focus_on_upselling'
            when repeat_customer_rate < 0.5 then 'improve_customer_experience'
            else 'maintain_current_strategy'
        end as optimization_recommendation
        
    from cohort_analysis
)

select 
    {{ generate_surrogate_key(['cohort_month', 'customer_segment', 'partner_channel', 'period_number']) }} as cohort_analysis_key,
    *,
    
    -- Final strategic classification
    case 
        when business_value_assessment = 'high_value_cohort' and retention_classification = 'high_retention' then 'star_cohort'
        when business_value_assessment = 'high_value_cohort' and retention_classification = 'medium_retention' then 'optimize_cohort'
        when business_value_assessment = 'medium_value_cohort' and retention_classification = 'high_retention' then 'scale_cohort'
        when retention_classification = 'high_churn' then 'at_risk_cohort'
        else 'standard_cohort'
    end as strategic_classification,
    
    -- Investment priority
    case 
        when strategic_classification = 'star_cohort' then 'maintain_excellence'
        when strategic_classification = 'at_risk_cohort' and cohort_size >= 50 then 'immediate_intervention'
        when strategic_classification = 'optimize_cohort' then 'targeted_optimization'
        when strategic_classification = 'scale_cohort' then 'growth_investment'
        else 'standard_management'
    end as investment_priority,
    
    -- Expected outcomes
    case 
        when optimization_recommendation = 'improve_onboarding' then 'increase_month_1_retention_by_15%'
        when optimization_recommendation = 'implement_retention_campaigns' then 'reduce_churn_by_20%'
        when optimization_recommendation = 'focus_on_upselling' then 'increase_ltv_by_25%'
        when optimization_recommendation = 'improve_customer_experience' then 'increase_repeat_rate_by_30%'
        else 'maintain_current_metrics'
    end as expected_outcome,
    
    current_timestamp as analysis_created_at
    
from cohort_insights
where cohort_month >= date_trunc('month', current_date - interval '{{ var("cohort_analysis_months") }}' MONTH)
order by cohort_month desc, customer_segment, partner_channel, period_number