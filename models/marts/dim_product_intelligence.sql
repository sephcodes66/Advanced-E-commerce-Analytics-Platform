{{
    config(
        materialized='table',
        tags=['marts', 'dimension', 'product_intelligence'],
        description='Enhanced product dimension with AI-driven insights and performance analytics'
    )
}}

with product_base as (
    select 
        product_sku,
        product_category,
        product_style,
        product_asin,
        
        -- Sales performance metrics
        count(distinct order_id) as total_orders,
        sum(quantity) as total_units_sold,
        sum(revenue) as total_revenue,
        count(distinct order_date) as active_sales_days,
        count(distinct partner_channel) as channel_presence,
        
        -- Time-based metrics
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        datediff('day', min(order_date), max(order_date)) as product_lifespan_days,
        
        -- Customer metrics
        count(distinct case when customer_segment = 'b2b' then order_id end) as b2b_orders,
        count(distinct case when customer_segment = 'b2c' then order_id end) as b2c_orders,
        count(distinct case when is_b2b then order_id end) as business_orders,
        
        -- Channel distribution
        sum(case when partner_channel = 'amazon' then revenue else 0 end) as amazon_revenue,
        sum(case when partner_channel = 'merchant_website' then revenue else 0 end) as merchant_revenue,
        sum(case when partner_channel = 'international' then revenue else 0 end) as international_revenue,
        
        -- Order value distribution
        count(case when order_value_tier = 'high_value' then 1 end) as high_value_orders,
        count(case when order_value_tier = 'medium_value' then 1 end) as medium_value_orders,
        count(case when order_value_tier = 'low_value' then 1 end) as low_value_orders,
        
        -- Seasonal patterns
        sum(case when quarter = 1 then revenue else 0 end) as q1_revenue,
        sum(case when quarter = 2 then revenue else 0 end) as q2_revenue,
        sum(case when quarter = 3 then revenue else 0 end) as q3_revenue,
        sum(case when quarter = 4 then revenue else 0 end) as q4_revenue,
        
        -- Performance metrics
        avg(revenue) as avg_order_value,
        avg(quantity) as avg_units_per_order,
        stddev(revenue) as revenue_volatility,
        avg(fulfilment_efficiency_score) as avg_fulfilment_score,
        avg(estimated_processing_days) as avg_processing_days
        
    from {{ ref('stg_partner_performance') }}
    where product_sku is not null
    group by product_sku, product_category, product_style, product_asin
),

cost_intelligence as (
    select 
        product_sku,
        avg(cost_price_tp1) as avg_cost_price,
        avg(cost_price_tp2) as avg_cost_price_alt,
        avg(final_mrp_old) as avg_cost_price_premium,
        stddev(cost_price_tp1) as cost_price_volatility,
        
        -- Cost tier classification
        case 
            when avg(cost_price_tp1) <= 50 then 'low_cost'
            when avg(cost_price_tp1) <= 200 then 'medium_cost'
            else 'high_cost'
        end as cost_tier,
        
        -- Cost stability
        case 
            when stddev(cost_price_tp1) / avg(cost_price_tp1) < 0.1 then 'stable_cost'
            when stddev(cost_price_tp1) / avg(cost_price_tp1) < 0.3 then 'moderate_cost_variation'
            else 'volatile_cost'
        end as cost_stability
        
    from {{ ref('stg_pl_march_2021') }}
    where cost_price_tp1 is not null and cost_price_tp1 > 0
    group by product_sku
),

expense_intelligence as (
    select 
        product_sku,
        sum(expense_amount) as total_allocated_expenses,
        avg(expense_amount) as avg_expense_per_transaction,
        count(*) as expense_transaction_count,
        
        -- Expense patterns
        case 
            when avg(expense_amount) <= 10 then 'low_expense'
            when avg(expense_amount) <= 50 then 'medium_expense'
            else 'high_expense'
        end as expense_tier
        
    from {{ ref('stg_expense_iigf') }}
    where expense_amount is not null
    group by product_sku
),

product_analytics as (
    select 
        p.*,
        
        -- Cost and expense data
        coalesce(c.avg_cost_price, 0) as unit_cost,
        coalesce(c.avg_cost_price_alt, c.avg_cost_price, 0) as unit_cost_alternative,
        coalesce(c.cost_tier, 'unknown') as cost_tier,
        coalesce(c.cost_stability, 'unknown') as cost_stability,
        coalesce(e.total_allocated_expenses, 0) as total_allocated_expenses,
        coalesce(e.avg_expense_per_transaction, 0) as avg_expense_per_transaction,
        coalesce(e.expense_tier, 'unknown') as expense_tier,
        
        -- Profitability calculations
        p.total_revenue - (coalesce(c.avg_cost_price, 0) * p.total_units_sold) as gross_profit,
        p.total_revenue - (coalesce(c.avg_cost_price, 0) * p.total_units_sold) - coalesce(e.total_allocated_expenses, 0) as net_profit,
        
        -- Margin calculations
        case 
            when p.total_revenue > 0 then
                (p.total_revenue - (coalesce(c.avg_cost_price, 0) * p.total_units_sold)) / p.total_revenue
            else 0
        end as gross_margin_rate,
        
        case 
            when p.total_revenue > 0 then
                (p.total_revenue - (coalesce(c.avg_cost_price, 0) * p.total_units_sold) - coalesce(e.total_allocated_expenses, 0)) / p.total_revenue
            else 0
        end as net_margin_rate,
        
        -- Unit economics
        case 
            when p.total_units_sold > 0 then
                (p.total_revenue - (coalesce(c.avg_cost_price, 0) * p.total_units_sold)) / p.total_units_sold
            else 0
        end as gross_profit_per_unit,
        
        -- Performance metrics
        case 
            when p.product_lifespan_days > 0 then
                p.total_revenue / p.product_lifespan_days
            else 0
        end as daily_revenue_rate,
        
        case 
            when p.product_lifespan_days > 0 then
                p.total_units_sold / p.product_lifespan_days
            else 0
        end as daily_sales_velocity,
        
        -- Channel performance
        case 
            when p.total_revenue > 0 then p.amazon_revenue / p.total_revenue
            else 0
        end as amazon_revenue_share,
        
        case 
            when p.total_revenue > 0 then p.merchant_revenue / p.total_revenue
            else 0
        end as merchant_revenue_share,
        
        case 
            when p.total_revenue > 0 then p.international_revenue / p.total_revenue
            else 0
        end as international_revenue_share,
        
        -- Customer segment performance
        case 
            when p.total_orders > 0 then p.b2b_orders * 1.0 / p.total_orders
            else 0
        end as b2b_order_rate,
        
        case 
            when p.total_orders > 0 then p.business_orders * 1.0 / p.total_orders
            else 0
        end as business_order_rate,
        
        -- Seasonal indicators
        case 
            when greatest(p.q1_revenue, p.q2_revenue, p.q3_revenue, p.q4_revenue) = p.q4_revenue then 'q4_peak'
            when greatest(p.q1_revenue, p.q2_revenue, p.q3_revenue, p.q4_revenue) = p.q3_revenue then 'q3_peak'
            when greatest(p.q1_revenue, p.q2_revenue, p.q3_revenue, p.q4_revenue) = p.q2_revenue then 'q2_peak'
            when greatest(p.q1_revenue, p.q2_revenue, p.q3_revenue, p.q4_revenue) = p.q1_revenue then 'q1_peak'
            else 'no_clear_seasonality'
        end as seasonal_peak,
        
        -- Seasonal coefficient of variation
        case 
            when (p.q1_revenue + p.q2_revenue + p.q3_revenue + p.q4_revenue) > 0 then
                sqrt(power(p.q1_revenue - (p.q1_revenue + p.q2_revenue + p.q3_revenue + p.q4_revenue) / 4, 2) +
                     power(p.q2_revenue - (p.q1_revenue + p.q2_revenue + p.q3_revenue + p.q4_revenue) / 4, 2) +
                     power(p.q3_revenue - (p.q1_revenue + p.q2_revenue + p.q3_revenue + p.q4_revenue) / 4, 2) +
                     power(p.q4_revenue - (p.q1_revenue + p.q2_revenue + p.q3_revenue + p.q4_revenue) / 4, 2)) / 
                ((p.q1_revenue + p.q2_revenue + p.q3_revenue + p.q4_revenue) / 4)
            else 0
        end as seasonal_variability
        
    from product_base p
    left join cost_intelligence c using(product_sku)
    left join expense_intelligence e using(product_sku)
),

product_classification as (
    select 
        *,
        -- Performance classification
        case 
            when gross_margin_rate >= 0.4 then 'high_margin'
            when gross_margin_rate >= 0.25 then 'medium_margin'
            when gross_margin_rate >= 0.1 then 'low_margin'
            else 'negative_margin'
        end as margin_category,
        
        -- Sales velocity classification
        case 
            when daily_sales_velocity >= 10 then 'fast_moving'
            when daily_sales_velocity >= 5 then 'medium_velocity'
            when daily_sales_velocity >= 1 then 'slow_moving'
            else 'very_slow_moving'
        end as velocity_category,
        
        -- Revenue contribution
        case 
            when total_revenue >= 50000 then 'star_product'
            when total_revenue >= 20000 then 'key_product'
            when total_revenue >= 5000 then 'supporting_product'
            when total_revenue >= 1000 then 'niche_product'
            else 'low_performer'
        end as revenue_tier,
        
        -- Channel strategy
        case 
            when amazon_revenue_share >= 0.7 then 'amazon_focused'
            when merchant_revenue_share >= 0.7 then 'merchant_focused'
            when international_revenue_share >= 0.3 then 'international_opportunity'
            else 'multi_channel'
        end as channel_strategy,
        
        -- Customer segment focus
        case 
            when b2b_order_rate >= 0.7 then 'b2b_focused'
            when b2b_order_rate >= 0.3 then 'mixed_segment'
            else 'b2c_focused'
        end as customer_focus,
        
        -- Seasonality classification
        case 
            when seasonal_variability >= 0.8 then 'highly_seasonal'
            when seasonal_variability >= 0.4 then 'moderately_seasonal'
            when seasonal_variability >= 0.2 then 'slightly_seasonal'
            else 'non_seasonal'
        end as seasonality_level,
        
        -- Product lifecycle stage
        case 
            when last_sale_date < current_date - interval '90 days' then 'declining'
            when last_sale_date < current_date - interval '30 days' then 'mature'
            when product_lifespan_days <= 30 then 'new_product'
            when daily_revenue_rate > 100 then 'growth'
            else 'stable'
        end as lifecycle_stage,
        
        -- Investment priority
        case 
            when revenue_tier = 'star_product' and margin_category = 'high_margin' then 'invest_heavily'
            when revenue_tier in ('star_product', 'key_product') and margin_category != 'negative_margin' then 'maintain_investment'
            when margin_category = 'negative_margin' and revenue_tier not in ('star_product', 'key_product') then 'divest'
            when velocity_category = 'very_slow_moving' and margin_category = 'low_margin' then 'review_for_discontinuation'
            else 'standard_investment'
        end as investment_recommendation,
        
        -- Marketing strategy
        case 
            when seasonality_level = 'highly_seasonal' then 'seasonal_marketing'
            when channel_strategy = 'amazon_focused' then 'amazon_optimization'
            when customer_focus = 'b2b_focused' then 'b2b_marketing'
            when velocity_category = 'fast_moving' then 'scale_marketing'
            when lifecycle_stage = 'new_product' then 'awareness_building'
            else 'standard_marketing'
        end as marketing_strategy
        
    from product_analytics
)

select 
    {{ generate_surrogate_key(['product_sku']) }} as product_intelligence_key,
    *,
    
    -- Composite scores
    least(100, greatest(0, 
        (case when gross_margin_rate > 0 then gross_margin_rate * 30 else 0 end) +
        (case when daily_sales_velocity > 0 then least(25, daily_sales_velocity * 2.5) else 0 end) +
        (case when total_revenue > 0 then least(25, total_revenue / 2000) else 0 end) +
        (case when channel_presence >= 2 then 10 else channel_presence * 5 end) +
        (case when lifecycle_stage in ('growth', 'stable') then 10 else 0 end)
    )) as product_score,
    
    -- Strategic classification
    case 
        when revenue_tier = 'star_product' and margin_category = 'high_margin' then 'cash_cow'
        when revenue_tier in ('star_product', 'key_product') and lifecycle_stage = 'growth' then 'rising_star'
        when margin_category = 'negative_margin' or lifecycle_stage = 'declining' then 'sunset_product'
        when velocity_category = 'very_slow_moving' and revenue_tier = 'low_performer' then 'question_mark'
        else 'core_product'
    end as strategic_classification,
    
    -- Next best action
    case 
        when strategic_classification = 'cash_cow' then 'maximize_profitability'
        when strategic_classification = 'rising_star' then 'invest_for_growth'
        when strategic_classification = 'sunset_product' then 'phase_out_planning'
        when strategic_classification = 'question_mark' then 'evaluate_potential'
        else 'optimize_operations'
    end as recommended_action,
    
    current_timestamp as intelligence_updated_at
    
from product_classification
order by product_score desc, total_revenue desc