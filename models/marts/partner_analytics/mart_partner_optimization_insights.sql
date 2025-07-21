{{
    config(
        materialized='table',
        tags=['marts', 'partner_analytics', 'optimization', 'insights'],
        description='Advanced partner optimization insights with AI-driven recommendations for ZMS Analytics Engineering team'
    )
}}

with partner_performance_base as (
    select 
        partner_channel,
        customer_segment,
        
        -- Recent 30-day performance
        revenue_30d,
        orders_30d,
        avg_order_value_30d,
        gross_profit_30d,
        avg_gross_margin_30d,
        
        -- 30-day growth metrics
        revenue_growth_30d,
        order_growth_30d,
        
        -- 30-day operational efficiency
        avg_fulfilment_efficiency_30d,
        avg_data_quality_30d,
        avg_processing_days_30d,
        
        -- Key performance indicators
        overall_trend_direction,
        overall_health_score,
        efficiency_rating,
        monthly_target_status,
        partner_priority_rank,
        
        -- 30-day quality metrics
        high_value_order_rate_30d,
        red_flag_days,
        efficient_days
        
    from {{ ref('mart_partner_performance_dashboard') }}
),

partner_benchmarks as (
    select 
        customer_segment,
        
        -- Average performance benchmarks
        avg(revenue_30d) as avg_revenue_benchmark,
        avg(orders_30d) as avg_orders_benchmark,
        avg(avg_order_value_30d) as avg_aov_benchmark,
        avg(avg_gross_margin_30d) as avg_margin_benchmark,
        avg(avg_fulfilment_efficiency_30d) as avg_efficiency_benchmark,
        avg(overall_health_score) as avg_health_score_benchmark,
        
        -- Top 25% performance benchmarks
        percentile_cont(0.75) within group (order by revenue_30d) as top_revenue_benchmark,
        percentile_cont(0.75) within group (order by avg_order_value_30d) as top_aov_benchmark,
        percentile_cont(0.75) within group (order by avg_gross_margin_30d) as top_margin_benchmark,
        percentile_cont(0.75) within group (order by overall_health_score) as top_health_benchmark,
        
        -- Lower 25% performance thresholds
        percentile_cont(0.25) within group (order by revenue_30d) as low_revenue_threshold,
        percentile_cont(0.25) within group (order by avg_gross_margin_30d) as low_margin_threshold,
        percentile_cont(0.25) within group (order by overall_health_score) as low_health_threshold
        
    from partner_performance_base
    group by customer_segment
),

optimization_analysis as (
    select 
        p.*,
        b.avg_revenue_benchmark,
        b.avg_orders_benchmark,
        b.avg_aov_benchmark,
        b.avg_margin_benchmark,
        b.avg_efficiency_benchmark,
        b.avg_health_score_benchmark,
        b.top_revenue_benchmark,
        b.top_aov_benchmark,
        b.top_margin_benchmark,
        b.top_health_benchmark,
        
        -- Gaps against average performance
        p.revenue_30d - b.avg_revenue_benchmark as revenue_gap_vs_avg,
        p.avg_order_value_30d - b.avg_aov_benchmark as aov_gap_vs_avg,
        p.avg_gross_margin_30d - b.avg_margin_benchmark as margin_gap_vs_avg,
        p.overall_health_score - b.avg_health_score_benchmark as health_gap_vs_avg,
        
        -- Potential for improvement
        b.top_revenue_benchmark - p.revenue_30d as revenue_upside_potential,
        b.top_aov_benchmark - p.avg_order_value_30d as aov_upside_potential,
        b.top_margin_benchmark - p.avg_gross_margin_30d as margin_upside_potential,
        b.top_health_benchmark - p.overall_health_score as health_upside_potential,
        
        -- Performance rank within customer segment
        percent_rank() over (partition by p.customer_segment order by p.revenue_30d) as revenue_percentile,
        percent_rank() over (partition by p.customer_segment order by p.avg_order_value_30d) as aov_percentile,
        percent_rank() over (partition by p.customer_segment order by p.avg_gross_margin_30d) as margin_percentile,
        percent_rank() over (partition by p.customer_segment order by p.overall_health_score) as health_percentile,
        
        -- Opportunity level for improvement
        case 
            when p.revenue_30d < b.low_revenue_threshold then 'high_opportunity'
            when p.revenue_30d < b.avg_revenue_benchmark then 'medium_opportunity'
            when p.revenue_30d < b.top_revenue_benchmark then 'optimization_opportunity'
            else 'maintain_performance'
        end as revenue_opportunity_level,
        
        case 
            when p.avg_gross_margin_30d < b.low_margin_threshold then 'high_opportunity'
            when p.avg_gross_margin_30d < b.avg_margin_benchmark then 'medium_opportunity'
            when p.avg_gross_margin_30d < b.top_margin_benchmark then 'optimization_opportunity'
            else 'maintain_performance'
        end as margin_opportunity_level,
        
        case 
            when p.overall_health_score < b.low_health_threshold then 'high_opportunity'
            when p.overall_health_score < b.avg_health_score_benchmark then 'medium_opportunity'
            when p.overall_health_score < b.top_health_benchmark then 'optimization_opportunity'
            else 'maintain_performance'
        end as health_opportunity_level
        
    from partner_performance_base p
    left join partner_benchmarks b on p.customer_segment = b.customer_segment
),

strategic_insights as (
    select 
        *,
        -- Partner's strategic category
        case 
            when revenue_percentile >= 0.8 and margin_percentile >= 0.8 then 'star_performer'
            when revenue_percentile >= 0.6 and health_percentile >= 0.6 then 'solid_performer'
            when revenue_percentile >= 0.4 or health_percentile >= 0.4 then 'potential_improver'
            when revenue_percentile >= 0.2 or health_percentile >= 0.2 then 'needs_attention'
            else 'underperformer'
        end as strategic_classification,
        
        -- Recommended investment level
        case 
            when revenue_upside_potential > 50000 and margin_upside_potential > 0.1 then 'high_investment'
            when revenue_upside_potential > 20000 and margin_upside_potential > 0.05 then 'medium_investment'
            when revenue_upside_potential > 5000 then 'low_investment'
            else 'minimal_investment'
        end as investment_priority,
        
        -- Primary area for optimization
        case 
            when avg_order_value_30d < avg_aov_benchmark * 0.8 then 'aov_optimization'
            when avg_gross_margin_30d < avg_margin_benchmark * 0.8 then 'margin_optimization'
            when avg_fulfilment_efficiency_30d < avg_efficiency_benchmark * 0.9 then 'operational_optimization'
            when red_flag_days > 5 then 'quality_optimization'
            else 'growth_optimization'
        end as primary_optimization_focus,
        
        -- Actionable recommendations
        case 
            -- Strategies to grow revenue
            when revenue_gap_vs_avg < -10000 and aov_gap_vs_avg < -50 then 'implement_upselling_cross_selling'
            when revenue_gap_vs_avg < -10000 and order_growth_30d < 0.1 then 'increase_marketing_investment'
            when revenue_gap_vs_avg < -5000 and high_value_order_rate_30d < 0.3 then 'target_high_value_customers'
            
            -- Strategies to improve profit margins
            when margin_gap_vs_avg < -0.1 and avg_processing_days_30d > 5 then 'improve_operational_efficiency'
            when margin_gap_vs_avg < -0.1 and partner_channel = 'amazon' then 'optimize_amazon_fees'
            when margin_gap_vs_avg < -0.05 then 'negotiate_better_supplier_terms'
            
            -- Strategies to improve operations
            when avg_fulfilment_efficiency_30d < 85 then 'upgrade_fulfillment_processes'
            when avg_data_quality_30d < 0.95 then 'implement_data_quality_controls'
            when red_flag_days > 3 then 'address_quality_issues'
            
            -- Strategies to accelerate growth
            when strategic_classification = 'star_performer' then 'scale_successful_practices'
            when overall_trend_direction = 'positive_trend' then 'accelerate_growth_initiatives'
            
            else 'maintain_current_operations'
        end as specific_recommendation,
        
        -- Potential impact of recommendations
        case 
            when revenue_upside_potential > 100000 then 'high_impact'
            when revenue_upside_potential > 50000 then 'medium_impact'
            when revenue_upside_potential > 10000 then 'low_impact'
            else 'minimal_impact'
        end as expected_impact,
        
        -- Estimated complexity of implementation
        case 
            when primary_optimization_focus = 'quality_optimization' then 'low_complexity'
            when primary_optimization_focus = 'aov_optimization' then 'medium_complexity'
            when primary_optimization_focus = 'operational_optimization' then 'high_complexity'
            when primary_optimization_focus = 'margin_optimization' then 'high_complexity'
            else 'medium_complexity'
        end as implementation_complexity,
        
        -- Required resources for implementation
        case 
            when investment_priority = 'high_investment' then 'significant_resources'
            when investment_priority = 'medium_investment' then 'moderate_resources'
            when investment_priority = 'low_investment' then 'minimal_resources'
            else 'maintenance_resources'
        end as resource_requirements,
        
        -- Recommended timeline for implementation
        case 
            when strategic_classification = 'underperformer' then 'immediate_action'
            when expected_impact = 'high_impact' then 'priority_implementation'
            when implementation_complexity = 'low_complexity' then 'quick_wins'
            else 'standard_timeline'
        end as implementation_timeline,
        
        -- Key metrics to measure success
        case 
            when primary_optimization_focus = 'aov_optimization' then 'increase_avg_order_value_by_20%'
            when primary_optimization_focus = 'margin_optimization' then 'improve_gross_margin_by_5pp'
            when primary_optimization_focus = 'operational_optimization' then 'reduce_processing_time_by_2_days'
            when primary_optimization_focus = 'quality_optimization' then 'achieve_99%_data_quality'
            else 'increase_revenue_by_15%'
        end as success_metrics,
        
        -- Assessed level of risk
        case 
            when strategic_classification = 'underperformer' and revenue_30d > 50000 then 'high_risk'
            when overall_trend_direction = 'negative_trend' and partner_priority_rank <= 3 then 'medium_risk'
            when red_flag_days > 10 then 'operational_risk'
            else 'low_risk'
        end as risk_level
        
    from optimization_analysis
)

select 
    {{ generate_surrogate_key(['partner_channel', 'customer_segment']) }} as optimization_key,
    *,
    
    -- Overall optimization priority score
    least(100, greatest(0,
        (case when expected_impact = 'high_impact' then 30 
              when expected_impact = 'medium_impact' then 20
              when expected_impact = 'low_impact' then 10 else 0 end) +
        (case when implementation_complexity = 'low_complexity' then 25
              when implementation_complexity = 'medium_complexity' then 15
              when implementation_complexity = 'high_complexity' then 5 else 10 end) +
        (case when strategic_classification = 'underperformer' then 25
              when strategic_classification = 'needs_attention' then 20
              when strategic_classification = 'potential_improver' then 15 else 10 end) +
        (case when risk_level = 'high_risk' then 20
              when risk_level = 'medium_risk' then 15
              when risk_level = 'operational_risk' then 10 else 5 end)
    )) as optimization_priority_score,
    
    -- High-level summary for executives
    case 
        when strategic_classification = 'star_performer' then 'Maintain excellence and scale best practices'
        when strategic_classification = 'underperformer' then 'Immediate intervention required'
        when expected_impact = 'high_impact' and implementation_complexity = 'low_complexity' then 'High-value quick win opportunity'
        when risk_level = 'high_risk' then 'Risk mitigation is priority'
        else 'Standard optimization opportunity'
    end as executive_summary,
    
    -- Insights specific to ZMS
    case 
        when partner_channel = 'amazon' and margin_gap_vs_avg < -0.05 then 'Review Amazon fee structure and pricing strategy'
        when customer_segment = 'b2b' and aov_gap_vs_avg < -100 then 'Implement B2B-specific value propositions'
        when overall_trend_direction = 'negative_trend' then 'Investigate competitive pressures and market changes'
        when efficiency_rating = 'needs_improvement' then 'Leverage MOTHERSHIP platform capabilities'
        else 'Continue current optimization initiatives'
    end as zms_specific_insight,
    
    current_timestamp as insights_generated_at
    
from strategic_insights
order by optimization_priority_score desc, revenue_upside_potential desc