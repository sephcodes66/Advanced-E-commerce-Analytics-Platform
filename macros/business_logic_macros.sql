{% macro calculate_customer_lifetime_value(customer_column, revenue_column, order_date_column, periods=12) %}
    
    with customer_metrics as (
        select 
            {{ customer_column }},
            count(distinct {{ order_date_column }}) as frequency,
            avg({{ revenue_column }}) as avg_order_value,
            sum({{ revenue_column }}) as total_revenue,
            min({{ order_date_column }}) as first_order_date,
            max({{ order_date_column }}) as last_order_date,
            datediff('day', min({{ order_date_column }}), max({{ order_date_column }})) as customer_lifespan_days
        from {{ this }}
        where {{ customer_column }} is not null
        group by {{ customer_column }}
    ),
    
    clv_calculation as (
        select 
            *,
            case 
                when customer_lifespan_days > 0 then
                    (frequency * avg_order_value * {{ periods }}) / (customer_lifespan_days / 30.0)
                else avg_order_value
            end as estimated_clv
        from customer_metrics
    )
    
    select * from clv_calculation
    
{% endmacro %}

{% macro calculate_cohort_analysis(user_id_column, order_date_column, revenue_column) %}
    
    with user_first_order as (
        select 
            {{ user_id_column }},
            min({{ order_date_column }}) as first_order_date,
            date_trunc('month', min({{ order_date_column }})) as cohort_month
        from {{ this }}
        group by {{ user_id_column }}
    ),
    
    user_orders as (
        select 
            o.{{ user_id_column }},
            o.{{ order_date_column }},
            o.{{ revenue_column }},
            f.cohort_month,
            datediff('month', f.first_order_date, o.{{ order_date_column }}) as period_number
        from {{ this }} o
        left join user_first_order f on o.{{ user_id_column }} = f.{{ user_id_column }}
    ),
    
    cohort_table as (
        select 
            cohort_month,
            period_number,
            count(distinct {{ user_id_column }}) as customers,
            sum({{ revenue_column }}) as revenue
        from user_orders
        group by cohort_month, period_number
    ),
    
    cohort_sizes as (
        select 
            cohort_month,
            customers as cohort_size
        from cohort_table
        where period_number = 0
    )
    
    select 
        c.cohort_month,
        c.period_number,
        c.customers,
        c.revenue,
        s.cohort_size,
        c.customers * 1.0 / s.cohort_size as retention_rate,
        c.revenue * 1.0 / s.cohort_size as revenue_per_cohort_user
    from cohort_table c
    left join cohort_sizes s on c.cohort_month = s.cohort_month
    
{% endmacro %}

{% macro calculate_partner_performance_score(revenue_column, order_count_column, customer_satisfaction_column=null) %}
    
    with performance_metrics as (
        select 
            sum({{ revenue_column }}) as total_revenue,
            sum({{ order_count_column }}) as total_orders,
            avg({{ revenue_column }}) as avg_order_value,
            {% if customer_satisfaction_column %}
                avg({{ customer_satisfaction_column }}) as avg_satisfaction
            {% else %}
                5.0 as avg_satisfaction  -- Default satisfaction score
            {% endif %}
        from {{ this }}
    ),
    
    normalized_metrics as (
        select 
            *,
            -- Revenue score (0-40 points)
            least(40, (total_revenue / {{ var('revenue_target_monthly') }}) * 40) as revenue_score,
            -- Order volume score (0-30 points)
            least(30, (total_orders / 1000) * 30) as volume_score,
            -- AOV score (0-20 points)
            least(20, (avg_order_value / {{ var('high_value_order_threshold') }}) * 20) as aov_score,
            -- Satisfaction score (0-10 points)
            (avg_satisfaction / 5.0) * 10 as satisfaction_score
        from performance_metrics
    )
    
    select 
        *,
        revenue_score + volume_score + aov_score + satisfaction_score as total_performance_score,
        case 
            when revenue_score + volume_score + aov_score + satisfaction_score >= 90 then 'A+'
            when revenue_score + volume_score + aov_score + satisfaction_score >= 80 then 'A'
            when revenue_score + volume_score + aov_score + satisfaction_score >= 70 then 'B'
            when revenue_score + volume_score + aov_score + satisfaction_score >= 60 then 'C'
            else 'D'
        end as performance_grade
    from normalized_metrics
    
{% endmacro %}

{% macro calculate_seasonal_adjustment(date_column, value_column, periods=4) %}
    
    with seasonal_data as (
        select 
            {{ date_column }},
            {{ value_column }},
            extract(quarter from {{ date_column }}) as quarter,
            extract(year from {{ date_column }}) as year
        from {{ this }}
    ),
    
    seasonal_averages as (
        select 
            quarter,
            avg({{ value_column }}) as avg_value_for_quarter
        from seasonal_data
        group by quarter
    ),
    
    overall_average as (
        select avg({{ value_column }}) as overall_avg
        from seasonal_data
    ),
    
    seasonal_factors as (
        select 
            quarter,
            avg_value_for_quarter / overall_avg as seasonal_factor
        from seasonal_averages
        cross join overall_average
    )
    
    select 
        d.{{ date_column }},
        d.{{ value_column }},
        d.quarter,
        sf.seasonal_factor,
        d.{{ value_column }} / sf.seasonal_factor as seasonally_adjusted_value
    from seasonal_data d
    left join seasonal_factors sf on d.quarter = sf.quarter
    
{% endmacro %}

{% macro attribution_model(touchpoint_column, conversion_column, attribution_type='first_touch') %}
    
    {% if attribution_type == 'first_touch' %}
        with first_touch as (
            select 
                {{ touchpoint_column }},
                {{ conversion_column }},
                row_number() over (partition by {{ conversion_column }} order by created_at) as touch_rank
            from {{ this }}
        )
        select * from first_touch where touch_rank = 1
        
    {% elif attribution_type == 'last_touch' %}
        with last_touch as (
            select 
                {{ touchpoint_column }},
                {{ conversion_column }},
                row_number() over (partition by {{ conversion_column }} order by created_at desc) as touch_rank
            from {{ this }}
        )
        select * from last_touch where touch_rank = 1
        
    {% elif attribution_type == 'linear' %}
        with linear_attribution as (
            select 
                {{ touchpoint_column }},
                {{ conversion_column }},
                count(*) over (partition by {{ conversion_column }}) as total_touches,
                1.0 / count(*) over (partition by {{ conversion_column }}) as attribution_weight
            from {{ this }}
        )
        select * from linear_attribution
        
    {% endif %}
    
{% endmacro %}