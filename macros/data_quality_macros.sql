{% macro test_data_quality_score(model, column_name, threshold=0.95) %}
    
    with data_quality_check as (
        select 
            count(*) as total_records,
            count(case when {{ column_name }} is not null and {{ column_name }} != '' then 1 end) as valid_records,
            case 
                when count(*) = 0 then 0
                else count(case when {{ column_name }} is not null and {{ column_name }} != '' then 1 end) * 1.0 / count(*)
            end as data_quality_score
        from {{ model }}
    )
    
    select *
    from data_quality_check
    where data_quality_score < {{ threshold }}
    
{% endmacro %}

{% macro generate_surrogate_key(field_list) %}
    
    {{ dbt_utils.generate_surrogate_key(field_list) }}
    
{% endmacro %}

{% macro audit_helper(model_name) %}
    
    select 
        '{{ model_name }}' as model_name,
        count(*) as record_count,
        count(distinct {{ get_primary_key_column(model_name) }}) as unique_key_count,
        min({{ get_date_column(model_name) }}) as min_date,
        max({{ get_date_column(model_name) }}) as max_date,
        current_timestamp as audit_timestamp
    from {{ ref(model_name) }}
    
{% endmacro %}

{% macro get_primary_key_column(model_name) %}
    {% if model_name.startswith('fct_') %}
        {% set pk_column = model_name.replace('fct_', '') + '_key' %}
    {% elif model_name.startswith('dim_') %}
        {% set pk_column = model_name.replace('dim_', '') + '_key' %}
    {% else %}
        {% set pk_column = 'id' %}
    {% endif %}
    
    {{ return(pk_column) }}
{% endmacro %}

{% macro get_date_column(model_name) %}
    {% if model_name.startswith('fct_') %}
        {% set date_column = 'order_date' %}
    {% else %}
        {% set date_column = 'created_at' %}
    {% endif %}
    
    {{ return(date_column) }}
{% endmacro %}

{% macro calculate_business_days_between(start_date, end_date) %}
    
    with date_spine as (
        {{ dbt_utils.date_spine(
            datepart="day",
            start_date="cast('" ~ start_date ~ "' as date)",
            end_date="cast('" ~ end_date ~ "' as date)"
        ) }}
    ),
    
    business_days as (
        select 
            date_day,
            case 
                when extract(dow from date_day) in (0, 6) then 0  -- Sunday = 0, Saturday = 6
                else 1 
            end as is_business_day
        from date_spine
    )
    
    select sum(is_business_day) as business_days_count
    from business_days
    
{% endmacro %}

{% macro cents_to_dollars(amount_in_cents) %}
    
    ({{ amount_in_cents }} / 100.0)
    
{% endmacro %}

{% macro safe_divide(numerator, denominator) %}
    
    case 
        when {{ denominator }} = 0 or {{ denominator }} is null then null
        else {{ numerator }} * 1.0 / {{ denominator }}
    end
    
{% endmacro %}

{% macro pivot_table(column_name, value_column, agg_func='sum') %}
    
    {% set pivot_values_query %}
        select distinct {{ column_name }} as pivot_value
        from {{ this }}
        where {{ column_name }} is not null
        order by {{ column_name }}
    {% endset %}
    
    {% set results = run_query(pivot_values_query) %}
    {% if execute %}
        {% set pivot_values = results.columns[0].values() %}
    {% else %}
        {% set pivot_values = [] %}
    {% endif %}
    
    {% for value in pivot_values %}
        {{ agg_func }}(
            case when {{ column_name }} = '{{ value }}' then {{ value_column }} else 0 end
        ) as {{ value | replace(' ', '_') | replace('-', '_') | lower }}
        {%- if not loop.last -%},{%- endif -%}
    {% endfor %}
    
{% endmacro %}