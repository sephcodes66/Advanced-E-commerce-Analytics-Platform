-- Data Quality Framework Macros

-- Macro for comprehensive data quality checks
{% macro data_quality_check(table_name, primary_key=none, required_columns=[], numeric_columns=[], date_columns=[]) %}
    WITH quality_metrics AS (
        SELECT 
            '{{ table_name }}' as table_name,
            COUNT(*) as total_rows,
            
            -- Completeness checks
            {% for col in required_columns %}
            SUM(CASE WHEN {{ col }} IS NULL THEN 1 ELSE 0 END) as null_count_{{ col }},
            ROUND(100.0 * SUM(CASE WHEN {{ col }} IS NULL THEN 1 ELSE 0 END) / COUNT(*), 2) as null_rate_{{ col }},
            {% endfor %}
            
            -- Uniqueness checks
            {% if primary_key %}
            COUNT(DISTINCT {{ primary_key }}) as unique_{{ primary_key }},
            COUNT(*) - COUNT(DISTINCT {{ primary_key }}) as duplicate_count_{{ primary_key }},
            {% endif %}
            
            -- Data type consistency checks
            {% for col in numeric_columns %}
            SUM(CASE WHEN TRY_CAST({{ col }} AS DECIMAL) IS NULL AND {{ col }} IS NOT NULL THEN 1 ELSE 0 END) as invalid_numeric_{{ col }},
            {% endfor %}
            
            {% for col in date_columns %}
            SUM(CASE WHEN TRY_CAST({{ col }} AS DATE) IS NULL AND {{ col }} IS NOT NULL THEN 1 ELSE 0 END) as invalid_date_{{ col }},
            {% endfor %}
            
            -- Data freshness
            MAX(CASE WHEN {{ date_columns[0] }} IS NOT NULL THEN {{ date_columns[0] }} END) as max_date,
            DATEDIFF('hour', MAX(CASE WHEN {{ date_columns[0] }} IS NOT NULL THEN {{ date_columns[0] }} END), CURRENT_TIMESTAMP) as hours_since_last_update,
            
            -- Data distribution checks
            {% for col in numeric_columns %}
            AVG({{ col }}) as avg_{{ col }},
            STDDEV({{ col }}) as stddev_{{ col }},
            MIN({{ col }}) as min_{{ col }},
            MAX({{ col }}) as max_{{ col }},
            {% endfor %}
            
            -- Overall quality score
            ROUND(
                100.0 * (
                    1 - (
                        {% for col in required_columns %}
                        + (SUM(CASE WHEN {{ col }} IS NULL THEN 1 ELSE 0 END) * 1.0 / COUNT(*))
                        {% endfor %}
                        {% if primary_key %}
                        + ((COUNT(*) - COUNT(DISTINCT {{ primary_key }})) * 1.0 / COUNT(*))
                        {% endif %}
                    ) / {{ (required_columns | length) + (1 if primary_key else 0) }}
                ), 2
            ) as quality_score
            
        FROM {{ ref(table_name) }}
    )
    SELECT * FROM quality_metrics
{% endmacro %}

-- Macro for business rule validation
{% macro validate_business_rules(table_name, rules=[]) %}
    WITH business_validation AS (
        SELECT 
            '{{ table_name }}' as table_name,
            {% for rule in rules %}
            SUM(CASE WHEN NOT ({{ rule.condition }}) THEN 1 ELSE 0 END) as violations_{{ rule.name }},
            '{{ rule.description }}' as rule_{{ rule.name }}_description,
            {% endfor %}
            COUNT(*) as total_rows
        FROM {{ ref(table_name) }}
    )
    SELECT * FROM business_validation
{% endmacro %}

-- Macro for data lineage tracking
{% macro track_data_lineage(source_tables=[], transformation_type='unknown') %}
    SELECT 
        '{{ this }}' as target_table,
        {% for source in source_tables %}
        '{{ source }}' as source_table_{{ loop.index }},
        {% endfor %}
        '{{ transformation_type }}' as transformation_type,
        '{{ run_started_at }}' as processed_at,
        '{{ invocation_id }}' as run_id
{% endmacro %}

-- Macro for automated data profiling
{% macro profile_table(table_name, sample_size=10000) %}
    WITH sample_data AS (
        SELECT * FROM {{ ref(table_name) }}
        {% if sample_size %}
        TABLESAMPLE BERNOULLI({{ (sample_size * 100.0) / 1000000 }})
        {% endif %}
    ),
    column_stats AS (
        SELECT 
            '{{ table_name }}' as table_name,
            COUNT(*) as sample_size,
            
            -- Profile all columns dynamically
            {% set columns = adapter.get_columns_in_relation(ref(table_name)) %}
            {% for column in columns %}
            
            -- Basic stats for {{ column.name }}
            '{{ column.name }}' as column_{{ loop.index }}_name,
            '{{ column.dtype }}' as column_{{ loop.index }}_type,
            SUM(CASE WHEN {{ column.name }} IS NULL THEN 1 ELSE 0 END) as column_{{ loop.index }}_null_count,
            COUNT(DISTINCT {{ column.name }}) as column_{{ loop.index }}_distinct_count,
            
            {% if column.dtype in ['int', 'float', 'numeric', 'decimal'] %}
            -- Numeric column stats
            AVG({{ column.name }}) as column_{{ loop.index }}_avg,
            MIN({{ column.name }}) as column_{{ loop.index }}_min,
            MAX({{ column.name }}) as column_{{ loop.index }}_max,
            STDDEV({{ column.name }}) as column_{{ loop.index }}_stddev,
            {% endif %}
            
            {% if column.dtype in ['varchar', 'text', 'string'] %}
            -- String column stats
            AVG(LENGTH({{ column.name }})) as column_{{ loop.index }}_avg_length,
            MIN(LENGTH({{ column.name }})) as column_{{ loop.index }}_min_length,
            MAX(LENGTH({{ column.name }})) as column_{{ loop.index }}_max_length,
            {% endif %}
            
            {% if not loop.last %},{% endif %}
            {% endfor %}
            
        FROM sample_data
    )
    SELECT * FROM column_stats
{% endmacro %}

-- Macro for data drift detection
{% macro detect_data_drift(table_name, baseline_date, current_date, columns_to_monitor=[]) %}
    WITH baseline_stats AS (
        SELECT 
            {% for col in columns_to_monitor %}
            AVG({{ col }}) as baseline_avg_{{ col }},
            STDDEV({{ col }}) as baseline_stddev_{{ col }},
            MIN({{ col }}) as baseline_min_{{ col }},
            MAX({{ col }}) as baseline_max_{{ col }},
            {% endfor %}
            COUNT(*) as baseline_count
        FROM {{ ref(table_name) }}
        WHERE DATE(created_at) = '{{ baseline_date }}'
    ),
    current_stats AS (
        SELECT 
            {% for col in columns_to_monitor %}
            AVG({{ col }}) as current_avg_{{ col }},
            STDDEV({{ col }}) as current_stddev_{{ col }},
            MIN({{ col }}) as current_min_{{ col }},
            MAX({{ col }}) as current_max_{{ col }},
            {% endfor %}
            COUNT(*) as current_count
        FROM {{ ref(table_name) }}
        WHERE DATE(created_at) = '{{ current_date }}'
    ),
    drift_analysis AS (
        SELECT 
            '{{ table_name }}' as table_name,
            '{{ baseline_date }}' as baseline_date,
            '{{ current_date }}' as current_date,
            
            {% for col in columns_to_monitor %}
            -- Statistical drift detection for {{ col }}
            ABS(c.current_avg_{{ col }} - b.baseline_avg_{{ col }}) / NULLIF(b.baseline_stddev_{{ col }}, 0) as drift_zscore_{{ col }},
            CASE 
                WHEN ABS(c.current_avg_{{ col }} - b.baseline_avg_{{ col }}) / NULLIF(b.baseline_stddev_{{ col }}, 0) > 2 THEN 'HIGH'
                WHEN ABS(c.current_avg_{{ col }} - b.baseline_avg_{{ col }}) / NULLIF(b.baseline_stddev_{{ col }}, 0) > 1 THEN 'MEDIUM'
                ELSE 'LOW'
            END as drift_severity_{{ col }},
            {% endfor %}
            
            -- Volume drift
            ABS(c.current_count - b.baseline_count) * 100.0 / b.baseline_count as volume_drift_percent,
            CASE 
                WHEN ABS(c.current_count - b.baseline_count) * 100.0 / b.baseline_count > 20 THEN 'HIGH'
                WHEN ABS(c.current_count - b.baseline_count) * 100.0 / b.baseline_count > 10 THEN 'MEDIUM'
                ELSE 'LOW'
            END as volume_drift_severity
            
        FROM baseline_stats b
        CROSS JOIN current_stats c
    )
    SELECT * FROM drift_analysis
{% endmacro %}

-- Macro for automated data quality monitoring
{% macro monitor_data_quality(table_name, alert_threshold=0.8) %}
    WITH quality_check AS (
        {{ data_quality_check(table_name, 
                             primary_key='id', 
                             required_columns=['customer_id', 'product_id', 'order_date'],
                             numeric_columns=['amount', 'quantity'],
                             date_columns=['order_date', 'created_at']) }}
    ),
    alerts AS (
        SELECT 
            *,
            CASE 
                WHEN quality_score < {{ alert_threshold * 100 }} THEN 'CRITICAL'
                WHEN quality_score < {{ (alert_threshold + 0.1) * 100 }} THEN 'WARNING'
                ELSE 'HEALTHY'
            END as alert_level,
            
            CASE 
                WHEN quality_score < {{ alert_threshold * 100 }} THEN 'Data quality below threshold'
                WHEN hours_since_last_update > {{ var('freshness_threshold_hours') }} THEN 'Data freshness issue'
                ELSE 'No issues detected'
            END as alert_message
            
        FROM quality_check
    )
    SELECT * FROM alerts
{% endmacro %}

-- Macro for data quality dashboard metrics
{% macro quality_dashboard_metrics() %}
    WITH table_metrics AS (
        -- Union all quality checks across tables
        SELECT 'staging_amazon_sales' as table_name, quality_score, total_rows, hours_since_last_update
        FROM {{ ref('staging_amazon_sales_quality') }}
        
        UNION ALL
        
        SELECT 'staging_international_sales' as table_name, quality_score, total_rows, hours_since_last_update  
        FROM {{ ref('staging_international_sales_quality') }}
        
        UNION ALL
        
        SELECT 'marts_customer_analytics' as table_name, quality_score, total_rows, hours_since_last_update
        FROM {{ ref('marts_customer_analytics_quality') }}
    ),
    summary_metrics AS (
        SELECT 
            COUNT(*) as total_tables,
            AVG(quality_score) as avg_quality_score,
            MIN(quality_score) as min_quality_score,
            SUM(total_rows) as total_rows_monitored,
            MAX(hours_since_last_update) as max_hours_since_update,
            SUM(CASE WHEN quality_score < 80 THEN 1 ELSE 0 END) as tables_with_issues,
            SUM(CASE WHEN hours_since_last_update > {{ var('freshness_threshold_hours') }} THEN 1 ELSE 0 END) as stale_tables
        FROM table_metrics
    )
    SELECT 
        *,
        ROUND(100.0 * tables_with_issues / total_tables, 2) as quality_issue_rate,
        ROUND(100.0 * stale_tables / total_tables, 2) as freshness_issue_rate,
        CASE 
            WHEN avg_quality_score >= 95 THEN 'EXCELLENT'
            WHEN avg_quality_score >= 90 THEN 'GOOD'
            WHEN avg_quality_score >= 80 THEN 'FAIR'
            ELSE 'POOR'
        END as overall_health_status
    FROM summary_metrics
{% endmacro %}