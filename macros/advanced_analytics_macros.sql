-- Macros for advanced e-commerce analytics

-- Calculates and segments customer lifetime value (LTV)
{% macro calculate_customer_ltv(customer_id_column, revenue_column, date_column, prediction_months=12) %}
    WITH customer_metrics AS (
        SELECT 
            {{ customer_id_column }},
            COUNT(DISTINCT {{ date_column }}) as purchase_frequency,
            AVG({{ revenue_column }}) as avg_order_value,
            SUM({{ revenue_column }}) as total_revenue,
            MIN({{ date_column }}) as first_purchase,
            MAX({{ date_column }}) as last_purchase,
            DATEDIFF('day', MIN({{ date_column }}), MAX({{ date_column }})) as customer_lifespan_days
        FROM {{ this }}
        GROUP BY {{ customer_id_column }}
    ),
    ltv_calculation AS (
        SELECT 
            *,
            CASE 
                WHEN customer_lifespan_days > 0 
                THEN (total_revenue / customer_lifespan_days) * 365 * ({{ prediction_months }} / 12.0)
                ELSE avg_order_value * purchase_frequency * ({{ prediction_months }} / 12.0)
            END as predicted_ltv,
            CASE 
                WHEN predicted_ltv > {{ var('customer_ltv_threshold') }} THEN 'High Value'
                WHEN predicted_ltv > {{ var('customer_ltv_threshold') }} * 0.5 THEN 'Medium Value'
                ELSE 'Low Value'
            END as ltv_segment
        FROM customer_metrics
    )
    SELECT * FROM ltv_calculation
{% endmacro %}

-- Performs cohort analysis with retention rates
{% macro cohort_analysis(user_id_column, date_column, revenue_column=none) %}
    WITH cohort_data AS (
        SELECT 
            {{ user_id_column }},
            {{ date_column }},
            DATE_TRUNC('month', {{ date_column }}) as cohort_month,
            DATE_TRUNC('month', {{ date_column }}) as period_month,
            {% if revenue_column %}
            {{ revenue_column }} as revenue
            {% else %}
            1 as revenue
            {% endif %}
        FROM {{ this }}
    ),
    cohort_table AS (
        SELECT 
            cohort_month,
            period_month,
            COUNT(DISTINCT {{ user_id_column }}) as users,
            SUM(revenue) as total_revenue,
            DATEDIFF('month', cohort_month, period_month) as period_number
        FROM cohort_data
        GROUP BY cohort_month, period_month
    ),
    cohort_sizes AS (
        SELECT 
            cohort_month,
            COUNT(DISTINCT {{ user_id_column }}) as cohort_size
        FROM cohort_data
        WHERE cohort_month = period_month
        GROUP BY cohort_month
    )
    SELECT 
        c.cohort_month,
        c.period_number,
        c.users,
        c.total_revenue,
        s.cohort_size,
        ROUND(100.0 * c.users / s.cohort_size, 2) as retention_rate,
        ROUND(c.total_revenue / s.cohort_size, 2) as revenue_per_user
    FROM cohort_table c
    JOIN cohort_sizes s ON c.cohort_month = s.cohort_month
    ORDER BY c.cohort_month, c.period_number
{% endmacro %}

-- Segments customers using RFM analysis
{% macro rfm_analysis(customer_id_column, date_column, revenue_column) %}
    WITH rfm_base AS (
        SELECT 
            {{ customer_id_column }},
            MAX({{ date_column }}) as last_purchase_date,
            COUNT(DISTINCT {{ date_column }}) as frequency,
            SUM({{ revenue_column }}) as monetary_value,
            DATEDIFF('day', MAX({{ date_column }}), CURRENT_DATE) as recency_days
        FROM {{ this }}
        GROUP BY {{ customer_id_column }}
    ),
    rfm_scores AS (
        SELECT 
            *,
            NTILE(5) OVER (ORDER BY recency_days DESC) as recency_score,
            NTILE(5) OVER (ORDER BY frequency ASC) as frequency_score,
            NTILE(5) OVER (ORDER BY monetary_value ASC) as monetary_score
        FROM rfm_base
    ),
    rfm_segments AS (
        SELECT 
            *,
            CASE 
                WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
                WHEN recency_score >= 3 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Loyal Customers'
                WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'Potential Loyalists'
                WHEN recency_score >= 4 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'New Customers'
                WHEN recency_score >= 3 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Promising'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score >= 3 THEN 'Need Attention'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score >= 3 THEN 'About to Sleep'
                WHEN recency_score <= 2 AND frequency_score >= 3 AND monetary_score <= 2 THEN 'At Risk'
                WHEN recency_score <= 2 AND frequency_score <= 2 AND monetary_score <= 2 THEN 'Lost'
                ELSE 'Others'
            END as rfm_segment
        FROM rfm_scores
    )
    SELECT * FROM rfm_segments
{% endmacro %}

-- Generates features for time series forecasting
{% macro time_series_features(date_column, value_column, window_sizes=[7, 30, 90]) %}
    WITH base_data AS (
        SELECT 
            {{ date_column }},
            {{ value_column }},
            ROW_NUMBER() OVER (ORDER BY {{ date_column }}) as row_num
        FROM {{ this }}
    ),
    feature_engineering AS (
        SELECT 
            *,
            -- Creates time-lagged features
            LAG({{ value_column }}, 1) OVER (ORDER BY {{ date_column }}) as lag_1,
            LAG({{ value_column }}, 7) OVER (ORDER BY {{ date_column }}) as lag_7,
            LAG({{ value_column }}, 30) OVER (ORDER BY {{ date_column }}) as lag_30,
            
            -- Calculates rolling window averages
            {% for window in window_sizes %}
            AVG({{ value_column }}) OVER (
                ORDER BY {{ date_column }} 
                ROWS BETWEEN {{ window - 1 }} PRECEDING AND CURRENT ROW
            ) as rolling_avg_{{ window }},
            {% endfor %}
            
            -- Extracts seasonal time-based features
            EXTRACT(dow FROM {{ date_column }}) as day_of_week,
            EXTRACT(day FROM {{ date_column }}) as day_of_month,
            EXTRACT(week FROM {{ date_column }}) as week_of_year,
            EXTRACT(month FROM {{ date_column }}) as month_of_year,
            EXTRACT(quarter FROM {{ date_column }}) as quarter_of_year,
            
            -- Generates trend-based features
            row_num as trend,
            row_num * row_num as trend_squared
        FROM base_data
    )
    SELECT * FROM feature_engineering
{% endmacro %}

-- Detects data anomalies using statistical methods
{% macro detect_anomalies(value_column, date_column, method='zscore', sensitivity=0.95) %}
    WITH stats AS (
        SELECT 
            AVG({{ value_column }}) as mean_value,
            STDDEV({{ value_column }}) as std_value,
            PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY {{ value_column }}) as q1,
            PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY {{ value_column }}) as q3
        FROM {{ this }}
    ),
    anomaly_detection AS (
        SELECT 
            *,
            {% if method == 'zscore' %}
            ABS(({{ value_column }} - s.mean_value) / NULLIF(s.std_value, 0)) as z_score,
            CASE 
                WHEN ABS(({{ value_column }} - s.mean_value) / NULLIF(s.std_value, 0)) > {{ 1 - sensitivity }} THEN TRUE
                ELSE FALSE
            END as is_anomaly
            {% else %}
            -- Anomaly detection using the IQR method
            (s.q3 - s.q1) as iqr,
            CASE 
                WHEN {{ value_column }} < (s.q1 - 1.5 * (s.q3 - s.q1)) 
                  OR {{ value_column }} > (s.q3 + 1.5 * (s.q3 - s.q1)) THEN TRUE
                ELSE FALSE
            END as is_anomaly
            {% endif %}
        FROM {{ this }}
        CROSS JOIN stats s
    )
    SELECT * FROM anomaly_detection
{% endmacro %}

-- Generates features for product recommendations
{% macro product_recommendation_features(user_id_column, product_id_column, rating_column=none) %}
    WITH user_product_matrix AS (
        SELECT 
            {{ user_id_column }},
            {{ product_id_column }},
            {% if rating_column %}
            AVG({{ rating_column }}) as avg_rating,
            COUNT(*) as interaction_count
            {% else %}
            COUNT(*) as interaction_count,
            1 as avg_rating
            {% endif %}
        FROM {{ this }}
        GROUP BY {{ user_id_column }}, {{ product_id_column }}
    ),
    user_similarity AS (
        SELECT 
            u1.{{ user_id_column }} as user1,
            u2.{{ user_id_column }} as user2,
            CORR(u1.avg_rating, u2.avg_rating) as similarity_score
        FROM user_product_matrix u1
        JOIN user_product_matrix u2 ON u1.{{ product_id_column }} = u2.{{ product_id_column }}
        WHERE u1.{{ user_id_column }} != u2.{{ user_id_column }}
        GROUP BY u1.{{ user_id_column }}, u2.{{ user_id_column }}
        HAVING COUNT(*) >= 3
    ),
    product_popularity AS (
        SELECT 
            {{ product_id_column }},
            COUNT(DISTINCT {{ user_id_column }}) as user_count,
            AVG(avg_rating) as avg_product_rating,
            SUM(interaction_count) as total_interactions
        FROM user_product_matrix
        GROUP BY {{ product_id_column }}
    )
    SELECT 
        u.*,
        p.user_count,
        p.avg_product_rating,
        p.total_interactions,
        RANK() OVER (PARTITION BY u.{{ user_id_column }} ORDER BY u.avg_rating DESC) as user_product_rank
    FROM user_product_matrix u
    JOIN product_popularity p ON u.{{ product_id_column }} = p.{{ product_id_column }}
{% endmacro %}

-- Logs dbt model run results for auditing
{% macro audit_log_run_results() %}
    INSERT INTO {{ target.schema }}_logs.dbt_run_log (
        run_id,
        model_name,
        status,
        execution_time,
        rows_affected,
        run_started_at,
        run_completed_at
    )
    SELECT 
        '{{ invocation_id }}' as run_id,
        '{{ this }}' as model_name,
        'success' as status,
        DATEDIFF('second', '{{ run_started_at }}', CURRENT_TIMESTAMP) as execution_time,
        COUNT(*) as rows_affected,
        '{{ run_started_at }}' as run_started_at,
        CURRENT_TIMESTAMP as run_completed_at
    FROM {{ this }}
{% endmacro %}