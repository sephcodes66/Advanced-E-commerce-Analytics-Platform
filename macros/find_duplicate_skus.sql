{% macro find_duplicate_skus() %}
{% set query %}
with product_base as (
    select 
        product_sku,
        product_category,
        product_style,
        product_asin,
        count(distinct order_id) as total_orders,
        sum(quantity) as total_units_sold,
        sum(revenue) as total_revenue,
        count(distinct order_date) as active_sales_days,
        count(distinct partner_channel) as channel_presence,
        min(order_date) as first_sale_date,
        max(order_date) as last_sale_date,
        datediff('day', min(order_date), max(order_date)) as product_lifespan_days,
        count(distinct case when customer_segment = 'b2b' then order_id end) as b2b_orders,
        count(distinct case when customer_segment = 'b2c' then order_id end) as b2c_orders,
        count(distinct case when is_b2b then order_id end) as business_orders,
        sum(case when partner_channel = 'amazon' then revenue else 0 end) as amazon_revenue,
        sum(case when partner_channel = 'merchant_website' then revenue else 0 end) as merchant_revenue,
        sum(case when partner_channel = 'international' then revenue else 0 end) as international_revenue,
        count(case when order_value_tier = 'high_value' then 1 end) as high_value_orders,
        count(case when order_value_tier = 'medium_value' then 1 end) as medium_value_orders,
        count(case when order_value_tier = 'low_value' then 1 end) as low_value_orders,
        sum(case when quarter = 1 then revenue else 0 end) as q1_revenue,
        sum(case when quarter = 2 then revenue else 0 end) as q2_revenue,
        sum(case when quarter = 3 then revenue else 0 end) as q3_revenue,
        sum(case when quarter = 4 then revenue else 0 end) as q4_revenue,
        avg(revenue) as avg_order_value,
        avg(quantity) as avg_units_per_order,
        stddev(revenue) as revenue_volatility,
        avg(fulfilment_efficiency_score) as avg_fulfilment_score,
        avg(estimated_processing_days) as avg_processing_days
        
    from {{ ref('stg_partner_performance') }}
    where product_sku is not null
    group by product_sku, product_category, product_style, product_asin
)
select product_sku, count(*) from product_base group by product_sku having count(*) > 1;
{% endset %}

{% set results = run_query(query) %}

{% if execute %}
{% for row in results %}
    {{ log(row.product_sku ~ ': ' ~ row[1], info=True) }}
{% endfor %}
{% endif %}

{% endmacro %}
