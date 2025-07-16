SELECT
    order_date,
    product_category,
    fulfillment_type,
    SUM(item_revenue_amount) AS daily_revenue,
    SUM(quantity) AS daily_quantity_sold,
    COUNT(DISTINCT order_id) AS daily_unique_orders,
    SUM(item_profit_amount) AS daily_profit
FROM {{ ref('fct_order_items') }}
GROUP BY 1, 2, 3
ORDER BY 1, 2, 3
