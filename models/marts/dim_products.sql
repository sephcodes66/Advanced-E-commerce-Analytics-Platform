SELECT DISTINCT
    product_sku,
    product_category,
    product_style,
    product_asin,
    COUNT(DISTINCT order_id) AS total_orders_containing_product,
    SUM(quantity) AS total_quantity_sold_across_all_orders
FROM {{ ref('stg_all_sales') }}
WHERE product_sku IS NOT NULL
GROUP BY product_sku, product_category, product_style, product_asin