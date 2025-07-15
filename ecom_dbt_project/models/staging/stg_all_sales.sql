SELECT
    order_item_key,
    order_id,
    order_date,
    order_status,
    fulfillment_type,
    product_category,
    style_code AS product_style,
    sku AS product_sku,
    asin AS product_asin,
    quantity,
    order_amount AS amount,
    currency_code AS currency,
    is_b2b,
    'amazon_sales' AS _source_file_name
FROM {{ ref('stg_amazon_sales') }}

UNION ALL

SELECT
    order_item_id AS order_item_key,
    order_id,
    order_date,
    order_status,
    fulfilment_method AS fulfillment_type,
    product_category,
    CAST(NULL AS VARCHAR) AS product_style,
    product_sku,
    product_asin,
    quantity,
    amount,
    currency,
    is_b2b_sale AS is_b2b,
    _source_file_name
FROM {{ ref('stg_sale_report') }}
