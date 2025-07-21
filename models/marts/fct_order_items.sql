SELECT
    s.order_id,
    s.order_date,
    s.order_status,
    s.fulfillment_type,
    s.is_b2b,
    s.product_sku,
    p.product_category,
    p.product_style,
    s.quantity,
    s.amount AS item_revenue_amount,
    s.currency,
    COALESCE(s.amount - (pl.cost_price_tp1 * s.quantity), 0) AS item_profit_amount, -- Profit is calculated using the TP1 cost price
    s._source_file_name
FROM {{ ref('stg_all_sales') }} s
LEFT JOIN {{ ref('dim_products') }} p
  ON s.product_sku = p.product_sku
LEFT JOIN {{ ref('dim_dates') }} d
  ON s.order_date = d.date_day
LEFT JOIN {{ ref('stg_pl_march_2021') }} pl
  ON s.product_sku = pl.product_sku
WHERE s.product_sku IS NOT NULL
