{{ config(
    materialized='table',
    tags=['raw', 'product', 'catalog'],
    meta={
        'owner': 'data_engineering',
        'description': 'Raw product catalog data with marketplace pricing intelligence',
        'freshness_threshold': '24 hours'
    }
) }}

WITH source_data AS (
    SELECT 
        -- Product identifiers
        "Sku" as sku,
        "Style Id" as style_id,
        "Catalog" as catalog_name,
        "Category" as product_category,
        
        -- Physical attributes
        TRY_CAST("Weight" as DECIMAL(5,2)) as weight_kg,
        
        -- Pricing data
        TRY_CAST("TP 1" as DECIMAL(10,2)) as trade_price_1,
        TRY_CAST("TP 2" as DECIMAL(10,2)) as trade_price_2,
        TRY_CAST("MRP Old" as DECIMAL(10,2)) as mrp_old,
        TRY_CAST("Final MRP Old" as DECIMAL(10,2)) as final_mrp_old,
        
        -- Marketplace pricing
        TRY_CAST("Ajio MRP" as DECIMAL(10,2)) as ajio_mrp,
        TRY_CAST("Amazon MRP" as DECIMAL(10,2)) as amazon_mrp,
        TRY_CAST("Amazon FBA MRP" as DECIMAL(10,2)) as amazon_fba_mrp,
        TRY_CAST("Flipkart MRP" as DECIMAL(10,2)) as flipkart_mrp,
        TRY_CAST("Limeroad MRP" as DECIMAL(10,2)) as limeroad_mrp,
        TRY_CAST("Myntra MRP" as DECIMAL(10,2)) as myntra_mrp,
        TRY_CAST("Paytm MRP" as DECIMAL(10,2)) as paytm_mrp,
        TRY_CAST("Snapdeal MRP" as DECIMAL(10,2)) as snapdeal_mrp,
        
        -- Data quality indicators
        CASE 
            WHEN "Sku" IS NULL THEN 'MISSING_SKU'
            WHEN "Style Id" IS NULL THEN 'MISSING_STYLE_ID'
            WHEN "Category" IS NULL THEN 'MISSING_CATEGORY'
            WHEN "TP 1" IS NULL THEN 'MISSING_TRADE_PRICE'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Audit fields
        CURRENT_TIMESTAMP as ingested_at,
        '{{ invocation_id }}' as ingestion_run_id,
        
        -- Hash for change detection
        MD5(CONCAT(
            COALESCE("Sku", ''),
            COALESCE("Style Id", ''),
            COALESCE("TP 1", ''),
            COALESCE("Amazon MRP", ''),
            COALESCE("Flipkart MRP", '')
        )) as row_hash
        
    FROM read_csv_auto('../data/raw/P  L March 2021.csv', header=true)
    WHERE "Sku" IS NOT NULL
),

marketplace_analysis AS (
    SELECT 
        *,
        -- Marketplace pricing analysis
        ARRAY[ajio_mrp, amazon_mrp, amazon_fba_mrp, flipkart_mrp, limeroad_mrp, myntra_mrp, paytm_mrp, snapdeal_mrp] as marketplace_prices,
        
        -- Price statistics
        LEAST(ajio_mrp, amazon_mrp, amazon_fba_mrp, flipkart_mrp, limeroad_mrp, myntra_mrp, paytm_mrp, snapdeal_mrp) as min_marketplace_price,
        GREATEST(ajio_mrp, amazon_mrp, amazon_fba_mrp, flipkart_mrp, limeroad_mrp, myntra_mrp, paytm_mrp, snapdeal_mrp) as max_marketplace_price,
        
        -- Calculate average marketplace price
        (ajio_mrp + amazon_mrp + amazon_fba_mrp + flipkart_mrp + limeroad_mrp + myntra_mrp + paytm_mrp + snapdeal_mrp) / 8.0 as avg_marketplace_price,
        
        -- Margin calculations
        ROUND(((ajio_mrp - trade_price_1) / NULLIF(ajio_mrp, 0)) * 100, 2) as ajio_margin_percent,
        ROUND(((amazon_mrp - trade_price_1) / NULLIF(amazon_mrp, 0)) * 100, 2) as amazon_margin_percent,
        ROUND(((flipkart_mrp - trade_price_1) / NULLIF(flipkart_mrp, 0)) * 100, 2) as flipkart_margin_percent,
        ROUND(((myntra_mrp - trade_price_1) / NULLIF(myntra_mrp, 0)) * 100, 2) as myntra_margin_percent,
        
        -- Product classification
        CASE 
            WHEN product_category = 'Kurta' THEN 'ETHNIC_WEAR'
            WHEN product_category = 'Set' THEN 'COMBO_WEAR'
            ELSE 'OTHER'
        END as product_classification,
        
        -- Size extraction from SKU
        CASE 
            WHEN sku LIKE '%_S' THEN 'S'
            WHEN sku LIKE '%_M' THEN 'M'
            WHEN sku LIKE '%_L' THEN 'L'
            WHEN sku LIKE '%_XL' THEN 'XL'
            WHEN sku LIKE '%_2XL' THEN '2XL'
            WHEN sku LIKE '%_3XL' THEN '3XL'
            ELSE 'UNKNOWN'
        END as size_from_sku
        
    FROM source_data
)

SELECT 
    *,
    -- Competitive pricing insights
    CASE 
        WHEN min_marketplace_price = max_marketplace_price THEN 'UNIFORM_PRICING'
        WHEN (max_marketplace_price - min_marketplace_price) / min_marketplace_price > 0.2 THEN 'HIGH_PRICE_VARIANCE'
        WHEN (max_marketplace_price - min_marketplace_price) / min_marketplace_price > 0.1 THEN 'MEDIUM_PRICE_VARIANCE'
        ELSE 'LOW_PRICE_VARIANCE'
    END as price_variance_category,
    
    -- Most competitive marketplace
    CASE 
        WHEN ajio_mrp = min_marketplace_price THEN 'AJIO'
        WHEN amazon_mrp = min_marketplace_price THEN 'AMAZON'
        WHEN flipkart_mrp = min_marketplace_price THEN 'FLIPKART'
        WHEN myntra_mrp = min_marketplace_price THEN 'MYNTRA'
        WHEN paytm_mrp = min_marketplace_price THEN 'PAYTM'
        WHEN snapdeal_mrp = min_marketplace_price THEN 'SNAPDEAL'
        WHEN limeroad_mrp = min_marketplace_price THEN 'LIMEROAD'
        ELSE 'AMAZON_FBA'
    END as most_competitive_marketplace,
    
    -- Pricing strategy recommendation
    CASE 
        WHEN avg_marketplace_price > trade_price_1 * 2 THEN 'REDUCE_PRICE'
        WHEN avg_marketplace_price < trade_price_1 * 1.5 THEN 'INCREASE_PRICE'
        ELSE 'MAINTAIN_PRICE'
    END as pricing_recommendation,
    
    -- Product performance tier
    CASE 
        WHEN avg_marketplace_price > 2000 THEN 'PREMIUM'
        WHEN avg_marketplace_price > 1000 THEN 'MID_RANGE'
        ELSE 'BUDGET'
    END as product_tier

FROM marketplace_analysis