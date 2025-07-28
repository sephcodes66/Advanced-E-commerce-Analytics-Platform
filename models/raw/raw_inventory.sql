{{ config(
    materialized='table',
    tags=['raw', 'inventory', 'stock'],
    meta={
        'owner': 'data_engineering',
        'description': 'Raw inventory data with stock level analysis',
        'freshness_threshold': '1 hour'
    }
) }}

WITH source_data AS (
    SELECT 
        -- Unique product identifiers
        "SKU Code" as sku_code,
        "Design No." as design_number,
        "Category" as product_category,
        "Size" as product_size,
        "Color" as product_color,
        
        -- Current stock quantity
        TRY_CAST("Stock" as INTEGER) as stock_quantity,
        
        -- Data quality validation flags
        CASE 
            WHEN "SKU Code" IS NULL THEN 'MISSING_SKU'
            WHEN "Stock" IS NULL THEN 'MISSING_STOCK'
            WHEN "Category" IS NULL THEN 'MISSING_CATEGORY'
            ELSE 'VALID'
        END as data_quality_flag,
        
        -- Timestamps and IDs for auditing
        CURRENT_TIMESTAMP as ingested_at,
        '{{ invocation_id }}' as ingestion_run_id,
        
        -- MD5 hash for detecting row changes
        MD5(CONCAT(
            COALESCE("SKU Code", ''),
            COALESCE("Design No.", ''),
            COALESCE("Stock", ''),
            COALESCE("Category", '')
        )) as row_hash
        
    FROM read_csv_auto('../data/raw/Sale Report.csv', header=true)
    WHERE "SKU Code" IS NOT NULL
)

SELECT 
    *,
    -- Inventory stock level category
    CASE 
        WHEN stock_quantity = 0 THEN 'OUT_OF_STOCK'
        WHEN stock_quantity <= 3 THEN 'LOW_STOCK'
        WHEN stock_quantity <= 10 THEN 'MEDIUM_STOCK'
        WHEN stock_quantity <= 20 THEN 'HIGH_STOCK'
        ELSE 'OVERSTOCKED'
    END as stock_level_category,
    
    -- Standardized product size
    CASE 
        WHEN product_size = 'S' THEN 'SMALL'
        WHEN product_size = 'M' THEN 'MEDIUM'
        WHEN product_size = 'L' THEN 'LARGE'
        WHEN product_size = 'XL' THEN 'EXTRA_LARGE'
        WHEN product_size = 'XXL' THEN 'DOUBLE_EXTRA_LARGE'
        ELSE 'OTHER'
    END as standardized_size,
    
    -- Product color family
    CASE 
        WHEN LOWER(product_color) IN ('red', 'maroon', 'crimson') THEN 'RED_FAMILY'
        WHEN LOWER(product_color) IN ('blue', 'navy', 'sky') THEN 'BLUE_FAMILY'
        WHEN LOWER(product_color) IN ('green', 'olive', 'mint') THEN 'GREEN_FAMILY'
        WHEN LOWER(product_color) IN ('black', 'grey', 'charcoal') THEN 'NEUTRAL_FAMILY'
        WHEN LOWER(product_color) IN ('yellow', 'orange', 'gold') THEN 'WARM_FAMILY'
        WHEN LOWER(product_color) IN ('purple', 'violet', 'pink') THEN 'COOL_FAMILY'
        ELSE 'OTHER_FAMILY'
    END as color_family,
    
    -- Broader product category
    CASE 
        WHEN product_category LIKE '%LEGGINGS%' THEN 'BOTTOM_WEAR'
        WHEN product_category LIKE '%KURTA%' THEN 'ETHNIC_WEAR'
        WHEN product_category LIKE '%DRESS%' THEN 'WESTERN_WEAR'
        WHEN product_category LIKE '%TOP%' THEN 'TOP_WEAR'
        ELSE 'OTHER_CATEGORY'
    END as category_group,
    
    -- Risk level based on stock quantity
    CASE 
        WHEN stock_quantity = 0 THEN 'CRITICAL'
        WHEN stock_quantity <= 3 THEN 'HIGH_RISK'
        WHEN stock_quantity <= 10 THEN 'MEDIUM_RISK'
        ELSE 'LOW_RISK'
    END as inventory_risk_level,
    
    -- Recommended action for reordering
    CASE 
        WHEN stock_quantity = 0 THEN 'IMMEDIATE_REORDER'
        WHEN stock_quantity <= 3 THEN 'URGENT_REORDER'
        WHEN stock_quantity <= 10 THEN 'PLAN_REORDER'
        ELSE 'NO_ACTION_NEEDED'
    END as reorder_recommendation,
    
    -- Estimated value of the current stock (placeholder)
    stock_quantity * 500 as estimated_stock_value,
    
    -- Inferred product demand based on stock levels
    CASE 
        WHEN stock_quantity = 0 THEN 'HIGH_DEMAND'
        WHEN stock_quantity <= 3 THEN 'MEDIUM_DEMAND'
        ELSE 'LOW_DEMAND'
    END as demand_inference

FROM source_data