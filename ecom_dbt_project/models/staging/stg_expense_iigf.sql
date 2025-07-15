SELECT
    try_strptime(CAST(column1 AS VARCHAR), '%m-%d-%y') AS expense_date,
    TRIM(LOWER(column3)) AS product_sku,
    TRIM(LOWER(column3)) AS expense_description,
    CAST(column4 AS DECIMAL(10, 2)) AS expense_amount,
    'expense_iigf' AS _source_file_name
FROM read_csv_auto('/Users/shabhrishreddyuddehal/Downloads/dbt/data/raw/Expense IIGF.csv', header=False)
WHERE try_cast(column4 as decimal) is not null
OFFSET 2
