# E-commerce Data Analytics

This project takes raw e-commerce data and turns it into useful information for making business decisions.

![Dashboard Screenshot](./screenshots/ss_1.png)

## What this project does

*   **Analyzes Partner Performance:** Tracks how well partners are doing.
*   **Summarizes Financials:** Gives a clear view of the company's financial health.
*   **Segments Customers:** Groups customers to understand them better.
*   **Analyzes Products:** Helps in making smart decisions about products.

## How to get started

### What you need
*   Python 3.8 or higher
*   dbt Core 1.0 or higher

### Steps to run the project
1.  **Install what you need:**
    ```bash
    pip install dbt-core dbt-duckdb pandas numpy plotly dash
    ```
2.  **Set up the dbt project:**
    ```bash
    dbt deps
    dbt seed
    ```
3.  **Run the dbt models:**
    ```bash
    dbt run
    dbt test
    ```
4.  **See the project documents:**
    ```bash
    dbt docs generate
    dbt docs serve
    ```

## Key Data Models

*   `mart_partner_performance_dashboard`: Shows how partners are performing in real-time.
*   `mart_financial_performance_summary`: Summarizes financial data for executives.
*   `mart_partner_optimization_insights`: Gives suggestions to improve partner performance.
*   `mart_cohort_analysis`: Analyzes customer retention and lifetime value.
*   `dim_customer_segments`: Groups customers based on their behavior.
*   `dim_product_intelligence`: Helps in making strategic decisions about products.
