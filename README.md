# E-commerce Analytics Platform

This project helps you understand your online store's sales data. It uses data tools to organize and show you important information about your customers, products, and sales.

## What this project does

*   **Organizes Sales Data:** Cleans and prepares your sales data from different sources.
*   **Shows Key Information:** Creates easy-to-understand tables and charts about your business.
*   **Analyzes Customer Behavior:** Helps you understand who your best customers are and what they buy.
*   **Tracks Financial Performance:** Shows how your business is doing financially.

## How to get started

### What you need
*   Python 3.8 or higher
*   Git

### Steps to run the project
1.  **Copy the project:**
    ```bash
    git clone <repository-url>
    cd ecom_dbt_project
    ```
2.  **Set up:**
    ```bash
    pip install -r requirements.txt
    dbt deps
    ```
3.  **Run the data models:**
    ```bash
    dbt run --profiles-dir .
    ```
4.  **See the dashboard:**
    ```bash
    streamlit run python_analytics/advanced_dashboard.py
    ```

## Technologies used

*   **dbt:** To transform and model the data.
*   **DuckDB:** A simple and fast database.
*   **Streamlit:** To create an interactive dashboard to see the data.
*   **Python:** For data analysis and running the dashboard.
