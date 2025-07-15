import duckdb
import pandas as pd
from statsmodels.tsa.statespace.sarimax import SARIMAX

def get_revenue_data():
    """
    Retrieves revenue data from the database.
    """
    try:
        with duckdb.connect('/Users/shabhrishreddyuddehal/Downloads/dbt/data/ecom_warehouse.duckdb') as conn:
            return conn.execute('SELECT * FROM mart_financial_performance_summary').fetchdf()
    except Exception as e:
        print(f"Error getting data: {e}")
        return pd.DataFrame()

def get_revenue_forecast(data):
    """
    Generates a revenue forecast using a SARIMA model.
    """
    # Prepare the data
    data['order_date'] = pd.to_datetime(data['order_date'])
    data = data.set_index('order_date')
    daily_revenue = data['daily_revenue'].resample('D').sum()

    # Train the model
    model = SARIMAX(daily_revenue, order=(1, 1, 1), seasonal_order=(1, 1, 1, 7))
    results = model.fit()

    # Generate forecast
    forecast = results.get_forecast(steps=30)
    return forecast.predicted_mean

if __name__ == "__main__":
    revenue_data = get_revenue_data()
    if not revenue_data.empty:
        forecast = get_revenue_forecast(revenue_data)
        print(forecast)
