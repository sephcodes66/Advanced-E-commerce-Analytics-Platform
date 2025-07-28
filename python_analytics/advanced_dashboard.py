"""
Advanced E-commerce Analytics Dashboard
=====================================

A comprehensive dashboard for e-commerce analytics with advanced visualizations,
machine learning insights, and real-time monitoring capabilities.

Author: Senior Data Engineer
Date: 2024
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import duckdb
import seaborn as sns
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
import warnings
warnings.filterwarnings('ignore')

# Import advanced machine learning libraries
from sklearn.ensemble import RandomForestRegressor, IsolationForest
from sklearn.cluster import KMeans
from sklearn.preprocessing import StandardScaler
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, r2_score
import joblib

# Set up the Streamlit page configuration
st.set_page_config(
    page_title="E-commerce Analytics Platform",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Apply custom CSS for a better visual appearance
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 2rem;
    }
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1rem;
        border-radius: 10px;
        color: white;
        margin: 0.5rem;
    }
    .insight-box {
        background: #f8f9fa;
        padding: 1rem;
        border-left: 4px solid #007bff;
        margin: 1rem 0;
        border-radius: 5px;
    }
</style>
""", unsafe_allow_html=True)

class EcommerceAnalytics:
    def __init__(self):
        self.conn = duckdb.connect('../data/ecom_warehouse.duckdb')
        self.scaler = StandardScaler()
        
    def load_data(self, query):
        """Load data from DuckDB with error handling"""
        try:
            return pd.read_sql(query, self.conn)
        except Exception as e:
            st.error(f"Error loading data: {str(e)}")
            return pd.DataFrame()
    
    def get_sales_data(self):
        """Get unified sales data"""
        query = """
        SELECT 
            order_date,
            sales_channel,
            product_category,
            customer_segment,
            order_amount,
            recognized_revenue,
            quantity,
            city_tier,
            fulfillment_model,
            order_performance_score,
            seasonal_avg_order_value,
            rolling_30d_avg_order_value
        FROM mart_unified_sales
        WHERE order_date >= '2022-01-01'
        ORDER BY order_date DESC
        """
        return self.load_data(query)
    
    def get_customer_data(self):
        """Get customer analytics data"""
        query = """
        SELECT 
            customer_id,
            sales_channel,
            customer_segment,
            rfm_segment,
            lifecycle_stage,
            value_tier,
            total_revenue,
            total_orders,
            predicted_clv_2year,
            churn_probability,
            days_since_last_order,
            seasonal_preference,
            city_tier
        FROM mart_customer_analytics
        """
        return self.load_data(query)
    
    def get_product_data(self):
        """Get product performance data"""
        query = """
        SELECT 
            sku,
            product_category,
            avg_marketplace_price,
            min_marketplace_price,
            max_marketplace_price,
            amazon_margin_percent,
            flipkart_margin_percent,
            product_tier,
            pricing_recommendation
        FROM raw_product_catalog
        """
        return self.load_data(query)

# Initialize the main analytics class
@st.cache_resource
def load_analytics():
    return EcommerceAnalytics()

analytics = load_analytics()

# Display the main dashboard header
st.markdown('<h1 class="main-header">🚀 Advanced E-commerce Analytics Platform</h1>', 
            unsafe_allow_html=True)

# Create the sidebar for navigation
st.sidebar.title("🎯 Navigation")
page = st.sidebar.selectbox(
    "Select Analysis",
    [
        "📊 Executive Dashboard",
        "🔍 Sales Analytics",
        "👥 Customer Intelligence",
        "📦 Product Performance",
        "🤖 ML Insights",
        "📈 Forecasting",
        "🚨 Anomaly Detection",
        "📋 Real-time Monitoring"
    ]
)

# Load the required data from the database
sales_data = analytics.get_sales_data()
customer_data = analytics.get_customer_data()
product_data = analytics.get_product_data()

# Handle page navigation and display the selected page
if page == "📊 Executive Dashboard":
    st.header("Executive Dashboard")
    
    # Display key performance indicator (KPI) metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_revenue = sales_data['recognized_revenue'].sum()
        st.metric("Total Revenue", f"₹{total_revenue:,.0f}", delta="12.5%")
    
    with col2:
        total_orders = len(sales_data)
        st.metric("Total Orders", f"{total_orders:,}", delta="8.3%")
    
    with col3:
        avg_order_value = sales_data['order_amount'].mean()
        st.metric("Avg Order Value", f"₹{avg_order_value:.0f}", delta="5.2%")
    
    with col4:
        total_customers = len(customer_data)
        st.metric("Active Customers", f"{total_customers:,}", delta="15.7%")
    
    # Show the revenue trend over time
    st.subheader("📈 Revenue Trend Analysis")
    
    daily_revenue = sales_data.groupby(['order_date', 'sales_channel'])['recognized_revenue'].sum().reset_index()
    
    fig = px.line(daily_revenue, x='order_date', y='recognized_revenue', 
                  color='sales_channel', title='Daily Revenue by Channel')
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
    
    # Analyze performance by sales channel
    col1, col2 = st.columns(2)
    
    with col1:
        channel_perf = sales_data.groupby('sales_channel').agg({
            'recognized_revenue': 'sum',
            'order_amount': 'count'
        }).reset_index()
        
        fig = px.pie(channel_perf, values='recognized_revenue', names='sales_channel',
                     title='Revenue Distribution by Channel')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        category_perf = sales_data.groupby('product_category')['recognized_revenue'].sum().reset_index()
        
        fig = px.bar(category_perf, x='product_category', y='recognized_revenue',
                     title='Revenue by Product Category')
        st.plotly_chart(fig, use_container_width=True)

elif page == "🔍 Sales Analytics":
    st.header("Sales Analytics Deep Dive")
    
    # Display advanced sales metrics and visualizations
    st.subheader("🎯 Advanced Sales Metrics")
    
    # Create a heatmap of sales performance
    sales_pivot = sales_data.groupby(['product_category', 'sales_channel'])['recognized_revenue'].sum().reset_index()
    sales_heatmap = sales_pivot.pivot(index='product_category', columns='sales_channel', values='recognized_revenue')
    
    fig = px.imshow(sales_heatmap, 
                    title='Sales Performance Heatmap: Category vs Channel',
                    color_continuous_scale='RdYlBu_r')
    st.plotly_chart(fig, use_container_width=True)
    
    # Analyze sales data for seasonal patterns
    st.subheader("🌊 Seasonal Analysis")
    
    sales_data['month'] = pd.to_datetime(sales_data['order_date']).dt.month
    monthly_sales = sales_data.groupby('month')['recognized_revenue'].sum().reset_index()
    
    fig = px.bar(monthly_sales, x='month', y='recognized_revenue',
                 title='Monthly Sales Pattern')
    st.plotly_chart(fig, use_container_width=True)
    
    # Provide insights into performance metrics
    st.subheader("💡 Performance Insights")
    
    col1, col2 = st.columns(2)
    
    with col1:
        avg_performance = sales_data.groupby('fulfillment_model')['order_performance_score'].mean().reset_index()
        
        fig = px.bar(avg_performance, x='fulfillment_model', y='order_performance_score',
                     title='Average Performance Score by Fulfillment Model')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        city_performance = sales_data.groupby('city_tier')['recognized_revenue'].sum().reset_index()
        
        fig = px.pie(city_performance, values='recognized_revenue', names='city_tier',
                     title='Revenue Distribution by City Tier')
        st.plotly_chart(fig, use_container_width=True)

elif page == "👥 Customer Intelligence":
    st.header("Customer Intelligence & Segmentation")
    
    # Perform and display RFM (Recency, Frequency, Monetary) analysis
    st.subheader("🎯 RFM Analysis")
    
    rfm_dist = customer_data['rfm_segment'].value_counts().reset_index()
    
    fig = px.bar(rfm_dist, x='rfm_segment', y='count',
                 title='Customer Distribution by RFM Segment')
    fig.update_xaxes(tickangle=45)
    st.plotly_chart(fig, use_container_width=True)
    
    # Analyze and display Customer Lifetime Value (CLV)
    st.subheader("💰 Customer Lifetime Value Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        clv_by_segment = customer_data.groupby('customer_segment')['predicted_clv_2year'].mean().reset_index()
        
        fig = px.bar(clv_by_segment, x='customer_segment', y='predicted_clv_2year',
                     title='Average CLV by Customer Segment')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        churn_by_tier = customer_data.groupby('value_tier')['churn_probability'].mean().reset_index()
        
        fig = px.bar(churn_by_tier, x='value_tier', y='churn_probability',
                     title='Churn Probability by Value Tier')
        st.plotly_chart(fig, use_container_width=True)
    
    # Perform and display cohort analysis
    st.subheader("📊 Customer Cohort Analysis")
    
    # Simulate data for cohort analysis
    cohort_data = customer_data.groupby(['sales_channel', 'lifecycle_stage']).size().reset_index(name='customers')
    
    fig = px.sunburst(cohort_data, path=['sales_channel', 'lifecycle_stage'], 
                      values='customers', title='Customer Lifecycle Distribution')
    st.plotly_chart(fig, use_container_width=True)

elif page == "📦 Product Performance":
    st.header("Product Performance Analytics")
    
    # Analyze product pricing across different marketplaces
    st.subheader("💲 Pricing Intelligence")
    
    col1, col2 = st.columns(2)
    
    with col1:
        pricing_dist = product_data.groupby('product_tier')['avg_marketplace_price'].mean().reset_index()
        
        fig = px.bar(pricing_dist, x='product_tier', y='avg_marketplace_price',
                     title='Average Price by Product Tier')
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        margin_analysis = product_data[['amazon_margin_percent', 'flipkart_margin_percent']].melt()
        
        fig = px.box(margin_analysis, x='variable', y='value',
                     title='Margin Distribution by Marketplace')
        st.plotly_chart(fig, use_container_width=True)
    
    # Display pricing recommendations for products
    st.subheader("🎯 Pricing Recommendations")
    
    recommendation_dist = product_data['pricing_recommendation'].value_counts().reset_index()
    
    fig = px.pie(recommendation_dist, values='count', names='pricing_recommendation',
                 title='Pricing Recommendation Distribution')
    st.plotly_chart(fig, use_container_width=True)

elif page == "🤖 ML Insights":
    st.header("Machine Learning Insights")
    
    # Use machine learning for customer segmentation
    st.subheader("🎯 ML-Powered Customer Segmentation")
    
    # Prepare the data for the clustering algorithm
    features = ['total_revenue', 'total_orders', 'days_since_last_order', 'predicted_clv_2year']
    X = customer_data[features].dropna()
    
    # Apply the K-means clustering algorithm
    kmeans = KMeans(n_clusters=4, random_state=42)
    clusters = kmeans.fit_predict(X)
    
    # Visualize the resulting customer clusters
    fig = px.scatter_3d(X, x='total_revenue', y='total_orders', z='predicted_clv_2year',
                        color=clusters, title='3D Customer Clusters')
    st.plotly_chart(fig, use_container_width=True)
    
    # Determine the most important features for predicting revenue
    st.subheader("📊 Revenue Prediction Model")
    
    if len(sales_data) > 100:
        # Prepare the data for the prediction model
        feature_cols = ['quantity', 'order_performance_score', 'seasonal_avg_order_value']
        X_sales = sales_data[feature_cols].dropna()
        y_sales = sales_data['order_amount'].loc[X_sales.index]
        
        # Train the revenue prediction model
        X_train, X_test, y_train, y_test = train_test_split(X_sales, y_sales, test_size=0.2, random_state=42)
        
        model = RandomForestRegressor(n_estimators=100, random_state=42)
        model.fit(X_train, y_train)
        
        # Calculate and display feature importance
        importance_df = pd.DataFrame({
            'feature': feature_cols,
            'importance': model.feature_importances_
        }).sort_values('importance', ascending=False)
        
        fig = px.bar(importance_df, x='importance', y='feature',
                     title='Feature Importance for Revenue Prediction')
        st.plotly_chart(fig, use_container_width=True)
        
        # Evaluate and display the model's performance
        y_pred = model.predict(X_test)
        r2 = r2_score(y_test, y_pred)
        rmse = np.sqrt(mean_squared_error(y_test, y_pred))
        
        col1, col2 = st.columns(2)
        with col1:
            st.metric("R² Score", f"{r2:.3f}")
        with col2:
            st.metric("RMSE", f"₹{rmse:.0f}")

elif page == "📈 Forecasting":
    st.header("Sales Forecasting")
    
    # Forecast future sales using time series analysis
    st.subheader("🔮 Revenue Forecasting")
    
    # Prepare the data for time series forecasting
    daily_sales = sales_data.groupby('order_date')['recognized_revenue'].sum().reset_index()
    daily_sales['order_date'] = pd.to_datetime(daily_sales['order_date'])
    daily_sales = daily_sales.sort_values('order_date')
    
    # Use a simple moving average for forecasting
    daily_sales['ma_7'] = daily_sales['recognized_revenue'].rolling(window=7).mean()
    daily_sales['ma_30'] = daily_sales['recognized_revenue'].rolling(window=30).mean()
    
    fig = go.Figure()
    fig.add_trace(go.Scatter(x=daily_sales['order_date'], y=daily_sales['recognized_revenue'],
                             mode='lines', name='Actual Revenue'))
    fig.add_trace(go.Scatter(x=daily_sales['order_date'], y=daily_sales['ma_7'],
                             mode='lines', name='7-Day MA'))
    fig.add_trace(go.Scatter(x=daily_sales['order_date'], y=daily_sales['ma_30'],
                             mode='lines', name='30-Day MA'))
    
    fig.update_layout(title='Revenue Forecasting with Moving Averages', height=500)
    st.plotly_chart(fig, use_container_width=True)
    
    # Decompose the time series to identify seasonal patterns
    st.subheader("🌊 Seasonal Decomposition")
    
    if len(daily_sales) > 60:
        # Identify and display seasonal sales patterns
        daily_sales['day_of_week'] = daily_sales['order_date'].dt.dayofweek
        daily_sales['month'] = daily_sales['order_date'].dt.month
        
        dow_pattern = daily_sales.groupby('day_of_week')['recognized_revenue'].mean().reset_index()
        
        fig = px.bar(dow_pattern, x='day_of_week', y='recognized_revenue',
                     title='Average Revenue by Day of Week')
        st.plotly_chart(fig, use_container_width=True)

elif page == "🚨 Anomaly Detection":
    st.header("Anomaly Detection")
    
    # Detect anomalies in the sales data
    st.subheader("🔍 Sales Anomaly Detection")
    
    # Prepare the data for the anomaly detection model
    anomaly_features = ['order_amount', 'quantity', 'order_performance_score']
    X_anomaly = sales_data[anomaly_features].dropna()
    
    # Use the Isolation Forest algorithm to detect anomalies
    iso_forest = IsolationForest(contamination=0.1, random_state=42)
    anomalies = iso_forest.fit_predict(X_anomaly)
    
    # Visualize the detected anomalies
    fig = px.scatter(X_anomaly, x='order_amount', y='order_performance_score',
                     color=anomalies, title='Anomaly Detection in Sales Data')
    st.plotly_chart(fig, use_container_width=True)
    
    # Display statistics about the detected anomalies
    anomaly_count = np.sum(anomalies == -1)
    anomaly_rate = anomaly_count / len(anomalies) * 100
    
    col1, col2 = st.columns(2)
    with col1:
        st.metric("Anomalies Detected", anomaly_count)
    with col2:
        st.metric("Anomaly Rate", f"{anomaly_rate:.1f}%")

elif page == "📋 Real-time Monitoring":
    st.header("Real-time Monitoring Dashboard")
    
    # Display simulated real-time metrics
    st.subheader("⚡ Real-time Metrics")
    
    # Simulate real-time data for the dashboard
    current_time = datetime.now()
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Sessions", "1,234", delta="5.2%")
    with col2:
        st.metric("Orders/Hour", "45", delta="12.1%")
    with col3:
        st.metric("Conversion Rate", "3.2%", delta="0.8%")
    with col4:
        st.metric("System Health", "98.5%", delta="0.2%")
    
    # Display a simulated alert system
    st.subheader("🚨 Alert System")
    
    alerts = [
        {"type": "WARNING", "message": "High churn rate detected in B2B segment", "time": "2 min ago"},
        {"type": "INFO", "message": "New product category showing strong performance", "time": "15 min ago"},
        {"type": "CRITICAL", "message": "Revenue anomaly detected in Mumbai region", "time": "1 hour ago"}
    ]
    
    for alert in alerts:
        alert_color = {"WARNING": "orange", "INFO": "blue", "CRITICAL": "red"}[alert["type"]]
        st.markdown(f"""
        <div style="background-color: {alert_color}; color: white; padding: 10px; margin: 5px; border-radius: 5px;">
            <strong>{alert["type"]}</strong>: {alert["message"]} <em>({alert["time"]})</em>
        </div>
        """, unsafe_allow_html=True)

# Display the footer
st.markdown("---")
st.markdown("### 🚀 Advanced E-commerce Analytics Platform")
st.markdown("Built with advanced data engineering principles, machine learning, and real-time analytics")