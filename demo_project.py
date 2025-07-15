#!/usr/bin/env python3
"""
Demo script showcasing the Advanced E-commerce Analytics Platform

This script demonstrates the key capabilities of the re-engineered dbt project:
- Advanced data transformations
- Customer analytics and segmentation
- Business intelligence
- Data quality monitoring
"""

import duckdb
import pandas as pd
import sys
import os

# Add the ecom_dbt_project to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'ecom_dbt_project'))

def connect_to_db():
    """Connect to the DuckDB database"""
    return duckdb.connect('data/ecom_warehouse.duckdb')

def demo_data_architecture():
    """Demonstrate the advanced data architecture"""
    print("🏗️  ADVANCED DATA ARCHITECTURE")
    print("=" * 50)
    
    conn = connect_to_db()
    
    # Show all tables in the database
    tables = conn.execute("SHOW TABLES").fetchall()
    
    print(f"\n📊 Total Tables Created: {len(tables)}")
    print("\nData Layers:")
    
    # Raw layer
    raw_tables = [t[0] for t in tables if t[0].startswith('raw_')]
    print(f"• Raw Layer: {len(raw_tables)} tables")
    for table in raw_tables:
        print(f"  - {table}")
    
    # Staging layer
    staging_tables = [t[0] for t in tables if t[0].startswith('stg_')]
    print(f"\n• Staging Layer: {len(staging_tables)} tables")
    for table in staging_tables[:5]:  # Show first 5
        print(f"  - {table}")
    
    # Marts layer
    mart_tables = [t[0] for t in tables if t[0].startswith('mart_')]
    print(f"\n• Marts Layer: {len(mart_tables)} tables")
    for table in mart_tables:
        print(f"  - {table}")
    
    conn.close()

def demo_unified_sales_analytics():
    """Demonstrate unified sales analytics"""
    print("\n\n📈 UNIFIED SALES ANALYTICS")
    print("=" * 50)
    
    conn = connect_to_db()
    
    # Key metrics
    metrics = conn.execute("""
        SELECT 
            COUNT(*) as total_orders,
            COUNT(DISTINCT sales_channel) as channels,
            SUM(recognized_revenue) as total_revenue,
            AVG(order_amount) as avg_order_value,
            COUNT(DISTINCT order_cohort) as cohorts_analyzed
        FROM mart_unified_sales
    """).fetchone()
    
    print(f"📊 Total Orders: {metrics[0]:,}")
    print(f"🛒 Sales Channels: {metrics[1]}")
    print(f"💰 Total Revenue: ${metrics[2]:,.2f}")
    print(f"📊 Average Order Value: ${metrics[3]:.2f}")
    print(f"📅 Cohorts Analyzed: {metrics[4]}")
    
    # Channel performance
    channel_perf = conn.execute("""
        SELECT 
            sales_channel,
            COUNT(*) as orders,
            SUM(recognized_revenue) as revenue,
            AVG(composite_performance_score) as avg_performance
        FROM mart_unified_sales
        GROUP BY sales_channel
        ORDER BY revenue DESC
    """).fetchall()
    
    print(f"\n📊 Channel Performance:")
    for channel, orders, revenue, perf in channel_perf:
        print(f"  {channel}: {orders:,} orders, ${revenue:,.2f} revenue, {perf:.1f} performance")
    
    conn.close()

def demo_customer_intelligence():
    """Demonstrate customer analytics and segmentation"""
    print("\n\n👥 CUSTOMER INTELLIGENCE & SEGMENTATION")
    print("=" * 50)
    
    conn = connect_to_db()
    
    # Customer overview
    customer_stats = conn.execute("""
        SELECT 
            COUNT(*) as total_customers,
            SUM(total_revenue) as total_clv,
            AVG(total_revenue) as avg_clv,
            AVG(churn_probability) as avg_churn_risk
        FROM mart_customer_analytics
    """).fetchone()
    
    print(f"👥 Total Customers: {customer_stats[0]:,}")
    print(f"💰 Total Customer LTV: ${customer_stats[1]:,.2f}")
    print(f"💰 Average Customer LTV: ${customer_stats[2]:.2f}")
    print(f"⚠️  Average Churn Risk: {customer_stats[3]:.2%}")
    
    # RFM Segmentation
    rfm_segments = conn.execute("""
        SELECT 
            rfm_segment,
            COUNT(*) as customers,
            AVG(total_revenue) as avg_revenue,
            AVG(churn_probability) as avg_churn_risk
        FROM mart_customer_analytics
        GROUP BY rfm_segment
        ORDER BY avg_revenue DESC
    """).fetchall()
    
    print(f"\n🎯 RFM Customer Segmentation:")
    for segment, customers, avg_revenue, churn_risk in rfm_segments:
        print(f"  {segment}: {customers:,} customers, ${avg_revenue:.2f} avg revenue, {churn_risk:.1%} churn risk")
    
    # Lifecycle stages
    lifecycle_stages = conn.execute("""
        SELECT 
            lifecycle_stage,
            COUNT(*) as customers,
            AVG(predicted_clv_2year) as avg_predicted_clv
        FROM mart_customer_analytics
        GROUP BY lifecycle_stage
        ORDER BY customers DESC
    """).fetchall()
    
    print(f"\n🔄 Customer Lifecycle Distribution:")
    for stage, customers, pred_clv in lifecycle_stages:
        print(f"  {stage}: {customers:,} customers, ${pred_clv:.2f} predicted 2-year CLV")
    
    conn.close()

def demo_advanced_analytics():
    """Demonstrate advanced analytics capabilities"""
    print("\n\n🧠 ADVANCED ANALYTICS & INSIGHTS")
    print("=" * 50)
    
    conn = connect_to_db()
    
    # Rolling metrics demonstration
    rolling_analysis = conn.execute("""
        SELECT 
            order_date,
            SUM(recognized_revenue) as daily_revenue,
            AVG(rolling_30d_avg_order_value) as rolling_30d_avg
        FROM mart_unified_sales
        WHERE order_date IS NOT NULL
        GROUP BY order_date, rolling_30d_avg_order_value
        ORDER BY order_date DESC
        LIMIT 10
    """).fetchall()
    
    print(f"📊 Rolling 30-Day Analytics (Last 10 days):")
    for date, daily_rev, rolling_avg in rolling_analysis:
        if date:
            print(f"  {date}: ${daily_rev:,.2f} daily revenue, ${rolling_avg:.2f} rolling 30d avg")
    
    # Seasonality analysis
    seasonal_analysis = conn.execute("""
        SELECT 
            season,
            COUNT(*) as orders,
            SUM(recognized_revenue) as revenue,
            AVG(order_amount) as avg_order_value
        FROM mart_unified_sales
        WHERE season IS NOT NULL
        GROUP BY season
        ORDER BY revenue DESC
    """).fetchall()
    
    print(f"\n🌊 Seasonal Analysis:")
    for season, orders, revenue, avg_order in seasonal_analysis:
        print(f"  {season}: {orders:,} orders, ${revenue:,.2f} revenue, ${avg_order:.2f} AOV")
    
    # Performance scoring
    performance_dist = conn.execute("""
        SELECT 
            CASE 
                WHEN composite_performance_score >= 90 THEN 'Excellent'
                WHEN composite_performance_score >= 70 THEN 'Good'
                WHEN composite_performance_score >= 50 THEN 'Fair'
                ELSE 'Poor'
            END as performance_category,
            COUNT(*) as orders,
            AVG(order_amount) as avg_order_value
        FROM mart_unified_sales
        GROUP BY performance_category
        ORDER BY avg_order_value DESC
    """).fetchall()
    
    print(f"\n🎯 Performance Score Distribution:")
    for category, orders, avg_order in performance_dist:
        print(f"  {category}: {orders:,} orders, ${avg_order:.2f} avg order value")
    
    conn.close()

def demo_data_quality():
    """Demonstrate data quality and monitoring"""
    print("\n\n🔍 DATA QUALITY & MONITORING")
    print("=" * 50)
    
    conn = connect_to_db()
    
    # Data quality metrics
    quality_metrics = conn.execute("""
        SELECT 
            'Raw Data' as layer,
            COUNT(*) as total_records,
            SUM(CASE WHEN data_quality_flag = 'VALID' THEN 1 ELSE 0 END) as valid_records,
            ROUND(100.0 * SUM(CASE WHEN data_quality_flag = 'VALID' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_percentage
        FROM raw_amazon_sales
        
        UNION ALL
        
        SELECT 
            'Processed Data' as layer,
            COUNT(*) as total_records,
            SUM(CASE WHEN business_validation_status = 'VALID' THEN 1 ELSE 0 END) as valid_records,
            ROUND(100.0 * SUM(CASE WHEN business_validation_status = 'VALID' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_percentage
        FROM stg_amazon_sales
    """).fetchall()
    
    print(f"📊 Data Quality Metrics:")
    for layer, total, valid, quality_pct in quality_metrics:
        print(f"  {layer}: {valid:,}/{total:,} valid records ({quality_pct}% quality)")
    
    # Business validation
    validation_stats = conn.execute("""
        SELECT 
            business_validation_status,
            COUNT(*) as record_count
        FROM stg_amazon_sales
        GROUP BY business_validation_status
        ORDER BY record_count DESC
    """).fetchall()
    
    print(f"\n✅ Business Validation Results:")
    for status, count in validation_stats:
        print(f"  {status}: {count:,} records")
    
    conn.close()

def demo_business_intelligence():
    """Demonstrate business intelligence capabilities"""
    print("\n\n💼 BUSINESS INTELLIGENCE INSIGHTS")
    print("=" * 50)
    
    conn = connect_to_db()
    
    # Geographic analysis
    geo_analysis = conn.execute("""
        SELECT 
            city_tier,
            COUNT(*) as orders,
            SUM(recognized_revenue) as revenue,
            AVG(order_amount) as avg_order_value
        FROM mart_unified_sales
        WHERE city_tier IS NOT NULL
        GROUP BY city_tier
        ORDER BY revenue DESC
    """).fetchall()
    
    print(f"🌍 Geographic Performance:")
    for tier, orders, revenue, avg_order in geo_analysis:
        print(f"  {tier}: {orders:,} orders, ${revenue:,.2f} revenue, ${avg_order:.2f} AOV")
    
    # Customer segment analysis
    segment_analysis = conn.execute("""
        SELECT 
            customer_segment,
            COUNT(*) as orders,
            SUM(recognized_revenue) as revenue,
            AVG(composite_performance_score) as avg_performance
        FROM mart_unified_sales
        GROUP BY customer_segment
        ORDER BY revenue DESC
    """).fetchall()
    
    print(f"\n👥 Customer Segment Analysis:")
    for segment, orders, revenue, performance in segment_analysis:
        print(f"  {segment}: {orders:,} orders, ${revenue:,.2f} revenue, {performance:.1f} performance")
    
    # Fulfillment model efficiency
    fulfillment_analysis = conn.execute("""
        SELECT 
            fulfillment_model,
            COUNT(*) as orders,
            AVG(composite_performance_score) as avg_performance,
            SUM(recognized_revenue) as revenue
        FROM mart_unified_sales
        GROUP BY fulfillment_model
        ORDER BY avg_performance DESC
    """).fetchall()
    
    print(f"\n📦 Fulfillment Model Efficiency:")
    for model, orders, performance, revenue in fulfillment_analysis:
        print(f"  {model}: {orders:,} orders, {performance:.1f} performance, ${revenue:,.2f} revenue")
    
    conn.close()

def main():
    """Main demonstration function"""
    print("🚀 ADVANCED E-COMMERCE ANALYTICS PLATFORM")
    print("=" * 60)
    print("Demonstrating 15+ Years of Data Engineering Expertise")
    print("Built with dbt, Advanced SQL, Python, and ML")
    print("=" * 60)
    
    try:
        # Run all demonstrations
        demo_data_architecture()
        demo_unified_sales_analytics()
        demo_customer_intelligence()
        demo_advanced_analytics()
        demo_data_quality()
        demo_business_intelligence()
        
        print("\n\n✅ PROJECT DEMONSTRATION COMPLETE!")
        print("=" * 60)
        print("🎯 Key Achievements Demonstrated:")
        print("• Advanced multi-layered dbt architecture")
        print("• Sophisticated SQL transformations and business logic")
        print("• Customer intelligence with RFM analysis and CLV prediction")
        print("• Real-time analytics with rolling metrics and seasonality")
        print("• Comprehensive data quality framework")
        print("• Production-ready business intelligence platform")
        print("• Enterprise-grade data engineering standards")
        
        print("\n📈 Next Steps:")
        print("• Run 'streamlit run python_analytics/advanced_dashboard.py' for interactive dashboard")
        print("• Execute 'dbt docs generate && dbt docs serve' for documentation")
        print("• Use 'make' commands for CI/CD workflows")
        
    except Exception as e:
        print(f"❌ Error during demonstration: {str(e)}")
        return 1
    
    return 0

if __name__ == "__main__":
    sys.exit(main())