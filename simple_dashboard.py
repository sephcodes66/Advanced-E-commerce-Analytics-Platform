#!/usr/bin/env python3
"""
Simple Interactive Dashboard for Advanced E-commerce Analytics Platform
Demonstrates key capabilities without complex dependencies
"""

import duckdb
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os

# Set up matplotlib for better display
plt.style.use('seaborn-v0_8')
sns.set_palette("husl")

def create_dashboard():
    """Create a comprehensive dashboard showing key analytics"""
    
    # Connect to database
    conn = duckdb.connect('data/ecom_warehouse.duckdb')
    
    # Create figure with subplots
    fig, axes = plt.subplots(2, 3, figsize=(18, 12))
    fig.suptitle('🚀 Advanced E-commerce Analytics Dashboard', fontsize=16, fontweight='bold')
    
    # 1. Customer Segmentation (RFM Analysis)
    customer_data = conn.execute('''
        SELECT rfm_segment, COUNT(*) as customers, AVG(total_revenue) as avg_revenue
        FROM mart_customer_analytics
        GROUP BY rfm_segment
        ORDER BY avg_revenue DESC
        LIMIT 8
    ''').fetchdf()
    
    axes[0,0].barh(customer_data['rfm_segment'], customer_data['customers'])
    axes[0,0].set_title('👥 Customer Segmentation (RFM)', fontweight='bold')
    axes[0,0].set_xlabel('Number of Customers')
    
    # 2. Sales Performance by Channel
    sales_data = conn.execute('''
        SELECT sales_channel, SUM(recognized_revenue) as revenue, COUNT(*) as orders
        FROM mart_unified_sales
        GROUP BY sales_channel
    ''').fetchdf()
    
    axes[0,1].pie(sales_data['revenue'], labels=sales_data['sales_channel'], autopct='%1.1f%%')
    axes[0,1].set_title('📊 Revenue by Sales Channel', fontweight='bold')
    
    # 3. Geographic Performance
    geo_data = conn.execute('''
        SELECT city_tier, SUM(recognized_revenue) as revenue, COUNT(*) as orders
        FROM mart_unified_sales
        WHERE city_tier IS NOT NULL
        GROUP BY city_tier
        ORDER BY revenue DESC
    ''').fetchdf()
    
    axes[0,2].bar(geo_data['city_tier'], geo_data['revenue'])
    axes[0,2].set_title('🌍 Geographic Performance', fontweight='bold')
    axes[0,2].set_ylabel('Revenue ($)')
    axes[0,2].tick_params(axis='x', rotation=45)
    
    # 4. Seasonal Analysis
    seasonal_data = conn.execute('''
        SELECT season, SUM(recognized_revenue) as revenue, COUNT(*) as orders
        FROM mart_unified_sales
        WHERE season IS NOT NULL
        GROUP BY season
        ORDER BY revenue DESC
    ''').fetchdf()
    
    axes[1,0].bar(seasonal_data['season'], seasonal_data['revenue'])
    axes[1,0].set_title('🌊 Seasonal Revenue Analysis', fontweight='bold')
    axes[1,0].set_ylabel('Revenue ($)')
    axes[1,0].tick_params(axis='x', rotation=45)
    
    # 5. Customer Lifecycle Distribution
    lifecycle_data = conn.execute('''
        SELECT lifecycle_stage, COUNT(*) as customers, AVG(predicted_clv_2year) as avg_clv
        FROM mart_customer_analytics
        GROUP BY lifecycle_stage
        ORDER BY customers DESC
    ''').fetchdf()
    
    axes[1,1].barh(lifecycle_data['lifecycle_stage'], lifecycle_data['customers'])
    axes[1,1].set_title('🔄 Customer Lifecycle Distribution', fontweight='bold')
    axes[1,1].set_xlabel('Number of Customers')
    
    # 6. Performance Score Distribution
    performance_data = conn.execute('''
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
    ''').fetchdf()
    
    axes[1,2].pie(performance_data['orders'], labels=performance_data['performance_category'], autopct='%1.1f%%')
    axes[1,2].set_title('🎯 Performance Score Distribution', fontweight='bold')
    
    # Adjust layout
    plt.tight_layout()
    
    # Save the dashboard
    plt.savefig('advanced_analytics_dashboard.png', dpi=300, bbox_inches='tight')
    print("📊 Dashboard saved as 'advanced_analytics_dashboard.png'")
    
    # Display key metrics
    print("\n🎯 KEY BUSINESS METRICS")
    print("=" * 40)
    
    # Overall metrics
    overall_metrics = conn.execute('''
        SELECT 
            COUNT(*) as total_orders,
            SUM(recognized_revenue) as total_revenue,
            AVG(order_amount) as avg_order_value,
            COUNT(DISTINCT CASE WHEN sales_channel = 'AMAZON' THEN order_id END) as amazon_orders,
            COUNT(DISTINCT CASE WHEN sales_channel = 'INTERNATIONAL' THEN order_id END) as intl_orders
        FROM mart_unified_sales
    ''').fetchone()
    
    print(f"📊 Total Orders: {overall_metrics[0]:,}")
    print(f"💰 Total Revenue: ${overall_metrics[1]:,.2f}")
    print(f"📈 Average Order Value: ${overall_metrics[2]:.2f}")
    print(f"🛒 Amazon Orders: {overall_metrics[3]:,}")
    print(f"🌍 International Orders: {overall_metrics[4]:,}")
    
    # Customer metrics
    customer_metrics = conn.execute('''
        SELECT 
            COUNT(*) as total_customers,
            AVG(total_revenue) as avg_clv,
            COUNT(CASE WHEN rfm_segment = 'Champions' THEN 1 END) as champions,
            COUNT(CASE WHEN churn_probability > 0.8 THEN 1 END) as high_churn_risk
        FROM mart_customer_analytics
    ''').fetchone()
    
    print(f"\n👥 Total Customers: {customer_metrics[0]:,}")
    print(f"💎 Average CLV: ${customer_metrics[1]:.2f}")
    print(f"🏆 Champion Customers: {customer_metrics[2]:,}")
    print(f"⚠️  High Churn Risk: {customer_metrics[3]:,}")
    
    # Data quality metrics
    quality_metrics = conn.execute('''
        SELECT 
            COUNT(*) as total_records,
            SUM(CASE WHEN business_validation_status = 'VALID' THEN 1 ELSE 0 END) as valid_records,
            ROUND(100.0 * SUM(CASE WHEN business_validation_status = 'VALID' THEN 1 ELSE 0 END) / COUNT(*), 2) as quality_score
        FROM stg_amazon_sales
    ''').fetchone()
    
    print(f"\n🔍 Data Quality Score: {quality_metrics[2]}%")
    print(f"✅ Valid Records: {quality_metrics[1]:,}/{quality_metrics[0]:,}")
    
    conn.close()
    
    return fig

def main():
    """Main function to run the dashboard"""
    print("🚀 ADVANCED E-COMMERCE ANALYTICS PLATFORM")
    print("=" * 50)
    print("Creating Interactive Dashboard...")
    
    try:
        # Create the dashboard
        fig = create_dashboard()
        
        print("\n✅ Dashboard Created Successfully!")
        print("=" * 50)
        print("🎯 Features Demonstrated:")
        print("• Customer Segmentation with RFM Analysis")
        print("• Multi-channel Sales Performance")
        print("• Geographic Intelligence")
        print("• Seasonal Trend Analysis")
        print("• Customer Lifecycle Management")
        print("• Performance Score Distribution")
        print("• Real-time Business Metrics")
        print("• Data Quality Monitoring")
        
        print("\n📊 Technical Achievements:")
        print("• Advanced SQL with Window Functions")
        print("• Multi-layered dbt Architecture")
        print("• Comprehensive Data Quality Framework")
        print("• Production-ready Business Intelligence")
        print("• Scalable Analytics Platform")
        
        print("\n🚀 Next Steps:")
        print("• View 'advanced_analytics_dashboard.png' for visual insights")
        print("• Run 'dbt docs generate && dbt docs serve' for documentation")
        print("• Execute 'make' commands for CI/CD workflows")
        print("• Deploy to production environment")
        
        return 0
        
    except Exception as e:
        print(f"❌ Error creating dashboard: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())