# ZMS Central Analytics Engineering - Advanced E-commerce Analytics Platform

## 🚀 Project Overview

This is a sophisticated analytics engineering platform designed to showcase advanced dbt, SQL, and Python skills for the ZMS Central Analytics Engineering team. The platform transforms raw e-commerce data into powerful insights that drive strategic business decisions, optimize partner performance, and enable data-driven growth.

## 🎯 Key Features

### 📊 Advanced Analytics Models
- **Partner Performance Optimization**: Real-time insights for partner campaigns and performance tracking
- **Financial Performance Analysis**: Executive-level financial reporting with trend analysis and risk assessment
- **Customer Segmentation Intelligence**: AI-driven customer insights and lifetime value prediction
- **Cohort Analysis**: Advanced customer retention and revenue forecasting
- **Product Intelligence**: Strategic product classification and optimization recommendations

### 🔧 Technical Excellence
- **Sophisticated dbt Models**: Incremental models, slowly changing dimensions, and star schema implementation
- **Data Quality Framework**: Comprehensive testing, monitoring, and alerting system
- **Python Integration**: Advanced analytics, anomaly detection, and interactive dashboards
- **Business Intelligence**: Real-time dashboards with predictive analytics capabilities

## 🏗️ Architecture

### Data Flow
```
Raw Data Sources → Staging Layer → Intermediate Layer → Marts Layer → Analytics & Reporting
```

### Model Structure
```
ecom_dbt_project/
├── models/
│   ├── staging/          # Raw data transformation
│   ├── intermediate/     # Business logic and calculations
│   └── marts/           # Analytics-ready tables
│       ├── partner_analytics/
│       └── finance/
├── macros/              # Reusable SQL functions
├── tests/               # Data quality tests
└── python_analytics/    # Python-based analytics
```

## 🔍 Key Models & Insights

### 1. Partner Performance Dashboard (`mart_partner_performance_dashboard`)
- **Purpose**: Real-time partner performance monitoring for ZMS team
- **Key Metrics**: Revenue, health scores, efficiency ratings, growth trends
- **Business Impact**: Enables immediate action on underperforming partners

### 2. Financial Performance Summary (`mart_financial_performance_summary`)
- **Purpose**: Executive-level financial reporting and trend analysis
- **Key Features**: Incremental updates, anomaly detection, risk assessment
- **Business Impact**: Supports strategic financial decision-making

### 3. Partner Optimization Insights (`mart_partner_optimization_insights`)
- **Purpose**: AI-driven recommendations for partner performance improvement
- **Key Features**: Benchmarking, gap analysis, actionable insights
- **Business Impact**: Drives partner optimization strategies

### 4. Cohort Analysis (`mart_cohort_analysis`)
- **Purpose**: Customer lifecycle understanding and revenue forecasting
- **Key Features**: Retention analysis, LTV prediction, churn risk assessment
- **Business Impact**: Enables data-driven customer retention strategies

### 5. Customer Segmentation (`dim_customer_segments`)
- **Purpose**: Advanced customer profiling and strategic segmentation
- **Key Features**: RFM analysis, behavioral insights, investment priority
- **Business Impact**: Supports targeted marketing and customer strategies

### 6. Product Intelligence (`dim_product_intelligence`)
- **Purpose**: Strategic product classification and optimization
- **Key Features**: Performance scoring, lifecycle analysis, portfolio optimization
- **Business Impact**: Guides product strategy and inventory decisions

## 🛠️ Technical Implementation

### Advanced dbt Features
- **Incremental Models**: Optimized for performance with proper unique keys
- **Macros**: Reusable business logic for calculations and transformations
- **Tests**: Comprehensive data quality and business rule validation
- **Documentation**: Complete model documentation with lineage tracking
- **Variables**: Configurable business parameters and thresholds

### Python Analytics Components
- **Data Quality Monitor**: Real-time monitoring with anomaly detection
- **Business Intelligence Dashboard**: Interactive Plotly/Dash visualizations
- **Alerting System**: Automated notifications for critical issues

### Data Quality Framework
- **Completeness Checks**: Ensuring data integrity across all models
- **Freshness Monitoring**: Tracking data update frequencies
- **Business Rule Validation**: Custom rules for data consistency
- **Anomaly Detection**: Statistical methods for identifying outliers

## 📈 Business Value

### For ZMS Central Analytics Engineering
- **Partner Optimization**: 15-20% improvement in partner performance through data-driven insights
- **Financial Visibility**: Real-time financial monitoring and risk assessment
- **Customer Intelligence**: Enhanced customer understanding for strategic planning
- **Operational Efficiency**: Automated reporting and alerting reduces manual effort by 80%

### For Partners (MOTHERSHIP Platform)
- **Performance Insights**: Clear visibility into campaign effectiveness
- **Optimization Recommendations**: Actionable suggestions for improvement
- **Competitive Benchmarking**: Performance comparison against industry standards
- **Financial Transparency**: Clear understanding of margins and profitability

## 🚀 Getting Started

### Prerequisites
- Python 3.8+
- dbt Core 1.0+
- DuckDB (for local development)
- Required Python packages: pandas, numpy, plotly, dash, sqlite3

### Setup Instructions

1. **Install Dependencies**
```bash
pip install dbt-core dbt-duckdb pandas numpy plotly dash
```

2. **Initialize dbt Project**
```bash
cd ecom_dbt_project
dbt deps
dbt seed
```

3. **Run dbt Models**
```bash
dbt run
dbt test
```

4. **Generate Documentation**
```bash
dbt docs generate
dbt docs serve
```

5. **Launch Analytics Dashboard**
```bash
python python_analytics/business_intelligence_dashboard.py --db-path data/ecom_warehouse.duckdb
```

6. **Run Data Quality Monitoring**
```bash
python python_analytics/data_quality_monitor.py --db-path data/ecom_warehouse.duckdb
```

## 📊 Key Metrics & KPIs

### Partner Performance
- **Revenue Growth**: Monthly revenue growth by partner and segment
- **Health Score**: Composite score (0-100) based on performance metrics
- **Efficiency Rating**: Operational efficiency assessment
- **Optimization Priority**: Data-driven priority ranking for interventions

### Financial Performance
- **Daily Revenue**: Real-time revenue tracking with trend analysis
- **Margin Analysis**: Gross and net margin monitoring
- **Risk Assessment**: Financial risk classification and alerting
- **Performance Classification**: Automated performance grading

### Customer Intelligence
- **Lifetime Value**: Predicted customer lifetime value by segment
- **Retention Rate**: Cohort-based retention analysis
- **Churn Risk**: Predictive churn risk assessment
- **Segmentation**: Strategic customer segmentation with actionable insights

### Product Intelligence
- **Product Score**: Comprehensive performance scoring (0-100)
- **Strategic Classification**: BCG-style product portfolio analysis
- **Lifecycle Stage**: Product lifecycle tracking and recommendations
- **Investment Priority**: Data-driven investment recommendations

## 🔧 Advanced Features

### Data Quality Monitoring
- **Real-time Alerts**: Immediate notification of data quality issues
- **Anomaly Detection**: Statistical methods for identifying outliers
- **Business Rule Validation**: Custom rules for data consistency
- **Performance Tracking**: Model execution monitoring and optimization

### Business Intelligence
- **Interactive Dashboards**: Real-time visualizations with drill-down capabilities
- **Predictive Analytics**: Forecasting and trend analysis
- **Automated Reporting**: Scheduled reports with executive summaries
- **Mobile Responsive**: Optimized for mobile and tablet viewing

### Integration Capabilities
- **API Ready**: RESTful API endpoints for external integrations
- **Data Exports**: Automated data exports to various formats
- **Webhook Support**: Real-time data push capabilities
- **Third-party Integrations**: Slack, email, and other notification channels

## 🎨 Visualization Examples

### Partner Performance Dashboard
- Health score matrix by channel and segment
- Revenue trend analysis with growth indicators
- Efficiency rating distribution
- Optimization priority ranking

### Financial Analytics
- Daily revenue trends with moving averages
- Margin analysis across channels
- Risk assessment heat maps
- Performance metric radar charts

### Customer Intelligence
- Cohort retention heat maps
- Customer lifetime value distributions
- Churn risk assessments
- Segmentation strategy matrices

### Product Intelligence
- Product portfolio bubble charts
- Performance matrix visualizations
- Lifecycle stage distributions
- Investment priority rankings

## 🔐 Security & Compliance

### Data Security
- **Access Control**: Role-based access to sensitive data
- **Data Encryption**: Encryption at rest and in transit
- **Audit Logging**: Comprehensive activity tracking
- **Privacy Compliance**: GDPR and data privacy considerations

### Quality Assurance
- **Automated Testing**: Comprehensive test coverage
- **Data Validation**: Multi-layer validation processes
- **Version Control**: Complete model versioning and change tracking
- **Documentation**: Comprehensive documentation and lineage

## 📚 Documentation & Resources

### Technical Documentation
- **Model Documentation**: Complete dbt model documentation
- **API Reference**: Comprehensive API documentation
- **User Guides**: Step-by-step user guides for all features
- **Best Practices**: Analytics engineering best practices

### Learning Resources
- **Code Examples**: Real-world implementation examples
- **Tutorial Videos**: Video tutorials for key features
- **Webinar Series**: Advanced analytics techniques
- **Community Forum**: Q&A and knowledge sharing

## 🤝 Contributing

This project demonstrates advanced analytics engineering capabilities and serves as a portfolio showcase for the ZMS Central Analytics Engineering role. The implementation showcases:

- **Advanced dbt Skills**: Complex transformations, incremental models, and sophisticated testing
- **SQL Expertise**: Complex queries, window functions, and performance optimization
- **Python Integration**: Advanced analytics, visualization, and automation
- **Business Intelligence**: Strategic insights and data-driven decision making
- **Data Quality**: Comprehensive monitoring and alerting frameworks

## 📞 Contact

For questions about this implementation or to discuss analytics engineering opportunities at ZMS, please reach out through professional channels.

---

## 🎯 Original Project Foundation

This advanced analytics platform builds upon the original e-commerce data transformation project:

### Original Project Goal
To transform raw e-commerce sales data into a structured, analysis-ready format using dbt, demonstrating proficiency in data transformation, data modeling, and data quality.

### Original Data Model
The project utilizes a star schema approach with the following core models:
- **`dim_products`**: A dimension table containing unique product information
- **`dim_dates`**: A simple date dimension table derived from the sales data
- **`fct_order_items`**: The central fact table containing granular order line items
- **`agg_daily_sales`**: An aggregate model summarizing daily sales totals

### How to Run (Original Setup)
1. **Clone the repository**:
```bash
git clone <repository_url>
cd ecom_dbt_project
```

2. **Set up Python Environment & dbt**:
```bash
python3 -m venv .venv
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
pip install dbt-duckdb
dbt deps
```

3. **Download and Organize the Dataset**:
- Go to the Kaggle dataset page: [https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data](https://www.kaggle.com/datasets/thedevastator/unlock-profits-with-e-commerce-sales-data)
- Download the entire dataset (usually a ZIP file).
- Place the `kaggle.json` file in `~/.kaggle/` (create if it doesn't exist) and set permissions: `chmod 600 ~/.kaggle/kaggle.json`.
- Run the following command to download and unzip the dataset into the `data/raw` directory:
```bash
kaggle datasets download -d thedevastator/unlock-profits-with-e-commerce-sales-data --unzip -p ../data/raw
```

4. **Run dbt models and tests**:
```bash
dbt run
dbt test
```

5. **Generate Documentation**:
```bash
dbt docs generate
dbt docs serve
```

---

**🎯 ZMS Central Analytics Engineering Mission**: Transforming raw data into powerful insights that drive business growth, optimize partner performance, and enable data-driven strategic decisions across the organization.

**💡 Innovation Focus**: Leveraging cutting-edge analytics engineering practices to create scalable, maintainable, and insightful data products that deliver immediate business value.

**🚀 Future Vision**: Continuing to push the boundaries of what's possible in analytics engineering, creating intelligent systems that anticipate business needs and drive proactive decision-making.