import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)
from datetime import datetime
import dash
from pathlib import Path
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import dash_bootstrap_components as dbc
from dash import dcc, html, Input, Output, callback, dash_table
import pandas as pd
import duckdb
from forecasting import get_revenue_forecast

class BusinessIntelligenceDashboard:
    """
    Advanced Business Intelligence Dashboard with interactive visualizations
    and real-time analytics capabilities.
    """
    
    def __init__(self, db_path: str, app_title: str = "ZMS Analytics Dashboard"):
        self.db_path = Path(db_path)
        self.app_title = app_title
        self.app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
        self.app.title = app_title
        self.setup_layout()
        self.setup_callbacks()
        
    def get_data(self, query: str) -> pd.DataFrame:
        """Execute query and return DataFrame"""
        print(f"Executing query: {query}")
        try:
            with duckdb.connect(str(self.db_path)) as conn:
                df = conn.execute(query).fetchdf()
                print(f"Query returned {len(df)} rows.")
                return df
        except Exception as e:
            logger.error(f"Error executing query: {str(e)}")
            return pd.DataFrame()
    
    def setup_layout(self):
        """Setup the dashboard layout"""
        self.app.layout = dbc.Container([
            # Header
            dbc.Row([
                dbc.Col([
                    html.H1(self.app_title, className="text-center mb-4"),
                    html.Hr()
                ])
            ]),
            
            # Navigation tabs
            dbc.Row([
                dbc.Col([
                    dbc.Tabs([
                        dbc.Tab(label="Partner Performance", tab_id="partner-performance"),
                        dbc.Tab(label="Financial Analytics", tab_id="financial-analytics"),
                        dbc.Tab(label="Cohort Analysis", tab_id="cohort-analysis"),
                        dbc.Tab(label="Customer Intelligence", tab_id="customer-intelligence"),
                        dbc.Tab(label="Product Intelligence", tab_id="product-intelligence"),
                        dbc.Tab(label="Predictive Analytics", tab_id="predictive-analytics"),
                    ], id="tabs", active_tab="partner-performance")
                ])
            ]),
            
            # Content area
            html.Div(id="tab-content", className="mt-4"),
            
            # Footer
            dbc.Row([
                dbc.Col([
                    html.Hr(),
                    html.P(f"© 2024 ZMS Central Analytics Engineering Team | Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", 
                           className="text-center text-muted")
                ])
            ])
        ], fluid=True)
    
    def setup_callbacks(self):
        """Setup dashboard callbacks"""
        @self.app.callback(
            Output("tab-content", "children"),
            [Input("tabs", "active_tab")]
        )
        def render_tab_content(active_tab):
            if active_tab == "partner-performance":
                return self.create_partner_performance_tab()
            elif active_tab == "financial-analytics":
                return self.create_financial_analytics_tab()
            elif active_tab == "cohort-analysis":
                return self.create_cohort_analysis_tab()
            elif active_tab == "customer-intelligence":
                return self.create_customer_intelligence_tab()
            elif active_tab == "product-intelligence":
                return self.create_product_intelligence_tab()
            elif active_tab == "predictive-analytics":
                return self.create_predictive_analytics_tab()
            return html.Div("Select a tab to view content")
    
    def create_partner_performance_tab(self) -> html.Div:
        """Create partner performance dashboard tab"""
        # Get partner performance data
        partner_data = self.get_data("""
            SELECT * FROM mart_partner_performance_dashboard
            ORDER BY overall_health_score DESC
        """)
        
        if partner_data.empty:
            return html.Div("No partner performance data available")
        
        # Create KPI cards
        kpi_cards = self.create_kpi_cards(partner_data)
        
        # Create partner performance chart
        performance_chart = self.create_partner_performance_chart(partner_data)
        
        # Create partner health matrix
        health_matrix = self.create_partner_health_matrix(partner_data)
        
        # Create trend analysis
        trend_chart = self.create_partner_trend_chart(partner_data)
        
        return html.Div([
            # KPI Cards
            dbc.Row([
                dbc.Col(kpi_cards, width=12)
            ], className="mb-4"),
            
            # Main charts
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Partner Performance Overview", className="card-title"),
                            dcc.Graph(figure=performance_chart)
                        ])
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Partner Health Matrix", className="card-title"),
                            dcc.Graph(figure=health_matrix)
                        ])
                    ])
                ], width=6)
            ], className="mb-4"),
            
            # Trend analysis
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Performance Trends", className="card-title"),
                            dcc.Graph(figure=trend_chart)
                        ])
                    ])
                ], width=12)
            ])
        ])
    
    def create_kpi_cards(self, data: pd.DataFrame) -> dbc.Row:
        """Create KPI cards for key metrics"""
        try:
            total_revenue = data['revenue_30d'].sum()
            total_orders = data['orders_30d'].sum()
            avg_health_score = data['overall_health_score'].mean()
            partners_count = len(data)
            
            cards = [
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"${total_revenue:,.0f}", className="card-title"),
                        html.P("Total Revenue (30d)", className="card-text")
                    ])
                ], color="primary", outline=True),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{total_orders:,.0f}", className="card-title"),
                        html.P("Total Orders (30d)", className="card-text")
                    ])
                ], color="success", outline=True),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{avg_health_score:.1f}", className="card-title"),
                        html.P("Avg Health Score", className="card-text")
                    ])
                ], color="info", outline=True),
                
                dbc.Card([
                    dbc.CardBody([
                        html.H4(f"{partners_count}", className="card-title"),
                        html.P("Active Partners", className="card-text")
                    ])
                ], color="warning", outline=True)
            ]
            
            return dbc.Row([
                dbc.Col(card, width=3) for card in cards
            ])
        
        except Exception as e:
            logger.error(f"Error creating KPI cards: {str(e)}")
            return dbc.Row([])
    
    def create_partner_performance_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create partner performance bubble chart"""
        try:
            fig = px.scatter(
                data,
                x='revenue_30d',
                y='overall_health_score',
                size='orders_30d',
                color='partner_channel',
                hover_data=['customer_segment', 'avg_order_value_30d'],
                title="Partner Performance: Revenue vs Health Score",
                labels={
                    'revenue_30d': 'Revenue (30 days)',
                    'overall_health_score': 'Health Score',
                    'orders_30d': 'Orders Count'
                }
            )
            
            fig.update_layout(
                height=400,
                showlegend=True,
                xaxis_title="Revenue (30 days)",
                yaxis_title="Health Score (0-100)"
            )
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating performance chart: {str(e)}")
            return go.Figure()
    
    def create_partner_health_matrix(self, data: pd.DataFrame) -> go.Figure:
        """Create partner health matrix heatmap"""
        try:
            # Create matrix data
            matrix_data = data.pivot_table(
                values='overall_health_score',
                index='partner_channel',
                columns='customer_segment',
                aggfunc='mean'
            )
            
            fig = px.imshow(
                matrix_data,
                text_auto=True,
                aspect="auto",
                title="Partner Health Matrix by Channel & Segment",
                color_continuous_scale="RdYlGn"
            )
            
            fig.update_layout(height=400)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating health matrix: {str(e)}")
            return go.Figure()
    
    def create_partner_trend_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create partner trend analysis chart"""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=("Revenue Growth", "Order Growth", "Health Score Distribution", "Efficiency Rating"),
                specs=[[{"type": "bar"}, {"type": "bar"}],
                       [{"type": "histogram"}, {"type": "pie"}]]
            )
            
            # Revenue growth
            fig.add_trace(
                go.Bar(
                    x=data['partner_channel'],
                    y=data['revenue_growth_30d'],
                    name="Revenue Growth",
                    marker_color="blue"
                ),
                row=1, col=1
            )
            
            # Order growth
            fig.add_trace(
                go.Bar(
                    x=data['partner_channel'],
                    y=data['order_growth_30d'],
                    name="Order Growth",
                    marker_color="green"
                ),
                row=1, col=2
            )
            
            # Health score distribution
            fig.add_trace(
                go.Histogram(
                    x=data['overall_health_score'],
                    name="Health Score Distribution",
                    nbinsx=20,
                    marker_color="orange"
                ),
                row=2, col=1
            )
            
            # Efficiency rating pie chart
            efficiency_counts = data['efficiency_rating'].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=efficiency_counts.index,
                    values=efficiency_counts.values,
                    name="Efficiency Rating"
                ),
                row=2, col=2
            )
            
            fig.update_layout(height=600, showlegend=False)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating trend chart: {str(e)}")
            return go.Figure()
    
    def create_financial_analytics_tab(self) -> html.Div:
        """Create financial analytics dashboard tab"""
        # Get financial data
        financial_data = self.get_data("""
            SELECT * FROM mart_financial_performance_summary
            WHERE order_date >= current_date - INTERVAL '30' DAY
            ORDER BY order_date DESC
        """)
        
        if financial_data.empty:
            return html.Div("No financial data available")
        
        # Create financial charts
        revenue_trend = self.create_revenue_trend_chart(financial_data)
        margin_analysis = self.create_margin_analysis_chart(financial_data)
        performance_metrics = self.create_financial_metrics_chart(financial_data)
        risk_assessment = self.create_risk_assessment_chart(financial_data)
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Revenue Trend Analysis", className="card-title"),
                            dcc.Graph(figure=revenue_trend)
                        ])
                    ])
                ], width=12)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Margin Analysis", className="card-title"),
                            dcc.Graph(figure=margin_analysis)
                        ])
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Performance Metrics", className="card-title"),
                            dcc.Graph(figure=performance_metrics)
                        ])
                    ])
                ], width=6)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Risk Assessment", className="card-title"),
                            dcc.Graph(figure=risk_assessment)
                        ])
                    ])
                ], width=12)
            ])
        ])
    
    def create_revenue_trend_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create revenue trend chart"""
        try:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=("Daily Revenue", "Revenue Growth Rate"),
                shared_xaxes=True
            )
            
            # Daily revenue
            fig.add_trace(
                go.Scatter(
                    x=data['order_date'],
                    y=data['daily_revenue'],
                    mode='lines+markers',
                    name='Daily Revenue',
                    line=dict(color='blue', width=2)
                ),
                row=1, col=1
            )
            
            # Revenue growth rate
            fig.add_trace(
                go.Scatter(
                    x=data['order_date'],
                    y=data['daily_revenue_growth_rate'],
                    mode='lines+markers',
                    name='Growth Rate',
                    line=dict(color='red', width=2)
                ),
                row=2, col=1
            )
            
            fig.update_layout(height=500, showlegend=True)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating revenue trend chart: {str(e)}")
            return go.Figure()
    
    def create_margin_analysis_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create margin analysis chart"""
        try:
            fig = go.Figure()
            
            # Gross margin
            fig.add_trace(
                go.Scatter(
                    x=data['order_date'],
                    y=data['daily_gross_margin_rate'],
                    mode='lines+markers',
                    name='Gross Margin',
                    line=dict(color='green', width=2)
                )
            )
            
            # Net margin
            fig.add_trace(
                go.Scatter(
                    x=data['order_date'],
                    y=data['daily_net_margin_rate'],
                    mode='lines+markers',
                    name='Net Margin',
                    line=dict(color='orange', width=2)
                )
            )
            
            fig.update_layout(
                title="Margin Analysis Over Time",
                xaxis_title="Date",
                yaxis_title="Margin Rate",
                height=400
            )
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating margin analysis chart: {str(e)}")
            return go.Figure()
    
    def create_financial_metrics_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create financial metrics radar chart"""
        try:
            # Calculate average metrics
            avg_metrics = {
                'Revenue Growth': data['daily_revenue_growth_rate'].mean() * 100,
                'Gross Margin': data['daily_gross_margin_rate'].mean() * 100,
                'Net Margin': data['daily_net_margin_rate'].mean() * 100,
                'Performance Score': data['daily_avg_performance_score'].mean(),
                'Efficiency': data['highly_profitable_sku_rate'].mean() * 100
            }
            
            fig = go.Figure()
            
            fig.add_trace(go.Scatterpolar(
                r=list(avg_metrics.values()),
                theta=list(avg_metrics.keys()),
                fill='toself',
                name='Financial Metrics'
            ))
            
            fig.update_layout(
                polar=dict(
                    radialaxis=dict(
                        visible=True,
                        range=[0, 100]
                    )
                ),
                showlegend=True,
                height=400
            )
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating financial metrics chart: {str(e)}")
            return go.Figure()
    
    def create_risk_assessment_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create risk assessment chart"""
        try:
            # Risk level distribution
            risk_counts = data['financial_risk_level'].value_counts()
            
            fig = px.pie(
                values=risk_counts.values,
                names=risk_counts.index,
                title="Financial Risk Assessment Distribution",
                color_discrete_map={
                    'minimal_risk': 'green',
                    'low_risk': 'lightgreen',
                    'medium_risk': 'yellow',
                    'high_risk': 'red'
                }
            )
            
            fig.update_layout(height=400)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating risk assessment chart: {str(e)}")
            return go.Figure()
    
    def create_cohort_analysis_tab(self) -> html.Div:
        """Create cohort analysis dashboard tab"""
        # Get cohort data
        cohort_data = self.get_data("""
            SELECT * FROM mart_cohort_analysis
            ORDER BY cohort_month DESC, period_number
        """)
        
        if cohort_data.empty:
            return html.Div("No cohort data available")
        
        # Create cohort charts
        retention_heatmap = self.create_cohort_retention_heatmap(cohort_data)
        revenue_cohort = self.create_cohort_revenue_chart(cohort_data)
        ltv_analysis = self.create_ltv_analysis_chart(cohort_data)
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Cohort Retention Heatmap", className="card-title"),
                            dcc.Graph(figure=retention_heatmap)
                        ])
                    ])
                ], width=12)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Revenue Cohort Analysis", className="card-title"),
                            dcc.Graph(figure=revenue_cohort)
                        ])
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Lifetime Value Analysis", className="card-title"),
                            dcc.Graph(figure=ltv_analysis)
                        ])
                    ])
                ], width=6)
            ])
        ])
    
    def create_cohort_retention_heatmap(self, data: pd.DataFrame) -> go.Figure:
        """Create cohort retention heatmap"""
        try:
            # Create retention matrix
            retention_matrix = data.pivot_table(
                values='retention_rate',
                index='cohort_month',
                columns='period_number',
                aggfunc='mean'
            )
            
            fig = px.imshow(
                retention_matrix,
                text_auto='.2%',
                aspect="auto",
                title="Customer Retention Rate by Cohort",
                color_continuous_scale="Blues"
            )
            
            fig.update_layout(height=500)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating cohort retention heatmap: {str(e)}")
            return go.Figure()
    
    def create_cohort_revenue_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create cohort revenue analysis chart"""
        try:
            fig = go.Figure()
            
            # Group by cohort month and show revenue progression
            for cohort_month in data['cohort_month'].unique():
                cohort_subset = data[data['cohort_month'] == cohort_month]
                
                fig.add_trace(
                    go.Scatter(
                        x=cohort_subset['period_number'],
                        y=cohort_subset['ltv_at_period'],
                        mode='lines+markers',
                        name=f'Cohort {cohort_month}',
                        line=dict(width=2)
                    )
                )
            
            fig.update_layout(
                title="Customer Lifetime Value by Cohort",
                xaxis_title="Period (Months)",
                yaxis_title="Lifetime Value",
                height=400
            )
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating cohort revenue chart: {str(e)}")
            return go.Figure()
    
    def create_ltv_analysis_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create LTV analysis chart"""
        try:
            fig = px.box(
                data,
                x='strategic_classification',
                y='predicted_ltv',
                title="Predicted Lifetime Value by Strategic Classification",
                color='strategic_classification'
            )
            
            fig.update_layout(height=400)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating LTV analysis chart: {str(e)}")
            return go.Figure()
    
    def create_customer_intelligence_tab(self) -> html.Div:
        """Create customer intelligence dashboard tab"""
        # Get customer data
        customer_data = self.get_data("""
            SELECT * FROM dim_customer_segments
            ORDER BY estimated_annual_value DESC
        """)
        
        if customer_data.empty:
            return html.Div("No customer data available")
        
        # Create customer intelligence charts
        segment_distribution = self.create_customer_segment_chart(customer_data)
        value_analysis = self.create_customer_value_chart(customer_data)
        behavior_patterns = self.create_customer_behavior_chart(customer_data)
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Customer Segment Distribution", className="card-title"),
                            dcc.Graph(figure=segment_distribution)
                        ])
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Customer Value Analysis", className="card-title"),
                            dcc.Graph(figure=value_analysis)
                        ])
                    ])
                ], width=6)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Customer Behavior Patterns", className="card-title"),
                            dcc.Graph(figure=behavior_patterns)
                        ])
                    ])
                ], width=12)
            ])
        ])
    
    def create_customer_segment_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create customer segment distribution chart"""
        try:
            fig = make_subplots(
                rows=1, cols=2,
                subplot_titles=("Strategic Segments", "CLV Tiers"),
                specs=[[{"type": "pie"}, {"type": "pie"}]]
            )
            
            # Strategic segments
            strategic_counts = data['strategic_segment'].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=strategic_counts.index,
                    values=strategic_counts.values,
                    name="Strategic Segments"
                ),
                row=1, col=1
            )
            
            # CLV tiers
            clv_counts = data['clv_tier'].value_counts()
            fig.add_trace(
                go.Pie(
                    labels=clv_counts.index,
                    values=clv_counts.values,
                    name="CLV Tiers"
                ),
                row=1, col=2
            )
            
            fig.update_layout(height=400, showlegend=True)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating customer segment chart: {str(e)}")
            return go.Figure()
    
    def create_customer_value_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create customer value analysis chart"""
        try:
            fig = px.scatter(
                data,
                x='total_revenue',
                y='estimated_annual_value',
                size='total_orders',
                color='strategic_segment',
                hover_data=['loyalty_tier', 'engagement_level'],
                title="Customer Value: Revenue vs Estimated Annual Value"
            )
            
            fig.update_layout(height=400)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating customer value chart: {str(e)}")
            return go.Figure()
    
    def create_customer_behavior_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create customer behavior patterns chart"""
        try:
            fig = make_subplots(
                rows=2, cols=2,
                subplot_titles=("Channel Preference", "Seasonal Patterns", "Loyalty Distribution", "Churn Risk"),
                specs=[[{"type": "bar"}, {"type": "bar"}],
                       [{"type": "histogram"}, {"type": "pie"}]]
            )
            
            # Channel preference
            channel_counts = data['channel_preference'].value_counts()
            fig.add_trace(
                go.Bar(x=channel_counts.index, y=channel_counts.values, name="Channel Preference"),
                row=1, col=1
            )
            
            # Seasonal patterns
            seasonal_counts = data['seasonal_pattern'].value_counts()
            fig.add_trace(
                go.Bar(x=seasonal_counts.index, y=seasonal_counts.values, name="Seasonal Patterns"),
                row=1, col=2
            )
            
            # Loyalty distribution
            fig.add_trace(
                go.Histogram(x=data['orders_per_month'], name="Orders per Month", nbinsx=20),
                row=2, col=1
            )
            
            # Churn risk
            churn_counts = data['churn_risk_level'].value_counts()
            fig.add_trace(
                go.Pie(labels=churn_counts.index, values=churn_counts.values, name="Churn Risk"),
                row=2, col=2
            )
            
            fig.update_layout(height=600, showlegend=False)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating customer behavior chart: {str(e)}")
            return go.Figure()
    
    def create_product_intelligence_tab(self) -> html.Div:
        """Create product intelligence dashboard tab"""
        # Get product data
        product_data = self.get_data("""
            SELECT * FROM dim_product_intelligence
            ORDER BY product_score DESC
        """)
        
        if product_data.empty:
            return html.Div("No product data available")
        
        # Create product intelligence charts
        product_portfolio = self.create_product_portfolio_chart(product_data)
        performance_matrix = self.create_product_performance_matrix(product_data)
        lifecycle_analysis = self.create_product_lifecycle_chart(product_data)
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Product Portfolio Analysis", className="card-title"),
                            dcc.Graph(figure=product_portfolio)
                        ])
                    ])
                ], width=12)
            ], className="mb-4"),
            
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Performance Matrix", className="card-title"),
                            dcc.Graph(figure=performance_matrix)
                        ])
                    ])
                ], width=6),
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Product Lifecycle Analysis", className="card-title"),
                            dcc.Graph(figure=lifecycle_analysis)
                        ])
                    ])
                ], width=6)
            ])
        ])
    
    def create_product_portfolio_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create product portfolio analysis chart"""
        try:
            fig = px.scatter(
                data,
                x='total_revenue',
                y='gross_margin_rate',
                size='product_score',
                color='strategic_classification',
                hover_data=['product_category', 'velocity_category'],
                title="Product Portfolio: Revenue vs Margin"
            )
            
            fig.update_layout(height=500)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating product portfolio chart: {str(e)}")
            return go.Figure()
    
    def create_product_performance_matrix(self, data: pd.DataFrame) -> go.Figure:
        """Create product performance matrix"""
        try:
            # Create matrix data
            matrix_data = data.pivot_table(
                values='product_score',
                index='product_category',
                columns='strategic_classification',
                aggfunc='mean'
            )
            
            fig = px.imshow(
                matrix_data,
                text_auto=True,
                aspect="auto",
                title="Product Performance Matrix",
                color_continuous_scale="Viridis"
            )
            
            fig.update_layout(height=400)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating product performance matrix: {str(e)}")
            return go.Figure()
    
    def create_product_lifecycle_chart(self, data: pd.DataFrame) -> go.Figure:
        """Create product lifecycle analysis chart"""
        try:
            lifecycle_counts = data['lifecycle_stage'].value_counts()
            
            fig = px.bar(
                x=lifecycle_counts.index,
                y=lifecycle_counts.values,
                title="Product Lifecycle Stage Distribution",
                labels={'x': 'Lifecycle Stage', 'y': 'Number of Products'}
            )
            
            fig.update_layout(height=400)
            
            return fig
        
        except Exception as e:
            logger.error(f"Error creating product lifecycle chart: {str(e)}")
            return go.Figure()
    
    def create_predictive_analytics_tab(self) -> html.Div:
        """Create predictive analytics dashboard tab"""
        # Get revenue data
        revenue_data = self.get_data("SELECT * FROM mart_financial_performance_summary")
        
        if revenue_data.empty:
            return html.Div("No revenue data available for forecasting.")
            
        # Get forecast
        forecast = get_revenue_forecast(revenue_data)
        
        # Create forecast chart
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=revenue_data['order_date'], y=revenue_data['daily_revenue'], name='Actual Revenue'))
        fig.add_trace(go.Scatter(x=forecast.index, y=forecast.values, name='Forecasted Revenue'))
        
        return html.Div([
            dbc.Row([
                dbc.Col([
                    dbc.Card([
                        dbc.CardBody([
                            html.H4("Revenue Forecast", className="card-title"),
                            dcc.Graph(figure=fig)
                        ])
                    ])
                ], width=12)
            ])
        ])
    
    def run_dashboard(self, debug: bool = False, port: int = 8050) -> None:
        """Run the dashboard application"""
        logger.info(f"Starting dashboard on port {port}")
        self.app.run(debug=debug, port=port)


def main():
    """Main function to run the dashboard"""
    import argparse
    
    parser = argparse.ArgumentParser(description="ZMS Business Intelligence Dashboard")
    parser.add_argument("--db-path", required=True, help="Path to the database file")
    parser.add_argument("--port", type=int, default=8050, help="Port to run the dashboard on")
    parser.add_argument("--debug", action="store_true", help="Run in debug mode")
    
    args = parser.parse_args()
    
    # Initialize and run dashboard
    dashboard = BusinessIntelligenceDashboard(args.db_path)
    dashboard.run_dashboard(debug=args.debug, port=args.port)


if __name__ == "__main__":
    main()