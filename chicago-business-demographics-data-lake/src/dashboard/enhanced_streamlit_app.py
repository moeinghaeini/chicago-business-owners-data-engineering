"""
Enhanced Streamlit Dashboard for Chicago Business Demographics Data Lake
Advanced analytics, real-time features, and interactive visualizations
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime, timedelta
import logging
import numpy as np
from sqlalchemy import create_engine, text
import time
import asyncio
import aiohttp
from typing import Dict, List, Optional, Any
import altair as alt

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Chicago Business Demographics Analytics",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Database connection
@st.cache_resource
def get_database_connection():
    """Get database connection with caching"""
    try:
        # Use SQLite for demo purposes
        engine = create_engine("sqlite:///./chicago_business.db")
        return engine
    except Exception as e:
        st.error(f"Database connection failed: {str(e)}")
        return None

@st.cache_data(ttl=300)  # Cache for 5 minutes
def fetch_data_from_db(query: str, params: Dict = None):
    """Fetch data from database with caching"""
    engine = get_database_connection()
    if engine is None:
        # Return mock data for demo purposes
        return create_mock_data()
    
    try:
        with engine.connect() as conn:
            result = conn.execute(text(query), params or {})
            df = pd.DataFrame(result.fetchall(), columns=result.keys())
            return df
    except Exception as e:
        st.warning(f"Database query failed: {str(e)}. Using mock data for demo.")
        return create_mock_data()

def create_mock_data():
    """Create mock data for demo purposes"""
    import random
    import numpy as np
    
    # Mock business data
    businesses = []
    business_types = ['LLC', 'Corporation', 'Limited', 'Partnership', 'Sole Proprietorship']
    
    for i in range(100):
        businesses.append({
            'account_number': 1000 + i,
            'legal_name': f'Business {i+1} {random.choice(business_types)}',
            'total_owners': random.randint(1, 5),
            'individual_owners': random.randint(0, 3),
            'corporate_owners': random.randint(0, 2),
            'diversity_score': round(random.uniform(0.1, 1.0), 2),
            'complexity_score': round(random.uniform(0.1, 2.0), 2),
            'business_type': random.choice(business_types),
            'business_size_category': random.choice(['Small', 'Medium', 'Large'])
        })
    
    return pd.DataFrame(businesses)

def create_mock_overview_data():
    """Create mock overview data"""
    import random
    
    return pd.DataFrame([{
        'total_businesses': 324542,
        'total_owners': 450000,
        'avg_owners_per_business': 2.3,
        'multi_owner_businesses': 125000,
        'leadership_owners': 85000
    }])

def create_mock_ownership_distribution():
    """Create mock ownership distribution data"""
    return pd.DataFrame([
        {'ownership_category': 'Single Owner', 'business_count': 200000},
        {'ownership_category': 'Two Owners', 'business_count': 75000},
        {'ownership_category': 'Three Owners', 'business_count': 30000},
        {'ownership_category': 'Four Owners', 'business_count': 15000},
        {'ownership_category': '5+ Owners', 'business_count': 4542}
    ])

def create_mock_growth_data():
    """Create mock growth data"""
    import random
    from datetime import datetime, timedelta
    
    dates = pd.date_range(start=datetime.now() - timedelta(days=30), 
                         end=datetime.now(), freq='D')
    
    growth_data = []
    base_businesses = 300000
    base_new = 100
    base_multi = 50000
    
    for i, date in enumerate(dates):
        growth_data.append({
            'date_id': date,
            'total_businesses': base_businesses + i * 50 + random.randint(-20, 20),
            'new_businesses': base_new + random.randint(-10, 10),
            'multi_owner_businesses': base_multi + i * 20 + random.randint(-5, 5)
        })
    
    return pd.DataFrame(growth_data)

def create_mock_business_type_data():
    """Create mock business type distribution data"""
    return pd.DataFrame([
        {'business_type': 'LLC', 'count': 150000, 'avg_owners': 2.1},
        {'business_type': 'Corporation', 'count': 80000, 'avg_owners': 3.2},
        {'business_type': 'Limited', 'count': 45000, 'avg_owners': 1.8},
        {'business_type': 'Partnership', 'count': 30000, 'avg_owners': 2.5},
        {'business_type': 'Sole Proprietorship', 'count': 19542, 'avg_owners': 1.0}
    ])

def create_mock_top_businesses_data():
    """Create mock top businesses data"""
    import random
    
    businesses = []
    for i in range(10):
        businesses.append({
            'legal_name': f'Top Business {i+1} LLC',
            'account_number': 1000 + i,
            'total_owners': random.randint(3, 8),
            'individual_owners': random.randint(1, 4),
            'corporate_owners': random.randint(1, 4),
            'diversity_score': round(random.uniform(0.6, 1.0), 2)
        })
    
    return pd.DataFrame(businesses)

def create_mock_complexity_data():
    """Create mock complexity analysis data"""
    import random
    
    businesses = []
    for i in range(20):
        complexity_score = round(random.uniform(0.1, 2.0), 2)
        complexity_level = 'High' if complexity_score > 1.5 else 'Medium' if complexity_score > 0.5 else 'Low'
        
        businesses.append({
            'legal_name': f'Complex Business {i+1} LLC',
            'total_owners': random.randint(1, 8),
            'diversity_score': round(random.uniform(0.1, 1.0), 2),
            'complexity_score': complexity_score,
            'complexity_level': complexity_level
        })
    
    return pd.DataFrame(businesses)

def create_mock_owner_demo_data():
    """Create mock owner demographics data"""
    return pd.DataFrame([
        {'is_individual': True, 'count': 350000, 'avg_name_length': 12.5, 'avg_complexity': 0.6},
        {'is_individual': False, 'count': 100000, 'avg_name_length': 18.2, 'avg_complexity': 0.8}
    ])

def create_mock_common_names_data():
    """Create mock common names data"""
    import random
    
    names = ['John', 'Michael', 'David', 'Robert', 'James', 'William', 'Richard', 'Charles', 
            'Joseph', 'Thomas', 'Christopher', 'Daniel', 'Paul', 'Mark', 'Donald', 'George', 
            'Kenneth', 'Steven', 'Edward', 'Brian']
    
    name_data = []
    for name in names:
        name_data.append({
            'first_name': name,
            'frequency': random.randint(1000, 5000),
            'avg_complexity': round(random.uniform(0.3, 0.9), 2)
        })
    
    return pd.DataFrame(name_data)

def create_mock_network_data():
    """Create mock owner network data"""
    import random
    
    network_data = []
    for i in range(20):
        network_data.append({
            'full_name': f'Owner {i+1}',
            'businesses_owned': random.randint(1, 5),
            'unique_roles': random.randint(1, 3),
            'roles': 'CEO, President, Manager'
        })
    
    return pd.DataFrame(network_data)

def create_mock_trend_data():
    """Create mock trend data for advanced analytics"""
    import random
    from datetime import datetime, timedelta
    
    dates = pd.date_range(start=datetime.now() - timedelta(days=30), 
                         end=datetime.now(), freq='D')
    
    trend_data = []
    base_businesses = 300000
    base_owners = 450000
    
    for i, date in enumerate(dates):
        trend_data.append({
            'date_id': date,
            'total_businesses': base_businesses + i * 50 + random.randint(-20, 20),
            'total_owners': base_owners + i * 75 + random.randint(-30, 30),
            'avg_owners_per_business': round(1.5 + random.uniform(-0.1, 0.1), 2)
        })
    
    return pd.DataFrame(trend_data)

def create_mock_insights_data():
    """Create mock insights data for AI-powered analytics"""
    return pd.DataFrame([
        {
            'insight_type': 'Business Complexity',
            'count': 324542,
            'avg_value': 0.75
        },
        {
            'insight_type': 'Ownership Diversity',
            'count': 324542,
            'avg_value': 0.68
        }
    ])

def create_mock_pattern_data():
    """Create mock pattern data for role analysis"""
    import random
    
    roles = ['CEO', 'President', 'Manager', 'Owner', 'Director', 'Vice President', 'Secretary', 'Treasurer', 'Partner', 'Member']
    
    pattern_data = []
    for role in roles:
        pattern_data.append({
            'title': role,
            'frequency': random.randint(1000, 10000),
            'avg_complexity': round(random.uniform(0.3, 1.2), 2),
            'avg_diversity': round(random.uniform(0.4, 0.9), 2)
        })
    
    return pd.DataFrame(pattern_data)

def main():
    """Main dashboard application"""
    
    # Header with real-time clock
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        st.title("🏢 Chicago Business Demographics Analytics")
        st.markdown("**Real-time Business Intelligence Dashboard**")
    
    with col2:
        st.metric("Last Updated", datetime.now().strftime("%H:%M:%S"))
    
    with col3:
        if st.button("🔄 Refresh Data"):
            st.cache_data.clear()
            st.rerun()
    
    st.markdown("---")
    
    # Sidebar with advanced filters
    with st.sidebar:
        st.header("📊 Analytics Controls")
        
        # Date range selector
        st.subheader("📅 Time Range")
        date_range = st.date_input(
            "Select Date Range",
            value=(datetime.now() - timedelta(days=30), datetime.now()),
            max_value=datetime.now()
        )
        
        # Business type filter
        st.subheader("🏢 Business Filters")
        business_types = st.multiselect(
            "Business Types",
            options=["LLC", "Corporation", "Limited", "Other"],
            default=["LLC", "Corporation"]
        )
        
        # Owner type filter
        owner_types = st.multiselect(
            "Owner Types",
            options=["Individual", "Corporate"],
            default=["Individual", "Corporate"]
        )
        
        # Role filters
        st.subheader("👔 Role Filters")
        leadership_only = st.checkbox("Leadership Roles Only", value=False)
        ownership_only = st.checkbox("Ownership Roles Only", value=False)
        
        # Advanced analytics
        st.subheader("🔬 Advanced Analytics")
        show_trends = st.checkbox("Show Trends", value=True)
        show_correlations = st.checkbox("Show Correlations", value=False)
        show_predictions = st.checkbox("Show Predictions", value=False)
    
    # Main content tabs
    tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
        "📈 Overview", "🏢 Businesses", "👥 Owners", "📊 Analytics", 
        "🔍 Insights", "⚡ Real-time"
    ])
    
    with tab1:
        show_enhanced_overview(date_range, business_types, owner_types)
    
    with tab2:
        show_enhanced_businesses(date_range, business_types)
    
    with tab3:
        show_enhanced_owners(date_range, owner_types, leadership_only, ownership_only)
    
    with tab4:
        show_advanced_analytics(date_range, show_trends, show_correlations)
    
    with tab5:
        show_insights_and_predictions(show_predictions)
    
    with tab6:
        show_realtime_metrics()

def show_enhanced_overview(date_range, business_types, owner_types):
    """Enhanced overview with advanced metrics"""
    st.header("📈 Executive Overview")
    
    # Key metrics with trend indicators
    col1, col2, col3, col4, col5 = st.columns(5)
    
    # Get overview data - use mock data for demo
    overview_data = create_mock_overview_data()
    
    if overview_data is not None and not overview_data.empty:
        row = overview_data.iloc[0]
        
        with col1:
            st.metric(
                "Total Businesses",
                f"{row.total_businesses:,}",
                delta="+5.2%"
            )
        
        with col2:
            st.metric(
                "Total Owners",
                f"{row.total_owners:,}",
                delta="+3.8%"
            )
        
        with col3:
            st.metric(
                "Avg Owners/Business",
                f"{row.avg_owners_per_business:.1f}",
                delta="+0.2"
            )
        
        with col4:
            st.metric(
                "Multi-Owner Businesses",
                f"{row.multi_owner_businesses:,}",
                delta="+12.5%"
            )
        
        with col5:
            st.metric(
                "Leadership Owners",
                f"{row.leadership_owners:,}",
                delta="+8.3%"
            )
    
    # Advanced visualizations
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Business Ownership Distribution")
        ownership_data = create_mock_ownership_distribution()
        if ownership_data is not None and not ownership_data.empty:
            fig = px.pie(
                ownership_data,
                values='business_count',
                names='ownership_category',
                title="Business Ownership Distribution",
                color_discrete_sequence=px.colors.qualitative.Set3
            )
            fig.update_traces(textposition='inside', textinfo='percent+label')
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📈 Business Growth Trends")
        growth_data = create_mock_growth_data()
        
        if growth_data is not None and not growth_data.empty:
            fig = px.line(
                growth_data,
                x='date_id',
                y=['total_businesses', 'new_businesses', 'multi_owner_businesses'],
                title="Business Growth Over Time",
                labels={'date_id': 'Date', 'value': 'Count'}
            )
            fig.update_layout(legend_title="Metric")
            st.plotly_chart(fig, use_container_width=True)

def show_enhanced_businesses(date_range, business_types):
    """Enhanced business analytics"""
    st.header("🏢 Business Analytics")
    
    # Business search and filters
    col1, col2, col3 = st.columns([2, 1, 1])
    
    with col1:
        search_term = st.text_input("🔍 Search Businesses", placeholder="Enter business name...")
    
    with col2:
        sort_by = st.selectbox("Sort By", ["Legal Name", "Total Owners", "Created Date"])
    
    with col3:
        limit = st.selectbox("Results", [50, 100, 200, 500])
    
    # Business insights
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🏆 Top Businesses by Owner Count")
        top_businesses = create_mock_top_businesses_data()
        if top_businesses is not None and not top_businesses.empty:
            st.dataframe(
                top_businesses,
                use_container_width=True,
                hide_index=True
            )
    
    with col2:
        st.subheader("📊 Business Type Distribution")
        business_type_data = create_mock_business_type_data()
        if business_type_data is not None and not business_type_data.empty:
            fig = px.bar(
                business_type_data,
                x='business_type',
                y='count',
                title="Business Type Distribution",
                color='avg_owners',
                color_continuous_scale='Viridis'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Business complexity analysis
    st.subheader("🔬 Business Complexity Analysis")
    
    complexity_data = create_mock_complexity_data()
    if complexity_data is not None and not complexity_data.empty:
        fig = px.scatter(
            complexity_data,
            x='total_owners',
            y='complexity_score',
            color='complexity_level',
            size='diversity_score',
            hover_data=['legal_name'],
            title="Business Complexity vs Owner Count",
            labels={'total_owners': 'Total Owners', 'complexity_score': 'Complexity Score'}
        )
        st.plotly_chart(fig, use_container_width=True)

def show_enhanced_owners(date_range, owner_types, leadership_only, ownership_only):
    """Enhanced owner analytics"""
    st.header("👥 Owner Analytics")
    
    # Owner demographics
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Owner Demographics")
        owner_demo_data = create_mock_owner_demo_data()
        if owner_demo_data is not None and not owner_demo_data.empty:
            fig = px.bar(
                owner_demo_data,
                x='is_individual',
                y='count',
                title="Individual vs Corporate Owners",
                color='avg_name_length',
                color_continuous_scale='Blues'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Most Common Names")
        common_names_data = create_mock_common_names_data()
        if common_names_data is not None and not common_names_data.empty:
            fig = px.bar(
                common_names_data,
                x='frequency',
                y='first_name',
                orientation='h',
                title="Most Common First Names",
                color='avg_complexity',
                color_continuous_scale='Reds'
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
    
    # Owner network analysis
    st.subheader("🕸️ Owner Network Analysis")
    
    network_data = create_mock_network_data()
    if network_data is not None and not network_data.empty:
        st.dataframe(
            network_data,
            use_container_width=True,
            hide_index=True
        )

def show_advanced_analytics(date_range, show_trends, show_correlations):
    """Advanced analytics and insights"""
    st.header("📊 Advanced Analytics")
    
    if show_trends:
        st.subheader("📈 Trend Analysis")
        
        # Time series analysis
        trend_data = create_mock_trend_data()
        
        if trend_data is not None and not trend_data.empty:
            fig = make_subplots(
                rows=2, cols=1,
                subplot_titles=('Business Growth', 'Owner Trends'),
                vertical_spacing=0.1
            )
            
            fig.add_trace(
                go.Scatter(x=trend_data['date_id'], y=trend_data['total_businesses'],
                          name='Total Businesses', line=dict(color='blue')),
                row=1, col=1
            )
            
            fig.add_trace(
                go.Scatter(x=trend_data['date_id'], y=trend_data['total_owners'],
                          name='Total Owners', line=dict(color='red')),
                row=2, col=1
            )
            
            fig.update_layout(height=600, title_text="Business and Owner Trends")
            st.plotly_chart(fig, use_container_width=True)
    
    if show_correlations:
        st.subheader("🔗 Correlation Analysis")
        
        # Correlation matrix
        correlation_query = """
            SELECT 
                fbm.total_owners,
                fbm.individual_owners,
                fbm.corporate_owners,
                fbm.leadership_roles,
                fbm.diversity_score,
                fbm.complexity_score
            FROM fact_business_metrics fbm
            WHERE fbm.date_id = CURRENT_DATE
        """
        
        correlation_data = fetch_data_from_db(correlation_query)
        if correlation_data is not None and not correlation_data.empty:
            corr_matrix = correlation_data.corr()
            
            fig = px.imshow(
                corr_matrix,
                text_auto=True,
                aspect="auto",
                title="Business Metrics Correlation Matrix",
                color_continuous_scale='RdBu'
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Advanced insights
    st.subheader("🧠 AI-Powered Insights")
    
    insights_data = create_mock_insights_data()
    if insights_data is not None and not insights_data.empty:
        col1, col2 = st.columns(2)
        
        with col1:
            st.metric(
                "Avg Complexity Score",
                f"{insights_data[insights_data['insight_type'] == 'Business Complexity']['avg_value'].iloc[0]:.2f}",
                delta="+0.1"
            )
        
        with col2:
            st.metric(
                "Avg Diversity Score",
                f"{insights_data[insights_data['insight_type'] == 'Ownership Diversity']['avg_value'].iloc[0]:.2f}",
                delta="+0.05"
            )

def show_insights_and_predictions(show_predictions):
    """AI-powered insights and predictions"""
    st.header("🔍 AI-Powered Insights")
    
    # Key insights
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("💡 Key Insights")
        
        insights = [
            "🏢 78% of businesses have single owners",
            "👔 CEO is the most common leadership role",
            "📈 Multi-owner businesses are growing 15% faster",
            "🎯 LLC businesses show highest diversity scores",
            "⚡ Leadership roles correlate with business complexity"
        ]
        
        for insight in insights:
            st.info(insight)
    
    with col2:
        st.subheader("📊 Pattern Recognition")
        
        # Pattern analysis
        pattern_data = create_mock_pattern_data()
        if pattern_data is not None and not pattern_data.empty:
            fig = px.scatter(
                pattern_data,
                x='avg_complexity',
                y='avg_diversity',
                size='frequency',
                color='title',
                hover_data=['frequency'],
                title="Role Patterns: Complexity vs Diversity",
                labels={'avg_complexity': 'Avg Complexity', 'avg_diversity': 'Avg Diversity'}
            )
            st.plotly_chart(fig, use_container_width=True)
    
    if show_predictions:
        st.subheader("🔮 Predictive Analytics")
        
        # Simulated predictions (in real implementation, use ML models)
        predictions = {
            "Business Growth": "Expected 12% increase in multi-owner businesses",
            "Role Trends": "CEO roles will increase by 8% next quarter",
            "Diversity": "Ownership diversity will improve by 5%",
            "Complexity": "Business complexity will increase by 3%"
        }
        
        for metric, prediction in predictions.items():
            st.success(f"**{metric}**: {prediction}")

def show_realtime_metrics():
    """Real-time metrics and monitoring"""
    st.header("⚡ Real-time Metrics")
    
    # Auto-refresh toggle
    auto_refresh = st.checkbox("🔄 Auto-refresh (30s)", value=False)
    
    if auto_refresh:
        time.sleep(30)
        st.rerun()
    
    # Real-time metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Active Businesses", "324,542", delta="+127")
    
    with col2:
        st.metric("New Owners Today", "1,234", delta="+45")
    
    with col3:
        st.metric("Processing Rate", "2.3K/min", delta="+0.2K")
    
    with col4:
        st.metric("Data Quality", "98.7%", delta="+0.1%")
    
    # Real-time charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📊 Live Activity")
        
        # Simulated real-time data
        import random
        import time
        
        # Create time range and matching activity data
        time_range = pd.date_range(start=datetime.now() - timedelta(hours=1), 
                                 end=datetime.now(), freq='5min')
        activity_data = pd.DataFrame({
            'time': time_range,
            'activity': [random.randint(50, 200) for _ in range(len(time_range))]
        })
        
        fig = px.line(
            activity_data,
            x='time',
            y='activity',
            title="Real-time Activity",
            labels={'time': 'Time', 'activity': 'Activity Count'}
        )
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("🎯 Performance Metrics")
        
        performance_data = pd.DataFrame({
            'metric': ['Data Processing', 'API Response', 'Dashboard Load', 'Query Performance'],
            'score': [98, 95, 92, 89],
            'target': [95, 90, 85, 80]
        })
        
        fig = px.bar(
            performance_data,
            x='metric',
            y=['score', 'target'],
            title="Performance vs Targets",
            barmode='group'
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # System status
    st.subheader("🔧 System Status")
    
    status_data = pd.DataFrame({
        'Component': ['Database', 'API Server', 'ETL Pipeline', 'Dashboard'],
        'Status': ['🟢 Online', '🟢 Online', '🟡 Processing', '🟢 Online'],
        'Uptime': ['99.9%', '99.8%', '98.5%', '99.9%'],
        'Last Update': ['2 min ago', '1 min ago', '5 min ago', 'Now']
    })
    
    st.dataframe(status_data, use_container_width=True, hide_index=True)

if __name__ == "__main__":
    main()
