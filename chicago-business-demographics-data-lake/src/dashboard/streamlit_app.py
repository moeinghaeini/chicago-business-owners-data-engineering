"""
Streamlit Dashboard for Chicago Business Demographics Data Lake
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import requests
import json
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Page configuration
st.set_page_config(
    page_title="Chicago Business Demographics Dashboard",
    page_icon="🏢",
    layout="wide",
    initial_sidebar_state="expanded"
)

# API Configuration
API_BASE_URL = "http://localhost:8000"

def fetch_data(endpoint: str):
    """Fetch data from API"""
    try:
        response = requests.get(f"{API_BASE_URL}{endpoint}")
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

def main():
    """Main dashboard application"""
    
    # Header
    st.title("🏢 Chicago Business Demographics Dashboard")
    st.markdown("---")
    
    # Sidebar
    st.sidebar.title("Navigation")
    page = st.sidebar.selectbox(
        "Select Page",
        ["Overview", "Businesses", "Owners", "Analytics", "Data Quality"]
    )
    
    if page == "Overview":
        show_overview()
    elif page == "Businesses":
        show_businesses()
    elif page == "Owners":
        show_owners()
    elif page == "Analytics":
        show_analytics()
    elif page == "Data Quality":
        show_data_quality()

def show_overview():
    """Show overview dashboard"""
    st.header("📊 Overview")
    
    # Fetch demographics summary
    data = fetch_data("/demographics/summary")
    if not data:
        st.error("Unable to fetch overview data")
        return
    
    # Key metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            "Total Businesses",
            f"{data['metadata']['total_businesses']:,}",
            delta=None
        )
    
    with col2:
        st.metric(
            "Total Owners",
            f"{data['metadata']['total_records_analyzed']:,}",
            delta=None
        )
    
    with col3:
        avg_owners = data['ownership_patterns']['avg_owners_per_business']
        st.metric(
            "Avg Owners per Business",
            f"{avg_owners:.1f}",
            delta=None
        )
    
    with col4:
        multi_owner = data['ownership_patterns']['businesses_with_multiple_owners']
        st.metric(
            "Multi-Owner Businesses",
            f"{multi_owner:,}",
            delta=None
        )
    
    # Ownership distribution chart
    st.subheader("Business Ownership Distribution")
    ownership_dist = data['ownership_patterns']['ownership_distribution']
    
    fig = px.bar(
        x=list(ownership_dist.keys()),
        y=list(ownership_dist.values()),
        title="Number of Businesses by Owner Count",
        labels={'x': 'Number of Owners', 'y': 'Number of Businesses'}
    )
    st.plotly_chart(fig, use_container_width=True)
    
    # Role distribution
    st.subheader("Top Business Roles")
    role_dist = data['business_roles']['most_common_roles']
    
    # Create horizontal bar chart
    roles_df = pd.DataFrame(list(role_dist.items()), columns=['Role', 'Count'])
    roles_df = roles_df.head(10)
    
    fig = px.bar(
        roles_df,
        x='Count',
        y='Role',
        orientation='h',
        title="Most Common Business Roles"
    )
    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
    st.plotly_chart(fig, use_container_width=True)

def show_businesses():
    """Show businesses page"""
    st.header("🏢 Businesses")
    
    # Search and filters
    col1, col2 = st.columns([3, 1])
    
    with col1:
        search_term = st.text_input("Search business names", placeholder="Enter business name...")
    
    with col2:
        limit = st.selectbox("Results per page", [50, 100, 200, 500])
    
    # Fetch businesses
    params = {"limit": limit}
    if search_term:
        params["search"] = search_term
    
    # Build query string
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    endpoint = f"/businesses?{query_string}"
    
    data = fetch_data(endpoint)
    if not data:
        st.error("Unable to fetch businesses data")
        return
    
    # Display results
    st.subheader(f"Found {data['pagination']['total_count']:,} businesses")
    
    # Create DataFrame for display
    businesses_df = pd.DataFrame(data['businesses'])
    
    if not businesses_df.empty:
        # Display in a nice format
        for idx, business in businesses_df.iterrows():
            with st.expander(f"🏢 {business['Legal Name']} (Account: {business['Account Number']})"):
                col1, col2 = st.columns(2)
                
                with col1:
                    st.write(f"**Account Number:** {business['Account Number']}")
                    st.write(f"**Legal Name:** {business['Legal Name']}")
                
                with col2:
                    st.write(f"**Number of Owners:** {len(business['Owner Full Name'])}")
                    if business['Owner Full Name']:
                        st.write("**Owners:**")
                        for owner, title in zip(business['Owner Full Name'], business['Title']):
                            st.write(f"  • {owner} ({title})")
    else:
        st.info("No businesses found matching your criteria")

def show_owners():
    """Show owners page"""
    st.header("👥 Business Owners")
    
    # Filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        search_term = st.text_input("Search owner names", placeholder="Enter owner name...")
    
    with col2:
        title_filter = st.text_input("Filter by title", placeholder="e.g., CEO, President...")
    
    with col3:
        limit = st.selectbox("Results per page", [50, 100, 200, 500])
    
    # Fetch owners
    params = {"limit": limit}
    if search_term:
        params["search"] = search_term
    if title_filter:
        params["title_filter"] = title_filter
    
    # Build query string
    query_string = "&".join([f"{k}={v}" for k, v in params.items()])
    endpoint = f"/owners?{query_string}"
    
    data = fetch_data(endpoint)
    if not data:
        st.error("Unable to fetch owners data")
        return
    
    # Display results
    st.subheader(f"Found {data['pagination']['total_count']:,} owners")
    
    # Create DataFrame for display
    owners_df = pd.DataFrame(data['owners'])
    
    if not owners_df.empty:
        # Display in table format
        display_columns = ['Owner Full Name', 'Title', 'Legal Name', 'Account Number']
        available_columns = [col for col in display_columns if col in owners_df.columns]
        
        st.dataframe(
            owners_df[available_columns],
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("No owners found matching your criteria")

def show_analytics():
    """Show analytics page"""
    st.header("📈 Analytics")
    
    # Tabs for different analytics
    tab1, tab2, tab3 = st.tabs(["Ownership Patterns", "Business Roles", "Name Demographics"])
    
    with tab1:
        st.subheader("Ownership Patterns")
        
        # Fetch ownership patterns
        patterns = fetch_data("/analytics/ownership-patterns")
        if patterns:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Businesses", f"{patterns['total_businesses']:,}")
                st.metric("Total Owners", f"{patterns['total_owners']:,}")
                st.metric("Individual Owners", f"{patterns['individual_owners']:,}")
                st.metric("Corporate Owners", f"{patterns['corporate_owners']:,}")
            
            with col2:
                st.metric("Avg Owners per Business", f"{patterns['avg_owners_per_business']:.1f}")
                st.metric("Multi-Owner Businesses", f"{patterns['businesses_with_multiple_owners']:,}")
                st.metric("Single-Owner Businesses", f"{patterns['single_owner_businesses']:,}")
            
            # Ownership distribution chart
            ownership_dist = patterns['ownership_distribution']
            fig = px.pie(
                values=list(ownership_dist.values()),
                names=list(ownership_dist.keys()),
                title="Business Ownership Distribution"
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Business Roles Analysis")
        
        # Fetch role analytics
        roles = fetch_data("/analytics/roles")
        if roles:
            # Most common roles
            common_roles = roles['most_common_roles']
            roles_df = pd.DataFrame(list(common_roles.items()), columns=['Role', 'Count'])
            roles_df = roles_df.head(15)
            
            fig = px.bar(
                roles_df,
                x='Count',
                y='Role',
                orientation='h',
                title="Most Common Business Roles"
            )
            fig.update_layout(yaxis={'categoryorder': 'total ascending'})
            st.plotly_chart(fig, use_container_width=True)
            
            # Leadership vs Ownership roles
            col1, col2 = st.columns(2)
            
            with col1:
                st.subheader("Leadership Roles")
                leadership = roles['leadership_roles']
                if leadership:
                    leadership_df = pd.DataFrame(list(leadership.items()), columns=['Role Type', 'Count'])
                    fig = px.bar(leadership_df, x='Role Type', y='Count', title="Leadership Roles")
                    st.plotly_chart(fig, use_container_width=True)
            
            with col2:
                st.subheader("Ownership Roles")
                ownership = roles['ownership_roles']
                if ownership:
                    ownership_df = pd.DataFrame(list(ownership.items()), columns=['Role Type', 'Count'])
                    fig = px.bar(ownership_df, x='Role Type', y='Count', title="Ownership Roles")
                    st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        st.subheader("Name Demographics")
        
        # Fetch name analytics
        names = fetch_data("/analytics/names")
        if names:
            col1, col2 = st.columns(2)
            
            with col1:
                st.metric("Total Individual Owners", f"{names['total_individual_owners']:,}")
                st.metric("Unique First Names", f"{names['unique_first_names']:,}")
            
            with col2:
                avg_length = names['name_length_stats']['avg_length']
                st.metric("Average Name Length", f"{avg_length:.1f} characters")
            
            # Most common first names
            common_names = names['most_common_first_names']
            if common_names:
                names_df = pd.DataFrame(list(common_names.items()), columns=['Name', 'Count'])
                names_df = names_df.head(20)
                
                fig = px.bar(
                    names_df,
                    x='Count',
                    y='Name',
                    orientation='h',
                    title="Most Common First Names"
                )
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)

def show_data_quality():
    """Show data quality page"""
    st.header("🔍 Data Quality")
    
    st.info("Data quality metrics would be displayed here. This requires implementing the data quality endpoint in the API.")
    
    # Placeholder for data quality metrics
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Data Completeness", "95.2%", delta="+2.1%")
    
    with col2:
        st.metric("Duplicate Records", "1,234", delta="-45")
    
    with col3:
        st.metric("Missing Values", "8,456", delta="-123")
    
    with col4:
        st.metric("Data Freshness", "2 hours", delta="Updated")

if __name__ == "__main__":
    main()
