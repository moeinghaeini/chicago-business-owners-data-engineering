import streamlit as st
import pandas as pd
import plotly.express as px

st.set_page_config(
    page_title="Chicago Business Demographics - Test",
    page_icon="🏢",
    layout="wide"
)

st.title("🏢 Chicago Business Demographics Data Lake - Test Dashboard")

# Create mock data
import random
import numpy as np

# Mock business data
businesses = []
for i in range(100):
    businesses.append({
        'Account Number': 1000 + i,
        'Legal Name': f'Business {i+1} LLC',
        'Total Owners': random.randint(1, 5),
        'Individual Owners': random.randint(0, 3),
        'Corporate Owners': random.randint(0, 2),
        'Diversity Score': round(random.uniform(0.1, 1.0), 2),
        'Complexity Score': round(random.uniform(0.1, 2.0), 2)
    })

df = pd.DataFrame(businesses)

# Display metrics
col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("Total Businesses", f"{len(df):,}", delta="+5.2%")

with col2:
    st.metric("Total Owners", f"{df['Total Owners'].sum():,}", delta="+3.8%")

with col3:
    st.metric("Avg Owners/Business", f"{df['Total Owners'].mean():.1f}", delta="+0.2")

with col4:
    st.metric("Multi-Owner Businesses", f"{len(df[df['Total Owners'] > 1]):,}", delta="+12.5%")

# Display charts
col1, col2 = st.columns(2)

with col1:
    st.subheader("📊 Ownership Distribution")
    ownership_dist = df['Total Owners'].value_counts().sort_index()
    fig = px.pie(
        values=ownership_dist.values,
        names=ownership_dist.index,
        title="Business Ownership Distribution"
    )
    st.plotly_chart(fig, use_container_width=True)

with col2:
    st.subheader("📈 Complexity vs Diversity")
    fig = px.scatter(
        df,
        x='Complexity Score',
        y='Diversity Score',
        size='Total Owners',
        color='Total Owners',
        title="Business Complexity vs Diversity",
        hover_data=['Legal Name']
    )
    st.plotly_chart(fig, use_container_width=True)

# Display data table
st.subheader("🏢 Business Data")
st.dataframe(df.head(20), use_container_width=True)

st.success("✅ Dashboard is working correctly!")
