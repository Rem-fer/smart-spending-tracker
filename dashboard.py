import streamlit as st
import pandas as pd
import numpy as np
from account_data import get_current_balances
from auth import get_access_token
from db_queries import get_spending_this_week, get_spending_this_month, get_last_transactions
from db_queries import get_spending_by_months, get_spending_by_category, get_each_account_balance_history
from db_queries import get_total_balance_history
from datetime import datetime, date
import plotly.express as px
from llm import generate_insights


#---------- FUNCTIONS ----------#

def display_balance_transactions(access_token):
    """
    Display account balances and recent transactions in the Overview tab.

    Shows total balance across all accounts with option to view individual
    account balances. Includes recent transaction history.

    Args:
        access_token (str): Valid TrueLayer access token for API authentication

    Displays:
        - Total balance metric (sum of all accounts)
        - Individual account balances (toggle-able)
        - Last 10 transactions table
        - Week and month spending metrics

    Note:
        Requires active API connection. Shows error if token invalid or
        balance data unavailable.
    """
    #Row 1: Account balances
    st.markdown("## 💰 Balances")

    if access_token:
        with st.spinner("Loading balances..."):
            current_balances = get_current_balances(access_token) # current_balances = get_current_balances(access_token) # Getting current Balance with API call

        if current_balances:
            # Total balance always visible
            st.markdown("### Total Balance")
            total = sum(current_balances.values())
            st.metric(label="Total Balance", value=f"£{total:,.2f}")


            if show_all: # Show_all toggle in sidebar
                st.markdown("#### Individual Accounts")
                # Use columns for horizontal layout
                cols = st.columns(min(len(current_balances), 3))  # Max 3 columns
                for idx, (acc_id, balance) in enumerate(current_balances.items()):
                    with cols[idx % 3]:
                        st.metric(
                            label=f"Account {acc_id[:8]}...",
                            value=f"£{balance:,.2f}"
                        )
        else:
            st.error("Failed to get account balances")
    else:
        st.error("Failed to get access token")


    # Row 3: Last Transactions and Spending
    col1, col2 = st.columns([3,2])
    # Last Transactions
    with col1:
        # Fetching and displaying current balance
        st.markdown("### Last 10 transactions")
        df = get_last_transactions(10)
        styled_df = df.style.map(
            lambda x: 'color: red' if x < 0 else 'color: green' if x > 0 else '',
            subset=['amount']
        ).format({'amount': '£{:.2f}'})
        st.dataframe(styled_df, hide_index=True)

    # Spending
    with col2:
        st.markdown("### Spending")
        dt = datetime.now()
        month_name = dt.strftime('%B')
        weeks_spending = get_spending_this_week()
        month_spending = get_spending_this_month()

        st.metric(label=f"Week {dt.isocalendar()[1]} spending's", value=f"£{weeks_spending:,.2f}")
        st.metric(label=f"{month_name} spending's", value=f"£{month_spending:,.2f}")



def display_spending_trends(time_period):
    """
     Display spending trends and category breakdowns in the Trends tab.

     Shows monthly spending trends over time and spending distribution across
     categories. Adapts visualizations based on selected time period.

     Args:
         time_period (str): Time period for analysis. Options:
             - "Last 7 days"
             - "Last 30 days"
             - "Last 3 months"
             - "Last 6 months"
             - "All time"

     Displays:
         - Monthly spending trend line chart (if period >= 3 months)
         - Spending by category horizontal bar chart (descending order)
         - Highest spending category highlight

     Note:
         Monthly trend chart only available for periods of 3 months or longer.
         Shorter periods show informational message instead.
     """

    st.markdown("## 📊 Spending Trends")

    # Monthly Trend
    st.markdown("### Monthly Trend")

    monthly_spending = get_spending_by_months(time_period)
    if time_period in ["Last 3 months", "Last 6 months", "All time"]:
        st.markdown(f"**For {time_period}**")
        st.line_chart(monthly_spending, x="month", y="spending")
    else:
        st.info("Monthly trend not available for periods under 3 months")

    # Spending by category
    st.markdown("### Spending by category")

    categories_spending = get_spending_by_category(time_period)
    categories_spending_reversed = categories_spending.iloc[::-1] # Reversing so catgories pending in order
    top_category = categories_spending.iloc[0]# Top spending category

    st.markdown(f"""
    🏆 **Highest spending for {time_period}:**  
    {top_category['category']} - £{top_category['spending']:,.2f}
    """)
    fig = px.bar(
        categories_spending_reversed,
        x='spending',
        y='category',
        orientation='h'
    )
    st.plotly_chart(fig)

    # ROW 2: Balance History
    st.markdown("## Balance History")

    # Total Balance history
    st.markdown("### Total balance History")
    total_balance_df = get_total_balance_history()
    fig_1 = px.line(total_balance_df, x='snapshot_date', y='current_balance')
    st.plotly_chart(fig_1)

    # History for each  account if show_all
    if show_all:
        st.markdown("### Balance History for each account")
        each_account_history = get_each_account_balance_history()
        fig_2 = px.line(each_account_history, x='snapshot_date', y='current_balance', color='account_id')
        st.plotly_chart(fig_2)


def display_llm_insights(time_period):
    """
    Display AI-generated spending insights in the AI Insights tab.

    Uses LLM to analyze spending data and generate personalized insights,
    patterns, and recommendations based on transaction history.

    Args:
        time_period (str): Time period for analysis. Options:
            - "Last 7 days"
            - "Last 30 days"
            - "Last 3 months"
            - "Last 6 months"
            - "All time"

    Displays:
        - "Generate Insights" button
        - AI-generated analysis with key findings and recommendations

    Note:
        Requires active internet connection for LLM API calls.
        Insights are generated on-demand when button is clicked.
        Uses Claude Sonnet 4 for analysis.
    """

    st.markdown("## 🤖 AI Insights")

    if st.button("Generate Insights"):
        with st.spinner("Analyzing your spending..."):
            insights = generate_insights(time_period)
            # Display insights
            st.markdown(insights)



#----------DASHBOARD---------#

# Title and get access token for API call
st.markdown("# Personal Finance Dashboard")
access_token = get_access_token()


# Sidebar settings. Control time_period for trends.
with st.sidebar:
    st.markdown("## Filters")
    time_period = st.selectbox(
        "Select time period",
        ["Last 7 days", "Last 30 days", "Last 3 months", "Last 6 months", "All time"]
    )
    show_all = st.toggle("Show All Accounts")
    # Refresh button
    if st.button("🔄 Refresh Data"):
        st.cache_data.clear()
        st.rerun()

# Display data
tab1, tab2, tab3 = st.tabs(["Overview", "Trends", "AI Insights"])
with tab1:
    display_balance_transactions(access_token)

with tab2:
    display_spending_trends(time_period)

with tab3:
    placeholder = st.empty()
    full_text = ""
    display_llm_insights(time_period)



