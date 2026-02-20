# Personal Spending Tracker

AI-powered personal finance dashboard with automated transaction categorization and spending insights.

## Features

- 💰 Multi-account balance tracking with history
- 📊 Automated LLM-based transaction categorization (15 categories)
- 🤖 AI-generated spending insights powered by Claude
- 📈 Interactive charts and trend analysis
- ⚡ Open Banking integration via TrueLayer

## Live Demo

🔗 https://smart-spending-tracker-production.up.railway.app/

![Screenshot 2026-02-20 at 11.52.52.png](Screenshot%202026-02-20%20at%2011.52.52.png)

*Uses TrueLayer sandbox with mock transaction data for demonstration*

## Tech Stack

Python • Streamlit • SQLite • Claude AI • Plotly • TrueLayer API

## Screenshots

[Add screenshots of your dashboard here]

## Key Features

**Smart Categorization**  
Automatically categorizes transactions into Groceries, Transport, Utilities, Insurance, Shopping, Subscriptions, Entertainment, Banking, Income, Fees, Transfers, Housing, Cash Withdrawal, and Savings.

**AI Insights**  
Personalized spending analysis with patterns, trends, and actionable recommendations.

**Balance Tracking**  
Daily balance snapshots with historical trend visualization.

**Cost Monitoring**  
Track LLM API usage and costs in real-time.

## Architecture

- **Frontend**: Streamlit dashboard with interactive visualizations
- **Backend**: Python with SQLite database
- **APIs**: TrueLayer (Open Banking), Anthropic Claude (LLM)
- **Data Processing**: Pandas, batch categorization via LiteLLM

## Portfolio Project

Built as a demonstration of:
- Open Banking API integration
- LLM integration for transaction analysis
- Full-stack Python development
- Production-ready data visualization

Created by Remy Fernando  
MSc Cognitive Science & AI applicant

---

*Note: Uses sandbox banking data for privacy and regulatory compliance*