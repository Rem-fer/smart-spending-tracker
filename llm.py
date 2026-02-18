import os
import json
from dotenv import load_dotenv
from anthropic import Anthropic
from litellm import completion
from dotenv import load_dotenv
import os
from db_queries import get_spending_by_months, get_spending_by_category
from db_queries import get_total_spending, get_largest_transactions
import sqlite3


load_dotenv()
# client = Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

SYSTEM_PROMPT = """You are a personal finance analyst helping users understand their spending patterns.

   Your role:
   - Analyze transaction data and identify meaningful patterns
   - Highlight unusual spending or potential issues
   - Provide actionable recommendations to improve financial health
   - Be specific with numbers and percentages
   - Keep insights concise (3-5 key points)

   Tone:
   - Helpful and supportive, not judgmental
   - Direct and clear
   - Use British English and £ currency

   Focus areas:
   - Largest spending categories
   - Month-over-month changes
   - Unusual or large transactions
   - Potential savings opportunities
   - Budget adherence

   Format your response as:
   📊 Key Insights:
   1. [Most important finding with specific numbers]
   2. [Second insight]
   3. [Third insight]

   💡 Recommendations:
   - [Actionable suggestion]
   - [Another suggestion]

   Keep it under 200 words."""

def log_api_cost(response):
    """Log LLM API call costs to database."""
    conn = sqlite3.connect('spending.db')
    cursor = conn.cursor()

    cursor.execute("""
        INSERT INTO api_costs 
        (model, input_tokens, output_tokens, total_tokens, cost)
        VALUES (?, ?, ?, ?, ?)
    """, (
        response.model,
        response.usage.prompt_tokens,
        response.usage.completion_tokens,
        response.usage.total_tokens,
        response._hidden_params["response_cost"]
    ))

    conn.commit()
    conn.close()

def categorise_transaction(description):
    """Categorize transaction using Claude API via LiteLLM."""

    prompt = f"""Categorize this bank transaction into ONE of these categories:
- Groceries
- Transport
- Utilities
- Insurance
- Shopping
- Subscriptions
- Entertainment
- Banking
- Income
- Fees
- Transfers
- Housing
- Cash Withdrawal
- Savings
- Uncategorized

Transaction: "{description}"

Return ONLY the category name, nothing else."""

    response = completion(
        model="claude-sonnet-4-20250514",
        messages=[
            {"role": "user", "content": prompt}
        ],
        max_tokens=50,
        api_key=os.getenv("ANTHROPIC_API_KEY")
    )
    log_api_cost(response)
    return response.choices[0].message.content.strip()


def batch_categorise_llm(descriptions):
    """
    Categorize multiple transactions at once.

    Args:
        descriptions (list): List of transaction descriptions

    Returns:
        dict: Mapping of description to category
    """
    # Format descriptions for prompt
    desc_list = "\n".join([f"{i + 1}. {desc}" for i, desc in enumerate(descriptions)])

    prompt = f"""Categorize each of these bank transactions into ONE of these categories:
- Groceries
- Transport
- Utilities
- Insurance
- Shopping
- Subscriptions
- Entertainment
- Banking
- Income
- Fees
- Transfers
- Housing
- Cash Withdrawal
- Savings
- Uncategorized

Transactions:
{desc_list}

Return ONLY a JSON array with the category for each transaction in order.
Example: ["Groceries", "Transport", "Insurance"]

Return only the JSON array, nothing else."""

    response = completion(
        model="claude-sonnet-4-20250514",
        messages=[{"role": "user", "content": prompt}],
        max_tokens=500,
        api_key=os.getenv("ANTHROPIC_API_KEY")
    )

    # Parse JSON response
    response_text = response.choices[0].message.content.strip()
    categories = json.loads(response_text)

    log_api_cost(response)
    # Map descriptions to categories
    return dict(zip(descriptions, categories))


def generate_insights(time_frame="All time"):
    # Get aggregated data
    monthly_trend = get_spending_by_months(time_frame)
    category_spending = get_spending_by_category(time_frame)
    total_spending = get_total_spending(time_frame)
    largest_transactions = get_largest_transactions(time_frame)

    # Format as structured summary
    user_data = f"""
    Time Period: {time_frame}

    Total Spending: £{total_spending}

    Spending by Category:
    {category_spending.to_string(index=False)}

    Monthly Trend:
    {monthly_trend.to_string(index=False)}

    Largest Transactions:
    {largest_transactions[['description', 'amount', 'category']].to_string(index=False)}
    """
    full_response =""
    response = completion(
        model="claude-sonnet-4-20250514",  # or "gpt-4", "llama-3.1-70b-versatile"
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_data}
        ],
        max_tokens=1000,
        api_key=os.getenv("ANTHROPIC_API_KEY")  # or OPENAI_API_KEY, GROQ_API_KEY
    )

    log_api_cost(response)
    return response.choices[0].message.content

