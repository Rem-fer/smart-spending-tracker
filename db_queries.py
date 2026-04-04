import sqlite3
import pandas as pd
from datetime import datetime, timedelta
import psycopg2
from db import get_connection


def count_nulls(column):
    """Gets Null vales for field selected in as parameter"""
    with get_connection() as conn:
        cursor = conn.cursor()

        cursor.execute(f"SELECT COUNT(*) FROM transactions WHERE {column} IS NULL")
        null_count = cursor.fetchone()[0]
        return null_count

def get_spending_this_week():
    """Query db for total spending of the current week to date"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SUM(amount) FROM finance.transactions 
        WHERE transaction_date >= CURRENT_DATE - INTERVAL '6 days'
        AND amount < 0
    """)
    result = cursor.fetchone()[0]
    cursor.close()
    return abs(result) if result else 0.0


def get_spending_this_month():
    """Query db for total spending of the current month to date"""
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT SUM(amount) FROM finance.transactions 
        WHERE transaction_date >= DATE_TRUNC('month', CURRENT_DATE)
        AND amount < 0
    """)
    result= cursor.fetchone()[0]
    cursor.close()
    return abs(result) if result else 0.0


def get_last_transactions(limit=10):
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT description, transaction_date, amount FROM finance.transactions 
        ORDER BY transaction_date DESC 
        LIMIT %s
    """, (limit,))
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

def get_spending_by_months(time_frame="All time"):
    """Returns spending per month in a df. Only have 4 months for now"""

    days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 3 months": 90,
        "Last 6 months": 180,
        "All time": None
    }
    days = days_map[time_frame]
    if days:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TO_CHAR(timestamp, 'YYYY-MM') AS month,
            ABS(SUM(amount)) AS spending
            FROM finance.transactions
            WHERE amount < 0 AND transaction_date >= %s
            GROUP BY month
            ORDER BY month
        """, (cutoff_date,))
    else:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT TO_CHAR(timestamp, 'YYYY-MM') AS month,
            ABS(SUM(amount)) AS spending
            FROM finance.transactions
            WHERE amount < 0
            GROUP BY month
            ORDER BY month
        """)

    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

def get_spending_by_category(time_frame="All time"):
    """Returns total spending by category in df"""

    # Map selection to SQL date filter
    days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 3 months": 90,
        "Last 6 months": 180,
        "All time": None
    }
    days = days_map[time_frame]
    if days:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')

        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT category, ROUND(ABS(SUM(amount)), 2) AS spending
            FROM finance.transactions
            WHERE amount < 0 AND transaction_date >= %s
            GROUP BY category
            ORDER BY spending DESC
        """, (cutoff_date,))
    else:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT category, ROUND(ABS(SUM(amount)), 2) AS spending
            FROM finance.transactions
            WHERE amount < 0
            GROUP BY category
            ORDER BY spending DESC
        """)

    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

def get_largest_transactions(time_frame="All time"):
    days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 3 months": 90,
        "Last 6 months": 180,
        "All time": None
    }
    days = days_map[time_frame]
    if days:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT transaction_date, description, category, amount
            FROM finance.transactions
            WHERE transaction_date >= %s
            ORDER BY amount DESC
            LIMIT 10
        """, (cutoff_date,))
    else:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT transaction_date, description, category, amount
            FROM finance.transactions
            ORDER BY amount DESC
            LIMIT 10
        """)
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

def get_total_spending(time_frame="All time"):
    days_map = {
        "Last 7 days": 7,
        "Last 30 days": 30,
        "Last 3 months": 90,
        "Last 6 months": 180,
        "All time": None
    }
    days = days_map[time_frame]
    if days:
        cutoff_date = (datetime.now() - timedelta(days=days)).strftime('%Y-%m-%d')
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("""
            SELECT SUM(amount) AS total_spending
            FROM finance.transactions
            WHERE transaction_date >= %s
        """, (cutoff_date,))
    else:
        conn = get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT SUM(amount) as total_spending "
                       "FROM finance.transactions "
                       )
    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)


def get_each_account_balance_history():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM finance.balance_history")

    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)

def get_total_balance_history():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT SUM(current_balance) AS current_balance, snapshot_date "
                   "FROM finance.balance_history "
                   "GROUP BY snapshot_date "
                   "ORDER BY snapshot_date")

    columns = [desc[0] for desc in cursor.description]
    data = cursor.fetchall()
    cursor.close()
    return pd.DataFrame(data, columns=columns)