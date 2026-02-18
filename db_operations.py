import sqlite3
from account_data import fetching_all_transactions, get_all_accounts_balance
from llm import batch_categorise_llm
from datetime import datetime

def create_transactions_database():
    conn = sqlite3.connect("spending.db")
    cursor = conn.cursor()
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id TEXT PRIMARY KEY,
            account_id TEXT,
            amount REAL,
            currency TEXT,
            description TEXT,
            transaction_date TEXT,
            timestamp TEXT,
            transaction_type TEXT,
            category TEXT,
            merchant_name TEXT
        )
    ''')
    conn.commit()
    conn.close()
    print("Database created successfully")

def create_balances_table():
    conn = sqlite3.connect("spending.db")
    cursor = conn.cursor()
    cursor.execute( ''' 
        CREATE TABLE IF NOT EXISTS balance_history (
            account_id TEXT NOT NULL,
            current_balance REAL,
            available_balance REAL,
            overdraft_limit REAL,
            snapshot_date TEXT NOT NULL,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,  -- When record was created
            PRIMARY KEY (account_id, snapshot_date)
        )
    ''')
    conn.commit()
    conn.close()
    print("Database created successfully")

#Create api_cost table
# conn = sqlite3.connect("spending.db")
# cursor = conn.cursor()
#
# cursor.execute("""
#     CREATE TABLE IF NOT EXISTS api_costs (
#         id INTEGER PRIMARY KEY AUTOINCREMENT,
#         model TEXT NOT NULL,
#         input_tokens INTEGER,
#         output_tokens INTEGER,
#         total_tokens INTEGER,
#         cost REAL,
#         timestamp TEXT DEFAULT CURRENT_TIMESTAMP
#     )
# """)
# conn.commit()
# conn.close()
# print("Database created successfully")

def save_single_transaction_to_db(transaction, account_id):
    """Save a single transaction to the database."""
    conn = sqlite3.connect('spending.db')
    cursor = conn.cursor()

    # Extract date from timestamp (YYYY-MM-DD)
    transaction_date = transaction['timestamp'][:10]

    # Extract running balance if exists
    running_balance = transaction.get('running_balance', {}).get('amount')

    try:
        cursor.execute('''
            INSERT OR IGNORE INTO transactions
            (transaction_id, account_id, amount, currency, description,
             transaction_date, timestamp, transaction_type, category,
             merchant_name)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            transaction['transaction_id'],
            account_id,
            transaction['amount'],
            transaction['currency'],
            transaction['description'],
            transaction_date,
            transaction['timestamp'],
            transaction['transaction_type'],
            None,  # category - will categorize later
            None,  # merchant_name - will extract later
        ))

        conn.commit()
        return True
    except sqlite3.Error as e:
        print(f"Error saving transaction: {e}")
        return False
    finally:
        conn.close()


def save_all_transactions_to_db(access_token):
    """
    Fetch and save all transactions for all accounts to the database.

    Args:
        access_token (str): Valid TrueLayer access token

    Returns:
        list: Transaction IDs that failed to save (empty if all successful)
        True: If all transactions saved successfully
    """
    all_transactions = fetching_all_transactions(access_token)
    failed_transactions = []
    saved_count = 0

    if not all_transactions:
        print("No transactions found")
        return []

    for account_id in all_transactions:
        if not all_transactions[account_id]:
            print(f"No transactions for account: {account_id}")
            continue

        for transaction in all_transactions[account_id]:
            try:
                save_single_transaction_to_db(transaction, account_id)
                saved_count += 1
            except sqlite3.Error as e:
                print(f"Database error for transaction {transaction['transaction_id']}: {e}")
                failed_transactions.append(transaction["transaction_id"])

    print(f"Successfully saved {saved_count} transactions")

    if failed_transactions:
        print(f"Failed to save {len(failed_transactions)} transactions")
        return failed_transactions

    return True


#
# def update_all_categories():
#     """Update categories for all transactions in batches."""
#     conn = sqlite3.connect('spending.db')
#     cursor = conn.cursor()
#
#     # Get all transactions without categories
#     cursor.execute("SELECT transaction_id, description FROM transactions WHERE category IS NULL")
#     transactions = cursor.fetchall()
#
#     print(f"Categorizing {len(transactions)} transactions...")
#
#     for trans_id, description in transactions:
#         category = categorize_with_llm(description)
#
#         # Update the transaction
#         cursor.execute(
#             "UPDATE transactions SET category = ? WHERE transaction_id = ?",
#             (category, trans_id)
#         )
#
#     conn.commit()
#     conn.close()
#     print("Done!")

def update_all_categories_batch():
    """Update categories for all transactions using batch processing."""
    conn = sqlite3.connect('spending.db')
    cursor = conn.cursor()

    cursor.execute("SELECT transaction_id, description FROM transactions WHERE category IS NULL")
    transactions = cursor.fetchall()

    print(f"Categorizing {len(transactions)} transactions...")

    batch_size = 50
    total_updated = 0

    for i in range(0, len(transactions), batch_size):
        batch = transactions[i:i + batch_size]
        descriptions = [desc for _, desc in batch]

        # Get categories for batch
        category_map = batch_categorise_llm(descriptions)

        # Update database
        for trans_id, description in batch:
            category = category_map.get(description, 'Uncategorized')
            cursor.execute(
                "UPDATE transactions SET category = ? WHERE transaction_id = ?",
                (category, trans_id)
            )
            total_updated += 1

        conn.commit()
        print(f"Processed {total_updated}/{len(transactions)} transactions...")

    conn.close()
    print("Done!")

def get_random_transactions(number):
    conn = sqlite3.connect('spending.db')
    cursor = conn.cursor()

    # cursor.execute("SELECT description, category FROM transactions LIMIT 100")
    cursor.execute("""
        SELECT description, category FROM transactions 
        ORDER BY RANDOM() 
        LIMIT ?
    """,(number,))

    for row in cursor.fetchall():
        print(tuple(row))

    conn.close()

def save_daily_balance_snapshot(access_token):
    """
    Save daily balance snapshot for all accounts.

    Args:
        access_token (str): Valid TrueLayer access token

    Note:
        Should be run once per day (e.g., midnight via cron job).
        Uses INSERT OR IGNORE to prevent duplicates if run multiple times.
    """
    balances = get_all_accounts_balance(access_token)

    if not balances:
        print("No balances to save")
        return

    conn = sqlite3.connect('spending.db')
    cursor = conn.cursor()

    snapshot_date = datetime.now().date().isoformat()  # YYYY-MM-DD format

    for account_id, balance_info in balances.items():
        cursor.execute("""
            INSERT OR IGNORE INTO balance_history
            (account_id, current_balance, available_balance, overdraft_limit, snapshot_date)
            VALUES (?, ?, ?, ?, ?)
        """, (
            account_id,
            balance_info.get('current'),
            balance_info.get('available'),
            balance_info.get('overdraft'),
            snapshot_date
        ))

    conn.commit()
    conn.close()
    print(f"Saved balance snapshot for {len(balances)} accounts on {snapshot_date}")