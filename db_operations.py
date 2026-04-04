import psycopg2
from db import get_connection
from account_data import fetching_all_transactions
from llm import batch_categorise_llm, extract_merchants_from_descriptions
from account_data import get_all_accounts_balance
from datetime import datetime


def save_single_transaction_to_db(transaction, account_id, provider, conn):
    """Save a single transaction to the database."""

    cursor = conn.cursor()

    # Extract date from timestamp (YYYY-MM-DD)
    transaction_date = transaction['timestamp'][:10]

    # Extract running balance if exists
    running_balance = transaction.get('running_balance', {}).get('amount')

    try:
        cursor.execute("""
    INSERT INTO finance.transactions
    (transaction_id, account_id, amount, currency, description,
     transaction_date, timestamp, transaction_type, category
     , provider)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
    """, (
            transaction['transaction_id'],
            account_id,
            transaction['amount'],
            transaction['currency'],
            transaction['description'],
            transaction_date,
            transaction['timestamp'],
            transaction['transaction_type'],
            None,  # category - will categorize later
            provider
        ))
        return True
    except psycopg2.Error as e:
        print(f"Error saving transaction: {e}")
        return False



def save_all_transactions_to_db(all_transactions, provider):
    """
    Save all transactions for all accounts to the database.

    Args:
        all_transactions (dict): Dictionary mapping account IDs to transaction lists.
                                 Format: {account_id: [transaction1, transaction2, ...]}
        provider (str): Provider identifier e.g. 'truelayer_barclays', 'truelayer_revolut'

    Returns:
        list: Transaction IDs that failed to save (empty if all successful)
        True: If all transactions saved successfully
    """
    print("Fetching all transactions...")
    print(f"Got transactions: {all_transactions is not None}")
    failed_transactions = []
    saved_count = 0

    if not all_transactions:
        print("No transactions found")
        return []

    conn = get_connection()

    try:
        for account_id in all_transactions:
            # print(f"Processing account: {account_id}")
            if not all_transactions[account_id]:
                print(f"No transactions for account: {account_id}")
                continue

            for transaction in all_transactions[account_id]:
                # print(f"Saving transaction: {transaction['transaction_id']}")
                try:
                    result = save_single_transaction_to_db(transaction, account_id, provider, conn)
                    # print(f"Saved: {transaction['transaction_id']} - {result}")
                    saved_count += 1
                except psycopg2.Error as e:
                    print(f"Database error for transaction {transaction['transaction_id']}: {e}")
                    failed_transactions.append(transaction["transaction_id"])
        conn.commit()
        print(f"Successfully saved {saved_count} transactions")
    finally:
        conn.close()

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
    conn = get_connection()
    cursor = conn.cursor()

    cursor.execute("SELECT transaction_id, description FROM finance.transactions WHERE category IS NULL")
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
                "UPDATE finance.transactions SET category = %s WHERE transaction_id = %s",
                (category, trans_id)
            )
            total_updated += 1

        conn.commit()
        print(f"Processed {total_updated}/{len(transactions)} transactions...")

    conn.close()
    print("Done!")

def get_random_transactions(number):
    conn = get_connection()
    cursor = conn.cursor()

    # cursor.execute("SELECT description, category FROM transactions LIMIT 100")
    cursor.execute("""
        SELECT description, category FROM finance.transactions 
        ORDER BY RANDOM() 
        LIMIT %s
    """,(number,))

    for row in cursor.fetchall():
        print(tuple(row))

    conn.close()

def save_daily_balance_snapshot(balances):
    """
    Save daily balance snapshot for all accounts.

    Args:
        balances (dict): Dictionary mapping account IDs to balance info.
                        Format: {account_id: {'current': 22.0, 'available': 222.0, ...}}

    Note:
        Should be run once per day (e.g., midnight via cron job).
        Uses ON CONFLICT to prevent duplicates if run multiple times.
    """
    if not balances:
        print("No balances to save")
        return

    conn = get_connection()
    cursor = conn.cursor()

    snapshot_date = datetime.now().date().isoformat()  # YYYY-MM-DD format

    for account_id, balance_info in balances.items():
        cursor.execute("""
            INSERT INTO finance.balance_history
            (account_id, currency, current_balance, available_balance, overdraft_limit, credit_limit, snapshot_date)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (account_id, snapshot_date) DO NOTHING
        """, (
            account_id,
            balance_info.get('currency'),
            balance_info.get('current'),
            balance_info.get('available'),
            balance_info.get('overdraft'),
            balance_info.get('credit_limit'),
            snapshot_date
        ))

    conn.commit()
    conn.close()
    print(f"Saved balance snapshot for {len(balances)} accounts on {snapshot_date}")

def categorise_transfers():
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
                UPDATE finance.transactions 
            SET category = 'Transfer'
            WHERE description LIKE 'OBA topup%'
            OR description LIKE '%TO REVOLUT%'
            OR description LIKE '%BARCLAYCARD%'
            """)
        conn.commit()
        print(f"Categorised {cursor.rowcount} transfers")
    except psycopg2.Error as e:
        print(f"Failed to flag transfers: {e}")
    finally:
        conn.close()

def categorise_bcard_payments():
    """Flag Barclaycard bill payments as Credit Card Payment category on both sides."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE finance.transactions 
            SET category = 'Barclaycard Payment'
            WHERE (description LIKE '%BBP%' AND provider = 'BARCLAYS')
            OR (description LIKE '%Thank You%' AND provider = 'BARCLAYCARD')
        """)
        conn.commit()
        print(f"Flagged {cursor.rowcount} credit card payments")
    except psycopg2.Error as e:
        print(f"Failed to flag credit card payments: {e}")
    finally:
        conn.close()

def save_new_descriptions():
    conn = get_connection()
    cursor = conn.cursor()
    cursor.execute("""
    INSERT INTO finance.merchants (description, merchant_name, category)
    SELECT DISTINCT description, NULL, NULL
    FROM finance.transactions
    WHERE category IS NULL
    ON CONFLICT (description) DO NOTHING
    """)
    conn.commit()
    conn.close()


def categorise_step_one():
    """
    Categorise transactions by matching against known merchants.

    Updates transaction categories where the description contains a known
    merchant name from the merchants table. Only updates uncategorised
    transactions (category IS NULL).
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            UPDATE finance.transactions t
            SET category = m.category
            FROM finance.merchants m
            WHERE t.description ILIKE '%' || m.merchant_name || '%'
            AND t.category IS NULL
            AND m.merchant_name IS NOT NULL
            AND m.category IS NOT NULL
        """)
        conn.commit()
        print(f"Categorised {cursor.rowcount} transactions from merchant lookup")
    except psycopg2.Error as e:
        print(f"Failed to categorise transactions: {e}")
    finally:
        conn.close()


def categorise_step_two():
    """
    Categorise remaining uncategorised transactions using LLM.

    For each uncategorised transaction:
    1. Calls LLM to extract merchant name and category from description
    2. Updates merchants table with new merchant/category if not already known
    3. Updates transaction category

    Note:
        Makes one LLM call per uncategorised transaction — use sparingly.
        New merchants are saved to merchants table to avoid repeat LLM calls.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("SELECT description FROM finance.transactions WHERE category IS NULL")
        rows = cursor.fetchall()
        print(f"Categorising {len(rows)} transactions with LLM...")

        for row in rows:
            try:
                categorised = extract_merchants_from_descriptions([row[0]])[0]
                cursor.execute(
                    """UPDATE finance.merchants
                       SET merchant_name = %s, category = %s
                       WHERE description = %s
                       AND merchant_name IS NULL
                       AND category IS NULL""",
                    (
                        categorised['merchant_name'],
                        categorised['category'],
                        row[0]
                    )
                )
                cursor.execute(
                    """UPDATE finance.transactions
                       SET category = %s
                       WHERE description = %s
                       AND category IS NULL""",
                    (
                        categorised['category'],
                        row[0]
                    )
                )
                conn.commit()
            except Exception as e:
                print(f"Failed to categorise: {row[0]}: {e}")
                conn.rollback()
                continue

        print("LLM categorisation complete")
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    finally:
        conn.close()


def categorise_transactions():
    categorise_transfers()
    categorise_bcard_payments()
    categorise_step_one()
    categorise_step_two()