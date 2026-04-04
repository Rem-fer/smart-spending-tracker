import psycopg2
from api import get_accounts, get_transactions, get_balance
from db import get_connection
from auth import get_access_token, get_initial_token

import requests


def save_accounts(accounts, is_card=False):
    """
    Save accounts information to database.

    Args:
        accounts (dict): Account data from TrueLayer API.
                        Format: {"results": [account1, account2, ...]}
        is_card (bool): If True, uses card-specific field names (card_type instead of account_type).
                        Set to True when passing data from TrueLayer /cards endpoint.
                        Defaults to False.

    Returns:
        bool: True if successful, False otherwise.
    """
    if not accounts:
        print("Failed to get accounts")
        return False

    conn = get_connection()
    cursor = conn.cursor()

    try:
        for account in accounts.get("results", []):
            cursor.execute("""
                INSERT INTO finance.accounts 
                (account_id, account_type, display_name, currency, provider_id, provider_name)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (account_id) DO UPDATE SET
                    display_name = EXCLUDED.display_name,
                    account_type = EXCLUDED.account_type,
                    currency = EXCLUDED.currency,
                    provider_id = EXCLUDED.provider_id,
                    provider_name = EXCLUDED.provider_name
            """, (
                account["account_id"],
                account["card_type"] if is_card else account["account_type"], # account["card_type"] if credit card
                account["display_name"],
                account["currency"],
                account.get("provider_name", {}).get("provider_id"),
                account.get("provider_name", {}).get("display_name")
            ))
        conn.commit()
        print(f"Saved {len(accounts.get('results', []))} accounts successfully")
        return True
    except psycopg2.Error as e:
        print(f"Failed to save accounts: {e}")
        return False
    finally:
        conn.close()


def get_accounts_info(provider=None):
    """
    Retrieve account information from the database filtered by provider.

    Args:
        provider (str): Provider name to filter by e.g. 'BARCLAYS', 'REVOLUT'

    Returns:
        dict: Dictionary mapping account IDs to their info.
              Format: {account_id: {'provider_id': str, 'account_type': str, 'currency': str}}
              Returns empty dict if no accounts found or on database error.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        accounts_info = {}
        if provider:
            cursor.execute("""
                SELECT account_id, account_type, currency, provider_id, provider_name 
                FROM finance.accounts 
                WHERE provider_name = %s
            """, (provider,))
        else:
            cursor.execute("""
                SELECT account_id, account_type, currency, provider_id, provider_name 
                FROM finance.accounts
            """)
        for row in cursor.fetchall():
            accounts_info[row[0]] = {
                "provider_id": row[3],
                "provider": row[4],
                "account_type": row[1],
                "currency": row[2]
            }
        return accounts_info
    except psycopg2.Error as e:
        print(f"Failed to get accounts: {e}")
        return {}
    finally:
        conn.close()

def get_account_ids(accounts_info, account_type=None):
    """
    Filter account IDs by type.

    Args:
        accounts_info (dict): Account info from get_accounts_info()
        account_type (list[str] | None): List of account types to filter by e.g. ['TRANSACTION', 'SAVINGS'].
                                  If None, returns all account IDs.

    Returns:
        list: List of matching account IDs
    """
    if not accounts_info:
        return []
    result = []
    for account_id, info in accounts_info.items():
        if account_type is None or info["account_type"] in account_type:
            result.append(account_id)
    return result

def fetching_all_transactions(access_token, account_ids, is_card=False):
    """
    Fetch all transactions for a list of account IDs.

    Args:
        access_token (str): Valid TrueLayer access token for API authentication
        account_ids (list): List of account IDs to fetch transactions for

    Returns:
        dict: Dictionary mapping account IDs to their transaction lists.
              Format: {account_id: [transaction1, transaction2, ...]}
              Returns None if no account IDs provided or all fetches failed.
    """
    if not account_ids:
        print("No accounts found")
        return None

    all_transactions = {}

    for acc_id in account_ids:
        try:
            response = get_transactions(access_token, acc_id,is_card)
            if response and "results" in response:
                all_transactions[acc_id] = response["results"]
            else:
                print(f"No transactions for account {acc_id}")
        except (TypeError, KeyError) as e:
            print(f"Error fetching transactions for account {acc_id}: {e}")
            continue

    return all_transactions if all_transactions else None


def get_all_accounts_balance(access_token, account_ids, is_card=False):
    """
    Fetch current balance information for a list of account IDs.

    Args:
        access_token (str): Valid TrueLayer access token
        account_ids (list): List of account IDs to fetch balances for

    Returns:
        dict: Dictionary mapping account IDs to their balance info.
              Format: {account_id: {'currency': 'GBP', 'current': 22.0, 'available': 222.0, ...}}
              Returns None if no account IDs provided.
              Returns empty dict if all balance fetches failed.
    """
    if not account_ids:
        print("No accounts found")
        return None

    balances = {}

    for acc_id in account_ids:
        try:
            response = get_balance(access_token, acc_id, is_card)
            if response["results"]:
                balances[acc_id] = response["results"][0]
            else:
                print(f"No balance info found for id: {acc_id}")
        except (TypeError, KeyError) as e:
            print(f"Error fetching balance info for account {acc_id}: {e}")
            continue

    return balances


def get_current_balances(access_token, account_ids, is_card=False):
    """
    Fetch available balance for a list of account IDs.

    Args:
        access_token (str): Valid TrueLayer access token
        account_ids (list): List of account IDs to fetch balances for

    Returns:
        dict: Dictionary mapping account IDs to available balance amounts.
              Format: {account_id: 222.0, ...}
              Returns None if no account IDs provided.
              Returns empty dict if all balance fetches failed.
    """
    # account_ids = get_account_ids()
    if not account_ids:
        print("No accounts found")
        return None

    current_balances = {}
    for acc_id in account_ids:
        try:
            response = get_balance(access_token, acc_id, is_card)
            if response["results"]:
                current_balances[acc_id] = response["results"][0]["available"]
            else:
                print(f"No balance info found for id: {acc_id}")
        except (TypeError, KeyError) as e:
            print(f"Error fetching current balance info for account {acc_id}: {e}")
            continue

    return current_balances



AUTH_CODE = ""   # Changes everytime — get from TrueLayer console
PROVIDER = ""    # e.g. "BARCLAYCARD", "BARCLAYS", "REVOLUT"
IS_CARD = False  # Set to True for credit cards accounts (Barclaycard, Amex)

if __name__ == "__main__":
    get_initial_token(AUTH_CODE, PROVIDER)
    access_token = get_access_token(PROVIDER)
    accounts = get_accounts(access_token, is_card=IS_CARD)
    save_accounts(accounts, is_card=IS_CARD)