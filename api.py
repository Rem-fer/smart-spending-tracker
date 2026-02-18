import requests
import os
from dotenv import load_dotenv
import time


load_dotenv()
API_BASE_URL = os.getenv("TL_API_BASE_URL")

def call_api(url,access_token, retries=3):
    """Generic API caller for TrueLayer endpoints."""
    headers = {"Authorization": f"Bearer {access_token}"}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.ConnectionError as e:
            if attempt < retries - 1:
                print(f"Connection failed, retrying... ({attempt + 1}/{retries})")
                time.sleep(2)
            else:
                print(f"API call failed after {retries} attempts: {e}")
                return None
        except requests.exceptions.RequestException as e:
            print(f"API call failed: {e}")
            return None

def get_accounts(access_token):
    """Fetch all accounts for the authenticated user."""
    return call_api(url=f"{API_BASE_URL}/data/v1/accounts", access_token=access_token)

def get_balance(access_token, account_id):
    """Fetch current balance for a specific account."""
    return call_api(f"{API_BASE_URL}/data/v1/accounts/{account_id}/balance", access_token)

def get_transactions(access_token, account_id):
    """Fetch completed transactions for a specific account."""
    return call_api(f"{API_BASE_URL}/data/v1/accounts/{account_id}/transactions", access_token)

def get_pending_transactions(access_token, account_id):
    """Fetch pending transactions for a specific account."""
    return call_api(f"{API_BASE_URL}/data/v1/accounts/{account_id}/transactions/pending", access_token)

def get_direct_debits(access_token, account_id):
    """Fetch direct debits for a specific account."""
    return call_api(f"{API_BASE_URL}/data/v1/accounts/{account_id}/transactions/direct_debits", access_token)
#
# def get_account_balance(access_token, account_id):
#     """Fetch balance for a specific account."""
#     return call_api(f"{API_BASE_URL}/data/v1/accounts/{account_id}/transactions/balance", access_token)
#
#
