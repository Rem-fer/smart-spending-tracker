import requests
import os
from dotenv import load_dotenv
import time
from auth import get_access_token

load_dotenv()
API_BASE_URL = os.getenv("TL_API_BASE_URL")

def call_api(url,access_token, retries=3):
    """Generic API caller for TrueLayer endpoints."""
    headers = {"Authorization": f"Bearer {access_token}"}
    for attempt in range(retries):
        try:
            response = requests.get(url, headers=headers, timeout=10)
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

def get_accounts(access_token, is_card=False):
    """Fetch all accounts for the authenticated user."""
    endpoint = "cards" if is_card else "accounts"
    return call_api(url=f"{API_BASE_URL}/data/v1/{endpoint}", access_token=access_token)


def get_balance(access_token, account_id, is_card=False):
    """Fetch current balance for a specific account."""
    endpoint = "cards" if is_card else "accounts"
    return call_api(f"{API_BASE_URL}/data/v1/{endpoint}/{account_id}/balance", access_token)

def get_transactions(access_token, account_id, is_card=False):
    """Fetch completed transactions for a specific account."""
    endpoint = "cards" if is_card else "accounts"
    return call_api(f"{API_BASE_URL}/data/v1/{endpoint}/{account_id}/transactions", access_token)

def get_pending_transactions(access_token, account_id, is_card=False):
    """Fetch pending transactions for a specific account."""
    endpoint = "cards" if is_card else "accounts"
    return call_api(f"{API_BASE_URL}/data/v1/{endpoint}/{account_id}/transactions/pending", access_token)

def get_direct_debits(access_token, account_id, is_card=False):
    """Fetch direct debits for a specific account."""
    endpoint = "cards" if is_card else "accounts"
    return call_api(f"{API_BASE_URL}/data/v1/{endpoint}/{account_id}/transactions/direct_debits", access_token)


