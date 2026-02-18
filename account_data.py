import json
from api import get_accounts, get_transactions, get_balance


def save_accounts(access_token):
    """Saving accounts information from account API call into a JSON"""
    accounts = get_accounts(access_token)

    if not accounts:
        print("Failed to get accounts")
        return False

    try:
        with open("accounts.json", "w") as file:
            json.dump(accounts, file, indent=4)
        print("Accounts saved successfully")
        return True
    except IOError as e:
        print(f"Failed to save accounts: {e}")
        return False

def get_account_ids():
    """Get list of account IDs from saved accounts file."""
    try:
        with open("accounts.json", "r") as file:
            data = json.load(file)
            return [acc["account_id"] for acc in data["results"]]
    except FileNotFoundError:
        print("No account file found")
        return []
    except KeyError:
        print("Invalid account data")
        return []

def get_account_info(field=None):
    """
    Load account info from saved JSON.

    Args:
        field: Specific field to extract ('id', 'name', 'type', None for all)

    Returns:
        List of requested info or full accounts data
    """
    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
    except FileNotFoundError:
        return None

    results = accounts.get("results", [])

    if field == "id":
        return [acc["account_id"] for acc in results]
    elif field == "name":
        return [acc["display_name"] for acc in results]
    elif field == "type":
        return [acc["account_type"] for acc in results]
    else:
        return results  # Return full account objects


def get_account_id_by_name(name):
    """Get account ID by display name."""
    try:
        with open("accounts.json", "r") as f:
            accounts = json.load(f)
    except FileNotFoundError:
        print("No accounts file found")
        return None

    for acc in accounts.get("results", []):
        if acc["display_name"] == name:
            return acc["account_id"]

    print(f"Account '{name}' not found")
    return None

def fetching_all_transactions(access_token):
    """
    Fetch all transactions for all accounts.

    Args:
        access_token (str): Valid TrueLayer access token for API authentication

    Returns:
        dict: Dictionary mapping account IDs to their transaction lists.
              Format: {account_id: [transaction1, transaction2, ...]}
              Returns None if no accounts found or all fetches failed.
    """
    account_ids = get_account_ids()

    if not account_ids:
        print("No accounts found")
        return None

    all_transactions = {}

    for acc_id in account_ids:
        try:
            response = get_transactions(access_token, acc_id)
            if response and "results" in response:
                all_transactions[acc_id] = response["results"]
            else:
                print(f"No transactions for account {acc_id}")
        except (TypeError, KeyError) as e:
            print(f"Error fetching transactions for account {acc_id}: {e}")
            continue

    return all_transactions if all_transactions else None


def get_all_accounts_balance(access_token):
    """
    Fetch current balance information for all accounts.

    Args:
        access_token (str): Valid TrueLayer access token

    Returns:
        dict: Dictionary mapping account IDs to their balance info.
              Format: {account_id: {'currency': 'GBP', 'current': 22.0, 'available': 222.0, ...}}
              Returns None if no accounts found.
              Returns empty dict if all balance fetches failed.
    """
    account_ids = get_account_ids()
    if not account_ids:
        print("No accounts found")
        return None

    balances = {}

    for acc_id in account_ids:
        try:
            response = get_balance(access_token, acc_id)
            if response["results"]:
                balances[acc_id] = response["results"][0]
            else:
                print(f"No balance info found for id: {acc_id}")
        except (TypeError, KeyError) as e:
            print(f"Error fetching balance info for account {acc_id}: {e}")
            continue

    return balances


def get_current_balances(access_token):
    """
    Fetch available balance for all accounts.

    Args:
        access_token (str): Valid TrueLayer access token

    Returns:
        dict: Dictionary mapping account IDs to available balance amounts.
              Format: {account_id: 222.0, ...}
              Returns None if no accounts found.
              Returns empty dict if all balance fetches failed.
    """
    account_ids = get_account_ids()
    if not account_ids:
        print("No accounts found")
        return None

    current_balances = {}
    for acc_id in account_ids:
        try:
            response = get_balance(access_token, acc_id)
            if response["results"]:
                current_balances[acc_id] = response["results"][0]["available"]
            else:
                print(f"No balance info found for id: {acc_id}")
        except (TypeError, KeyError) as e:
            print(f"Error fetching current balance info for account {acc_id}: {e}")
            continue

    return current_balances