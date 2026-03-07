print("Starting...")
from dotenv import load_dotenv
from auth import get_access_token
from db_operations import save_all_transactions_to_db
from db_operations import update_all_categories_batch, save_daily_balance_snapshot
import os

load_dotenv()

# if __name__ == "__main__":
#     print(os.getenv("DB_HOST"))
#     access_token = get_access_token()
#     save_all_transactions_to_db(access_token)
#     update_all_categories_batch()
#     save_daily_balance_snapshot(access_token)
#


if __name__ == "__main__":
    print(os.getenv("DB_HOST"))
    print("Getting access token...")
    access_token = get_access_token()
    print("Got access token, fetching transactions...")
    save_all_transactions_to_db(access_token)
    print("Transactions saved, updating categories...")
    update_all_categories_batch()
    print("Categories updated, saving balance...")
    save_daily_balance_snapshot(access_token)
    print("Done!")