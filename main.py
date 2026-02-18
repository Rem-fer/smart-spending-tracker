from auth import get_access_token
from db_operations import save_all_transactions_to_db
from db_operations import update_all_categories_batch, save_daily_balance_snapshot


if __name__ == "__main__":
    access_token = get_access_token()
    # save_all_transactions_to_db(access_token)
    # update_all_categories_batch()
    save_daily_balance_snapshot(access_token)



