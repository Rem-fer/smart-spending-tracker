import requests
import os
from dotenv import load_dotenv
import time
import psycopg2
from db import get_connection

load_dotenv()


def get_initial_token(auth_code, provider):
    """Exchange authorization code for initial access and refresh tokens."""
    try:
        response = requests.post(
            "https://auth.truelayer.com/connect/token",
            data={
                "grant_type": "authorization_code",
                "redirect_uri": "https://console.truelayer.com/redirect-page",
                "code": auth_code,
            },
            auth=(os.getenv("TL_CLIENT_ID_LIVE"), os.getenv("TL_SECRET_LIVE")),
            headers={"Accept": "application/json"},
            timeout=30,
        )
        # print(response.text)
        response.raise_for_status()
        data = response.json()

        tokens = {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_at": time.time() + data["expires_in"]
        }
        save_tokens(tokens, provider)
        print(f"Tokens saved for {provider}")

    except requests.exceptions.RequestException as e:
        print(f"Failed to exchange auth code: {e}")
        return None

def save_tokens(tokens, provider):
    """Save access and refresh tokens to database."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            INSERT INTO finance.tokens (provider, access_token, refresh_token, expires_at)
            VALUES (%s, %s, %s, %s)
            ON CONFLICT (provider) DO UPDATE SET
                access_token = EXCLUDED.access_token,
                refresh_token = EXCLUDED.refresh_token,
                expires_at = EXCLUDED.expires_at,
                updated_at = CURRENT_TIMESTAMP
        """, (
            provider,
            tokens["access_token"],
            tokens["refresh_token"],
            tokens["expires_at"]
        ))
        conn.commit()
    except psycopg2.Error as e:
        print(f"Failed to save tokens: {e}")
    finally:
        conn.close()


def load_tokens(provider):
    """Load saved access and refresh tokens from database."""
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute("""
            SELECT access_token, refresh_token, expires_at 
            FROM finance.tokens 
            WHERE provider = %s
        """, (provider,))
        row = cursor.fetchone()
        if row:
            return {
                "access_token": row[0],
                "refresh_token": row[1],
                "expires_at": row[2]
            }
        else:
            print(f"No tokens found for {provider}. Run auth first")
            return None
    except psycopg2.Error as e:
        print(f"Failed to load tokens: {e}")
        return None
    finally:
        conn.close()


def refresh_tokens(refresh_token, retries=3):
    """Exchange refresh token for new access and refresh tokens."""
    for attempt in range(retries):
        try:
            response = requests.post(
                "https://auth.truelayer.com/connect/token",
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                auth=(os.getenv("TL_CLIENT_ID_LIVE"), os.getenv("TL_SECRET_LIVE")),
                headers={"Accept": "application/json"},
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            return {
                "access_token": data["access_token"],
                "refresh_token": data["refresh_token"],
                "expires_at": time.time() + data['expires_in']
            }
        except requests.exceptions.RequestException as e:
            if attempt < retries - 1:
                print(f"Refresh failed, retrying... ({attempt + 1}/{retries})")
                time.sleep(2)
            else:
                print(f"Failed to refresh token: {e}")
                return None

def get_access_token(provider):
    """Get valid access token, refreshing if expired."""
    tokens = load_tokens(provider)
    if not tokens:
        return None
    if time.time() > tokens.get("expires_at", 0):
        new_tokens = refresh_tokens(tokens.get("refresh_token"))
        if not new_tokens:
            return None
        save_tokens(new_tokens,  provider)
        return new_tokens.get("access_token")
    else:
        return tokens.get("access_token")

AUTH_CODE = "" # <-- Changes everytime (For initial token Auth)
PROVIDER = "" # <-- How you want to name this provider in your db

if __name__ == "__main__":
    get_initial_token(AUTH_CODE, PROVIDER)