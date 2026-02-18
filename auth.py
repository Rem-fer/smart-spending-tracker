import requests
import os
from dotenv import load_dotenv
import json
import time

load_dotenv()

AUTH_CODE = "" # <-- Changes everytime (For initial token Auth)
TOKEN_URL = os.getenv("TL_AUTH_URL")


def get_initial_token(auth_code):
    """Exchange authorization code for initial access and refresh tokens."""
    try:
        response = requests.post(
            TOKEN_URL,
            data={
                "grant_type": "authorization_code",
                "redirect_uri": "https://console.truelayer.com/redirect-page",
                "code": auth_code,
            },
            auth=(os.getenv("TL_CLIENT_ID"), os.getenv("TL_CLIENT_SECRET")),
            headers={"Accept": "application/json"},
            timeout=30,
        )
        response.raise_for_status()
        data = response.json()

        tokens = {
            "access_token": data["access_token"],
            "refresh_token": data["refresh_token"],
            "expires_at": time.time() + data["expires_in"]
        }
        save_tokens(tokens)
        return tokens

    except requests.exceptions.RequestException as e:
        print(f"Failed to exchange auth code: {e}")
        return None

def load_tokens():
    """Load saved access and refresh tokens from JSON file."""
    try:
        with open("tokens.json", "r") as f:
            return json.load(f)
    except FileNotFoundError:
        print("No tokens found. Run auth first")
        return None
    except json.JSONDecodeError:
        print("Corrupted tokens file")
        return None

def save_tokens(tokens):
    """Save access and refresh tokens to JSON file."""
    try:
        with open("tokens.json", "w") as f:
            json.dump(tokens, f, indent=4)
    except IOError as e:
        print(f"Failed to save tokens: {e}")

def refresh_tokens(refresh_token, retries=3):
    """Exchange refresh token for new access and refresh tokens."""
    for attempt in range(retries):
        try:
            response = requests.post(
                TOKEN_URL,
                data={
                    "grant_type": "refresh_token",
                    "refresh_token": refresh_token,
                },
                auth=(os.getenv("TL_CLIENT_ID"), os.getenv("TL_CLIENT_SECRET")),
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

def get_access_token():
    """Get valid access token, refreshing if expired."""
    tokens = load_tokens()
    if not tokens:
        return None
    if time.time() > tokens.get("expires_at", 0):
        new_tokens = refresh_tokens(tokens.get("refresh_token"))
        if not new_tokens:
            return None
        save_tokens(new_tokens)
        return new_tokens.get("access_token")
    else:
        return tokens.get("access_token")