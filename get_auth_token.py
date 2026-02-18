import requests, os
from dotenv import load_dotenv
import json
import time

load_dotenv()

#Sandbox URL
TOKEN_URL = os.getenv("TL_AUTH_URL")  # <-- sandbox
AUTH_CODE = "" # <-- Changes everytime

def get_initial_token(auth_code):
    code = auth_code
    response = requests.post(
        TOKEN_URL,
        data={
            "grant_type": "authorization_code",
            "redirect_uri": "https://console.truelayer.com/redirect-page",
            "code": code,
        },
        auth=(os.getenv("TL_CLIENT_ID"), os.getenv("TL_CLIENT_SECRET")),  # Basic auth
        headers={"Accept": "application/json", "User-Agent": "Mozilla/5.0"},
        timeout=30,
    )
    response.raise_for_status()
    data = response.json()
    tokens = {
        "access_token": data["access_token"],
        "refresh_token": data["refresh_token"],
        "expires_at": time.time() + data['expires_in']
    }

    save_tokens(tokens)
    return tokens




