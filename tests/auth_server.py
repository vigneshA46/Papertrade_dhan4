from flask import Flask, request
import hashlib
from TradeMaster.TradeSync import *

app = Flask(__name__)

# 🔐 CONFIG
USER_ID = "YOUR_USER_ID"
SECRET_KEY = "YOUR_SECRET_KEY"

# GLOBAL SESSION STORE
SESSION = {
    "auth_code": None,
    "user_id": None,
    "session": None
}

def generate_checksum(user_id, auth_code, secret_key):
    data = user_id + auth_code + secret_key
    return hashlib.sha256(data.encode()).hexdigest()

@app.route("/")
def home():
    return "Auth server running..."

@app.route("/callback")
def callback():
    auth_code = request.args.get("authCode")
    user_id = request.args.get("userId")

    if not auth_code:
        return "Auth failed"

    print("Received authCode:", auth_code)

    # Save
    SESSION["auth_code"] = auth_code
    SESSION["user_id"] = user_id

    # Create TradeHub instance
    trade = TradeHub(
        user_id=user_id,
        auth_code=auth_code,
        secret_key=SECRET_KEY
    )

    checksum = generate_checksum(user_id, auth_code, SECRET_KEY)

    session = trade.get_session_id(check_sum=checksum)

    SESSION["session"] = session

    print("Session:", session)

    return "Login successful! You can close this tab."

if __name__ == "__main__":
    app.run(port=5000)