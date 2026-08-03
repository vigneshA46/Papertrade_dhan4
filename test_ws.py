from dhanhq import MarketFeed, dhanhq, DhanContext
import time
import os
from datetime import datetime
from dotenv import load_dotenv
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder

load_dotenv()

access_token = get_access_token()
CLIENT_ID = os.getenv("CLIENT_ID")

ALL_TOKENS = ["72312", "72317", "72323", "72279"]

active_tokens = [ALL_TOKENS[0]]
current_index = 0

candle_builders = {}

def create_feed():
    instruments = [
        (MarketFeed.NSE_FNO, token, MarketFeed.Quote)
        for token in active_tokens
    ]
    instruments.append((MarketFeed.IDX, "13", MarketFeed.Quote))

    dhan_context = DhanContext(CLIENT_ID, access_token)
    return MarketFeed(dhan_context, instruments, "v2")


def on_message(msg):
    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])

    if token not in candle_builders:
        candle_builders[token] = OneMinuteCandleBuilder()

    candle = candle_builders[token].process_tick({
        "type": "Quote Data",
        "LTP": float(msg["LTP"]),
        "volume": msg.get("volume", 0),
        "LTT": msg["LTT"]
    })

    if candle:
        print(f"CANDLE CLOSED → {token} → {candle}")


feed = create_feed()
feed.on_message = on_message

last_switch_time = time.time()
SWITCH_INTERVAL = 120


# ✅ Only ONE loop
while True:
    try:
        feed.run_forever()   # ⚠️ blocks internally

        data = feed.get_data()
        if data:
            on_message(data)

        # token switching logic
        if time.time() - last_switch_time >= SWITCH_INTERVAL:
            if current_index + 1 < len(ALL_TOKENS):
                current_index += 1
                new_token = ALL_TOKENS[current_index]
                active_tokens.append(new_token)

                print("Adding Token:", new_token)

                # ❗ recreate safely
                feed.close()   # VERY IMPORTANT
                feed = create_feed()
                feed.on_message = on_message

            last_switch_time = time.time()

    except Exception as e:
        print("WS ERROR:", e)
        time.sleep(2)