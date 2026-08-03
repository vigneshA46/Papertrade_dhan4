# sample_vwap_ws.py

from vwap_engine import VWAPManager, MinuteVWAPSampler
import threading
from dhanhq import marketfeed

    # 🔑 Replace with your credentials
CLIENT_ID = "1107425275"
ACCESS_TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzUxMiJ9.eyJpc3MiOiJkaGFuIiwicGFydG5lcklkIjoiIiwiZXhwIjoxNzc1MTg0NDQ4LCJpYXQiOjE3NzUwOTgwNDgsInRva2VuQ29uc3VtZXJUeXBlIjoiQVBQIiwiZGhhbkNsaWVudElkIjoiMTEwNzQyNTI3NSJ9.DSmIRZa6I1KvyBH_t0pa9_gllj6Vo6UtnHef0Gw-0glPgx37Pz0tTksXt8maty--bL2Do83D1Qu7bT_e53A0gg"

# 🔥 Your instruments
CE_ID = 40761
PE_ID = 40769

# Init VWAP manager
vwap_manager = VWAPManager()

# Optional: store latest prices
price_store = {}

sampler = MinuteVWAPSampler()


# -------------------------------
# 🧠 Tick Handler
# -------------------------------


def on_tick(msg):
    if msg["type"] == 'Quote Data':

        security_id = int(msg["security_id"])

        if security_id not in [CE_ID, PE_ID]:
            return

        # Update VWAP (tick-by-tick internally)
        _, vwap = vwap_manager.on_tick(msg)

        if vwap is None:
            return

        # 🔥 Emit only once per minute
        price = float(msg["LTP"])

        if security_id == CE_ID:
            print(f"🟢 CE Tick VWAP: {vwap:.2f} | Price: {price}")

        elif security_id == PE_ID:
            print(f"🔴 PE Tick VWAP: {vwap:.2f} | Price: {price}")


# -------------------------------
# 🔌 WebSocket Connection
# -------------------------------
def start_ws():
    instruments = [
    (marketfeed.NSE_FNO, str(CE_ID), marketfeed.Quote),
    (marketfeed.NSE_FNO, str(PE_ID), marketfeed.Quote)
    ]


    feed = marketfeed.DhanFeed(CLIENT_ID, ACCESS_TOKEN, instruments, "v2")
 
    while True:
        try:
            feed.run_forever()
            data = feed.get_data()

            if data:
                on_tick(data)

        except Exception as e:
            print("WS ERROR:", e)
            feed.run_forever()
# -------------------------------
# 🚀 Run
# -------------------------------
if __name__ == "__main__":
    start_ws()