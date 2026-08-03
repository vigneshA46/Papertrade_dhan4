import time
from dhanhq import marketfeed
from dhanhq import dhanhq
from candle_builder import OneMinuteCandleBuilder

# =========================
# CONFIG
# =========================
CLIENT_ID = "YOUR_CLIENT_ID"
ACCESS_TOKEN = "YOUR_ACCESS_TOKEN"

CE_ID = "63430"
PE_ID = "63431"

# =========================
# GLOBAL STATE
# =========================
instruments = [
    (marketfeed.NSE_FNO, CE_ID, marketfeed.Quote),
    (marketfeed.NSE_FNO, PE_ID, marketfeed.Quote)
]
builders = {}
feed = None

# =========================
# CANDLE BUILDER
# =========================


class CandleBuilder:
    def __init__(self, timeframe=60):
        self.timeframe = timeframe
        self.current_candle = None
        self.start_time = None

    def process_tick(self, msg):
        ltp = float(msg.get("LTP", 0))
        ts = int(time.time())

        if not self.current_candle:
            self.start_time = ts
            self.current_candle = {
                "open": ltp,
                "high": ltp,
                "low": ltp,
                "close": ltp
            }
            return None

        self.current_candle["high"] = max(self.current_candle["high"], ltp)
        self.current_candle["low"] = min(self.current_candle["low"], ltp)
        self.current_candle["close"] = ltp

        if ts - self.start_time >= self.timeframe:
            closed = self.current_candle
            print(f"[CANDLE CLOSED] {closed}")

            self.current_candle = None
            self.start_time = None

            return closed

        return None


# =========================
# ADD TOKEN
# =========================
def add_token(token):
    print(f"Adding token: {token}")

    instruments.append((marketfeed.NSE_FNO, token, marketfeed.Quote))
    builders[token] = CandleBuilder()


# =========================
# RUN FEED (BLOCKING)
# =========================

def run_feed(run_seconds=None):
    global feed

    print("Starting WS with:", instruments)

    feed = marketfeed.DhanFeed(
        CLIENT_ID,
        ACCESS_TOKEN,
        instruments,
        "v2"
    )

    # start websocket ONCE
    feed.run_forever()

    start_time = time.time()

    while True:
        try:
            msg = feed.get_data()

            if msg:
                on_message(msg)

            # restart condition
            if run_seconds and (time.time() - start_time > run_seconds):
                print("Stopping for restart...")
                break

        except Exception as e:
            print("READ ERROR:", e)
            time.sleep(0.5)

    try:
        feed.close()
    except:
        pass

# =========================
# MESSAGE HANDLER
# =========================
def on_message(msg):
    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])

    builder = builders.get(token)
    if not builder:
        return

    candle = builder.process_tick(msg)

    if candle:
        print(f"[{token}] Candle:", candle)


# =========================
# MAIN FLOW
# =========================
def main():

    # Step 1: Add CE
    add_token(CE_ID)

    # Run for 30 sec
    run_feed(run_seconds=30)

    # Step 2: Add PE
    add_token(PE_ID)

    # Restart feed with both tokens
    run_feed()


if __name__ == "__main__":
    main()