import time
import threading
from queue import Queue
import requests
import random

# =========================
# CONFIG
# =========================
TRADE_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/event"
COMMON_ID = "1fff432a-0411-40ff-aefd-c0b0026d5a6d"
SYMBOL = "NIFTY"
LOTSIZE = 65


# =========================
# QUEUE SETUP
# =========================
trade_log_queue = Queue()

# =========================
# WORKER
# =========================
def trade_log_worker():
    while True:
        payload = trade_log_queue.get()

        try:
            print("🚀 Sending:", payload["event_type"], payload["leg_name"], payload["price"])

            requests.post(TRADE_LOG_URL, json=payload, timeout=2)

        except Exception as e:
            print("❌ ERROR:", e)

        finally:
            trade_log_queue.task_done()


# =========================
# START WORKER
# =========================
threading.Thread(target=trade_log_worker, daemon=True).start()

# =========================
# LOG FUNCTION
# =========================
def log_trade_event(
    event_type,
    leg_name,
    token,
    symbol,
    side,
    lot,
    price,
    reason
):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "trade_id": COMMON_ID,

        "event_type": event_type,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": float(price),

        "reason": reason,
        "deployed_by": COMMON_ID
    }

    trade_log_queue.put(payload)


# =========================
# TEST FUNCTION
# =========================
def simulate_trades():

    for i in range(10):

        # ENTRY
        log_trade_event(
            event_type="ENTRY",
            leg_name="CE",
            token=62582,
            symbol="NIFTY",
            side="BUY",
            lot=1,
            price=round(random.uniform(200, 300), 2),
            reason="Test Entry"
        )

        time.sleep(0.2)

        # EXIT
        log_trade_event(
            event_type="EXIT",
            leg_name="CE",
            token=62582,
            symbol="NIFTY",
            side="SELL",
            lot=1,
            price=round(random.uniform(200, 300), 2),
            reason="Test Exit"
        )

        time.sleep(0.2)


# =========================
# RUN TEST
# =========================
if __name__ == "__main__":
    print("🔥 Starting Trade Logger Test...\n")

    simulate_trades()

    # wait for queue to finish
    trade_log_queue.join()

    print("\n✅ All logs processed successfully!")