import requests



COMMON_ID = "1fff432a-0411-40ff-aefd-c0b0026d5a6d"
SYMBOL = "NIFTY"
LOTSIZE = 65
TRADE_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/event"

def log_trade_event(
    
    event_type,   # ENTRY / EXIT
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

        "trade_id": COMMON_ID,         # 🔥 VERY IMPORTANT
        "event_type": event_type,     # ENTRY / EXIT

        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": price,

        "reason": reason,
        "deployed_by": COMMON_ID
    }

    try:
        requests.post(TRADE_LOG_URL, json=payload, timeout=3)
    except Exception as e:
        print("TRADE EVENT LOG ERROR:", e)




log_trade_event(
            
            event_type="EXIT",
            leg_name="CE",
            token=12345,
            symbol=SYMBOL,
            side="SELL",
            lot=1,
            price=243.46,
            reason="Below Mark"
                )
