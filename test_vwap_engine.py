import time
import pytz
import requests
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from dispatcher import subscribe
from queue import Queue
import pandas as pd
from vwap_engine import VWAPManager, MinuteVWAPSampler



vwap_manager = VWAPManager()



# =========================
# CONFIG
# =========================

COMMON_ID = '136b0559-76dc-435c-bb08-3e6a584a46d0'
trade_log_queue = Queue()
def trade_log_worker():
    while True:
        payload = trade_log_queue.get()
        try:
            requests.post(TRADE_LOG_URL, json=payload, timeout=2)
        except Exception as e:
            print("TRADE EVENT LOG ERROR:", e)
        finally:
            trade_log_queue.task_done()


def log_trade_event(
    event_type,   # ENTRY / EXIT
    leg_name,
    token,
    symbol,
    side,
    lot,
    price,
    reason,
    pnl,
    cum_pnl
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
        "deployed_by": COMMON_ID,

        "pnl": str(pnl),
        "cum_pnl":str(cum_pnl)
    }
   
    trade_log_queue.put(payload)


ATM = None 
TRADE_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/event"
EVENT_LOG_URL = "https://dreaminalgo-backend-production.up.railway.app/api/paperlogger/paperlogger"

COMMON_ID = "136b0559-76dc-435c-bb08-3e6a584a46d0"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "VWAP_NIFTY_OPTION_BUYING"
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()

INSTRUMENT_URL = "https://api.dhan.co/v2/instrument/NSE_FNO"
HIST_URL = "https://api.dhan.co/v2/charts/intraday"

HEADERS = {
    "Content-Type": "application/json",
    "access-token": access_token
}


IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 16)
TRADE_END   = dtime(15, 20)

TARGET_POINTS = 35
LOTSIZE = 65


CE_ID = None
PE_ID = None
combined_pnl = 0.0
today = datetime.now(IST).strftime("%Y-%m-%d")
# =========================
# LOGIN
# =========================

combined_exit_active = False
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)
fno_df = load_fno_master()




telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "ACTIVE",
    "pnl": 0.0,
    "pnl_percentage": 0.0,
    "ce_ltp": 0.0,
    "pe_ltp": 0.0,
    "ce_pnl": 0.0,
    "pe_pnl": 0.0
}


def telemetry_broadcaster():
    while True:
        try:
            # 🔥 COPY to avoid mutation issues
            payload = telemetry.copy()

            # 🔥 optional: sanitize (prevents TypeError)
            def safe_number(x):
                try:
                    return float(x)
                except:
                    return 0

            payload = {k: safe_number(v) if k in ["pnl","ce_pnl","pe_pnl","ce_ltp","pe_ltp","pnl_percentage"] else v
                for k, v in payload.items()}


            res = requests.post(
                "https://dreaminalgo-backend-production.up.railway.app/api/telemetry",
                json=payload,
                timeout=0.5   # 🔥 keep it LOW
            )

            # optional debug
            if res.status_code != 200:
                print("Telemetry failed:", res.status_code)

        except Exception as e:
            print("Telemetry error:", e)

        time.sleep(1)


t = threading.Thread(target=telemetry_broadcaster, daemon=True)
t.start()



# =========================
# HELPERS
# =========================

def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://dreaminalgo-backend-production.up.railway.app/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token":str(token)
    }

    try:
        response = requests.post(url, json=payload)

        if response.status_code == 200 or response.status_code == 201:
            print("✅ Trade leg logged successfully")
            return response.json()
        else:
            print(f"❌ Failed to log trade leg: {response.status_code}")
            print(response.text)
            return None

    except Exception as e:
        print(f"⚠️ Error while calling API: {e}")
        return None


# =========================
# STEP 2: GET NEAREST FUT
# =========================

def get_nearest_nifty_fut(df, trade_date):
    futs = df[
        (df["INSTRUMENT"] == "FUTIDX") &
        (df["UNDERLYING_SYMBOL"] == SYMBOL)
    ].copy()


    futs["SM_EXPIRY_DATE"] = pd.to_datetime(futs["SM_EXPIRY_DATE"])
    futs = futs[futs["SM_EXPIRY_DATE"] >= today]

    fut = futs.sort_values("SM_EXPIRY_DATE").iloc[0]
    return fut



def init_state():
    return {
        "position": False,
        "trading_disabled": False,
        "entry_price": None,
        "entry_time": None,
        "lot": 1,

        "pnl": 0.0,
        "last_price": None,   # 🔥 IMPORTANT

        "symbol": None,
        "entry_signal": False
    }


def update_pnl_tickwise(state, ltp):

    if not state["position"]:
        state["last_price"] = ltp
        return

    if state["last_price"] is None:
        state["last_price"] = ltp
        return

    diff = ltp - state["last_price"]
    state["pnl"] += diff * LOTSIZE

    state["last_price"] = ltp

# =========================
# STEP 4: ATM & ITM LOGIC
# =========================

def calculate_strikes(fut_price, step=50):
    atm = round(fut_price / step) * step
    return atm


def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)

# =========================
# STEP 3: HISTORICAL FETCH
# =========================

def fetch_intraday(security_id, exchange, instrument, from_dt, to_dt, oi=True):
    payload = {
        "securityId": str(security_id),
        "exchangeSegment": exchange,
        "instrument": instrument,
        "interval": 1,
        "oi": oi,
        "fromDate": from_dt,
        "toDate": to_dt
    }

    r = requests.post(HIST_URL, headers=HEADERS, json=payload)
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"],
        "volume": data["volume"],
        "oi": data.get("open_interest", [None] * len(data["timestamp"]))
    })

        # ✅ Correct datetime handling
    dt_index = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt_index.dt.tz_convert("Asia/Kolkata")

    return df

from datetime import time as dtime


def on_candle(state, candle, current_time, vwap):
    close = candle["close"]

    if state["trading_disabled"]:
        return None

    if current_time < dtime(9, 16):
        return None

    # =========================
    # 🔴 EXIT FIRST
    # =========================
    if state["position"] and close < vwap:
        state["position"] = False
        state["entry_price"] = None
        state["entry_time"] = None
        state["entry_signal"] = False
        return "EXIT"

    # =========================
    # 🔵 EXECUTE ENTRY
    # =========================
    if state["entry_signal"] and not state["position"]:
        state["position"] = True
        state["entry_price"] = close
        state["entry_time"] = current_time
        state["entry_signal"] = False
        return "BUY"

    # =========================
    # 🟡 GENERATE SIGNAL
    # =========================
    if not state["position"]:
        if close > vwap:
            state["entry_signal"] = True
        else:
            state["entry_signal"] = False

    return None

def check_mtm_and_kill_switch():
    global combined_exit_active , combined_pnl

    if combined_exit_active:
        return

    total_pnl = ce_state["pnl"] + pe_state["pnl"]
    combined_pnl = total_pnl

    telemetry["ce_pnl"] = ce_state["pnl"]
    telemetry["pe_pnl"] = pe_state["pnl"]
    telemetry["pnl"] = combined_pnl

    print(f"💰 MTM => CE: {ce_state['pnl']:.2f} | PE: {pe_state['pnl']:.2f} | TOTAL: {total_pnl:.2f}")

    if total_pnl >= 3000 or total_pnl <= -3000:

        print("🚨 MTM LIMIT HIT — FORCE EXIT ALL")

        combined_exit_active = True

        # CE FORCE EXIT
        if ce_state["position"]:
            print(f"🔴 CE FORCE EXIT | TOKEN: {CE_ID} | LTP: {telemetry.get('ce_ltp')} | TOTAL PNL: {ce_state['pnl']:.2f}")

            log_trade_event(
                
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=1,
                price=ltp,
                reason="FORCE EXIT MTM",
                pnl= state["pnl"],
                cum_pnl=combined_pnl
                )

            ce_state["position"] = False
            ce_state["entry_price"] = None
            ce_state["last_price"] = None

        # PE FORCE EXIT
        if pe_state["position"]:
            print(f"🔴 PE FORCE EXIT | TOKEN: {PE_ID} | LTP: {telemetry.get('pe_ltp')} | TOTAL PNL: {pe_state['pnl']:.2f}")

            log_trade_event(
                
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=1,
                price=ltp,
                reason="FORCE EXIT MTM",
                pnl= state["pnl"],
                cum_pnl=combined_pnl
                )

            pe_state["position"] = False
            pe_state["entry_price"] = None
            pe_state["last_price"] = None

        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True


def on_message(msg):

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0))

    # =========================
    # VWAP UPDATE
    # =========================
    _, vwap = vwap_manager.on_tick(msg)



    # =========================
    # CANDLE BUILD
    # =========================
    builder = builders.get(token)

    if not builder:
        print("no builder found")
        return

    candle = builder.process_tick(msg)

    # =========================
    # ON CANDLE CLOSE
    # =========================
    if candle and token=="74157":

        print("CANDLE", candle)
        print("TOKEN", token)
        print("VWAP", vwap)




#======================
#==main================
#======================



wait_for_start()

fut=get_nearest_nifty_fut(fno_df , today)

from_dt = f"{today} 09:15:00"
to_dt = f"{today} 09:17:00"

fut_df = fetch_intraday(
        fut["SECURITY_ID"],
        "NSE_FNO",
        "FUTIDX",
        from_dt,
        to_dt
    )

ref_price = fut_df.iloc[0]["close"]
print("FUT price",ref_price)

atm = calculate_strikes(ref_price)
print("ATM",atm)
ce_strike = atm - 200
pe_strike = atm + 200
print(ce_strike ,"CE strike")
print(pe_strike , "PE strike")

ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")


CE_ID = str(ce_row["SECURITY_ID"])
PE_ID = str(pe_row["SECURITY_ID"])

builders = {
    CE_ID: OneMinuteCandleBuilder(),
    PE_ID: OneMinuteCandleBuilder()
}

# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"NIFTY CE {ce_strike}",
    ce_strike,
    str(today),
    CE_ID
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {pe_strike}",
    pe_strike,
    str(today),
    PE_ID
)



print(ce_row["SECURITY_ID"], "CE ID")
print(pe_row["SECURITY_ID"], "PE ID")


instruments = [
    (MarketFeed.NSE_FNO, str(ce_row["SECURITY_ID"]), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(pe_row["SECURITY_ID"]), MarketFeed.Quote)
]

print("instruments", instruments)
feed = MarketFeed(dhan_context, instruments, "v2")
 
while True:
    try:
        feed.run_forever()
        data = feed.get_data()

        if data:
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()

