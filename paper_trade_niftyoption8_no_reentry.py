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
from signal_emitter import emit_signal
import asyncio
from find_instrument import FindInstrument


# =========================
# CONFIG
# =========================
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

            

ATM = None 
TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

COMMON_ID = "adb83f48-262a-4ddf-968f-9e639474bf89"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "NIFTY_OPTION_BUYING_50_reentry"
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()


IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 16)
TRADE_END   = dtime(15, 20)

CE_TARGET_POINTS = 8
TARGET_POINTS = 8
PE_TARGET_POINTS = 8
LOTSIZE = 65

today = datetime.now(IST).strftime("%Y-%m-%d")
# =========================
# LOGIN
# =========================

combined_exit_active = False
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)
fno_df = load_fno_master()


strategy_id = "adb83f48-262a-4ddf-968f-9e639474bf89"
loop = asyncio.new_event_loop()

def start_loop():
    asyncio.set_event_loop(loop)
    loop.run_forever()

threading.Thread(target=start_loop, daemon=True).start()

def run_async(coro):
    try:
        if asyncio.iscoroutine(coro):
            asyncio.run_coroutine_threadsafe(coro, loop)
        else:
            print("❌ Not coroutine:", coro)
    except Exception as e:
        print("WS error: ", e)

def get_today_deployments():
    url = f"https://algoapi.dreamintraders.in/api/deployments/today/{strategy_id}"

    try:
        response = requests.get(url, timeout=10)

        # Raise error if status not 200
        response.raise_for_status()

        data = response.json()

        # 👉 store in variable (this is what you asked)
        user_deployments = data

        return user_deployments

    except requests.exceptions.RequestException as e:
        print("API Error:", e)
        return None

def group_users_by_broker(deployments):
    grouped = {}

    if not deployments:
        return grouped

    for d in deployments:

        if d["type"] == "paper":
            continue
        broker = d.get("broker_name")

        if not broker:
            continue

        if broker not in grouped:
            grouped[broker] = []

        grouped[broker].append(d)

    return grouped


def build_payload(name, side, token , reason, event_type, ltp, pnl, cum_pnl, lot,users):

    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"NIFTY{day}{month}{year}{ATM}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

    return {
        "strategy_id": COMMON_ID,
        "users": users,
        "option": name,
        "side": side,
        "quantity": lot * LOTSIZE,
        "security_id": token,
        "token": int(row["token"]),
        "event_type": event_type,
        "leg_name": name,
        "symbol": symbol,
        "exchange": "NFO",
        "expiry":expiry,
        "strike": ATM,
        "price":ltp,
        "pnl":pnl,
        "cum_pnl":cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason":reason
    }


# =========================
# HELPERS
# =========================

def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://algoapi.dreamintraders.in/api/tradelegs/create"
    
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



def get_first_candle_mark(security_id):

    today = datetime.now(IST).strftime("%Y-%m-%d")
   

    idx= dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=today,
        to_date=today
    )
    print("Today :",type(today),today)

    data = idx.get("data", {})
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)  

        if ts.hour == 9 and ts.minute == 15:
            mark = float(closes[i])
            print(f"📍 HIST MARK {security_id} @ {mark}")
            return mark

    print("❌ 09:15 candle not found")
    return None


def log_event(leg_name, token, action, price, remark=""):
    payload = {
        "run_id": COMMON_ID,
        "strategy_id": COMMON_ID,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": SYMBOL,
        "action": action,
        "price": price,
        "log_type": "TRADE_EVENT",
        "remark": remark
    }

    try:
        requests.post(EVENT_LOG_URL, json=payload, timeout=3)
    except Exception as e:
        print("EVENT LOG ERROR:", e)


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

def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)

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
                "https://algoapi.dreamintraders.in/api/telemetry",
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



def init_state():
    return {
        "marked": None,
        "position": False,
        "trading_disabled": False,
        "entry_price": None,
        "entry_time": None,
        "lot": 1,
        "pnl": 0.0,
        "symbol": None,
        "rearm_required": False,
        "moment":0.0
    }

# =========================
# START
# =========================

wait_for_start()

print("\n🚀 NIFTY OPTION BUYING 50 STARTED\n")

threading.Thread(target=trade_log_worker, daemon=True).start()

# =========================
# INDEX FIRST CANDLE
# =========================
idx = dhan.intraday_minute_data(
    security_id=13,
    exchange_segment="IDX_I",
    instrument_type="INDEX",
    from_date=today,
    to_date=today
)

data = idx.get("data", {})

opens = data.get("open", [])
highs = data.get("high", [])
lows = data.get("low", [])
closes = data.get("close", [])
volumes = data.get("volume", [])
timestamps = data.get("timestamp", [])

opening_candles = []

for i in range(len(timestamps)):
    ts = datetime.fromtimestamp(timestamps[i], IST) 

    if ts.hour == 9 and 15 <= ts.minute <= 17:
        candle = {
            "timestamp": timestamps[i],
            "open": opens[i],
            "high": highs[i],
            "low": lows[i],
            "close": closes[i],
            "volume": volumes[i]
        }
        opening_candles.append(candle)

print("Opening candles:", opening_candles)

if opening_candles:
    atm_price = float(opening_candles[0]["close"])  
    ATM = calculate_atm(atm_price)
    print("📌 ATM:", ATM)
   
else:
    print("Waiting for 9:17 candle...")


# =========================
# OPTION SELECTION
# =========================

today = datetime.now().date()
finder=FindInstrument()

ce_row = find_option_security(fno_df, ATM, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, ATM, "PE", today, "NIFTY")


AngelCE = finder.get_option("NIFTY" , int(ATM) , "CE")
AngelPE = finder.get_option("NIFTY" , int(ATM) , "PE")

print("angel tokens" , AngelCE , AngelPE)

CE_ID = str(ce_row["SECURITY_ID"])
PE_ID = str(pe_row["SECURITY_ID"])

print("📌 CE:", CE_ID)
print("📌 PE:", PE_ID)

builders = {
    CE_ID: OneMinuteCandleBuilder(),
    PE_ID: OneMinuteCandleBuilder()
}

# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"NIFTY CE {ATM}",
    ATM,
    str(today),
    CE_ID
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {ATM}",
    ATM,
    str(today),
    PE_ID
)



# =========================
# STATE
# =========================

ce_state = init_state()
pe_state = init_state()

combined_pnl=0

ce_state["marked"] = get_first_candle_mark(CE_ID)
pe_state["marked"] = get_first_candle_mark(PE_ID)


# =========================
# STRATEGY ENGINE
# =========================


def handle_leg(name, token, candle, state, ltp):
    global combined_pnl

    now = datetime.now(IST).time()
    close = candle["close"]

    avg = (candle["open"] + candle["high"] +
           candle["low"] + candle["close"]) / 4

    timestamp = candle["timestamp"]


    # =========================
    # TIME EXIT (15:20)
    # =========================
    if now >= TRADE_END:
        telemetry["status"] = "CLOSED"

        if state["position"]:

            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            run_async(emit_signal(build_payload(name, "SELL", token , "exit","EXIT", ltp, pnl, combined_pnl, state["lot"],users)))
            log_trade_event(
                
                event_type="EXIT",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TIME EXIT",
                pnl= state["pnl"],
                cum_pnl=combined_pnl
                )

            state["position"] = False


        state["trading_disabled"] = True
        return

    # =========================
    # STOP TRADING
    # =========================
    if state["trading_disabled"]:
        return

    # =============================
    # ENTRY SIGNAL AND EXECUTION
    # =============================
    if not state["position"]:

        if close > state["marked"] and avg > state["marked"] and avg < close:

            entry_price = ltp   

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()

            state["position"] = True

            print("🟢 BUY", name, entry_price)

            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)
            run_async(emit_signal(build_payload(name, "BUY", token , "entry","ENTRY", ltp, state["pnl"], combined_pnl, state["lot"],users)))


            log_trade_event(
                event_type="ENTRY",
                leg_name=name,
                token=token,
                symbol="NIFTY",
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="Trade opened",
                pnl= state["pnl"],
                cum_pnl=combined_pnl
                )

            log_event(f"{name} BUY", token, "ENTRY_EXECUTED", entry_price, "Trade opened")

    # =========================
    # EXIT CONDITION (STRUCTURE BREAK)
    # =========================
    if state["position"] and ltp < state["marked"]:

        exit_price = ltp

        pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

        state["pnl"] += pnl
        combined_pnl += pnl

        print("🔴 EXIT", name, exit_price)

        log_trade_event(
            
            event_type="EXIT",
            leg_name=name,
            token=token,
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=exit_price,
            reason="Below Mark",
            pnl= state["pnl"],
            cum_pnl=combined_pnl
                )

        state["position"] = False
        

        state["rearm_required"] = True


def universal_exit_check(ce_ltp, pe_ltp):

    global combined_pnl, combined_exit_active , CE_TARGET_POINTS , PE_TARGET_POINTS , TARGET_POINTS

    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (ce_ltp - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    total = ce_state["pnl"] + pe_state["pnl"] + ce_running + pe_running

    if ce_state["position"] or pe_state["position"]:
        telemetry["status"] = 'RUNNING'

    ce_total = ce_state["pnl"] + ce_running
    pe_total = pe_state["pnl"] + pe_running

    combined_total = ce_total + pe_total

    # =========================
    # ✅ COMBINED EXIT (TICK LEVEL SAFE)
    # =========================



    if total >= TARGET_POINTS*65 and not ce_state["trading_disabled"] and not pe_state["trading_disabled"] :

        print("🏁 TARGET HIT", total)
        

        # FORCE EXIT CE
        if ce_state["position"]:
            exit_price = ce_ltp
            pnl = (exit_price - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

            ce_state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            run_async(emit_signal(build_payload("CE", "SELL", CE_ID , "exit","EXIT", ce_ltp, ce_state["pnl"], combined_pnl, ce_state["lot"],users))) 

            log_trade_event(
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=ce_state["lot"],
                price=exit_price,
                reason="UNIVERSAL EXIT",
                pnl= ce_state["pnl"],
                cum_pnl=combined_pnl
                )   

            ce_state["position"] = False
            ce_state["rearm_required"] = True
            ce_state["lot"] = 1
        ce_state["trading_disabled"] = True



        # FORCE EXIT PE
        if pe_state["position"]:
            exit_price = pe_ltp
            pnl = (exit_price - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

            pe_state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)
            
            run_async(emit_signal(build_payload("PE", "SELL", PE_ID , "exit","EXIT", pe_ltp, pe_state["pnl"], combined_pnl, pe_state["lot"],users))) 
            log_trade_event(
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=pe_state["lot"],
                price=exit_price,
                reason="UNIVERSAL EXIT",
                pnl= pe_state["pnl"],
                cum_pnl=combined_pnl
                )

            pe_state["position"] = False
            pe_state["rearm_required"] = True
            pe_state["lot"] = 1
        pe_state["trading_disabled"] = True


# =========================
# CALLBACKS
# =========================


def on_message(msg):

    if msg.get("type") != "Quote Data":
        return
    
    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0))

    builder = builders.get(token)

    if not builder:
        return

    candle = builder.process_tick(msg)

    token = str(msg["security_id"])

    # store LTP
    if token == CE_ID:
        
        telemetry["ce_ltp"] = float(ltp or 0)

    if token == PE_ID:
        
        telemetry["pe_ltp"] = float(ltp or 0)  

    # =========================
    # RUN UNIVERSAL EXIT (TICK LEVEL)
    # =========================
    if "ce_ltp" in telemetry and "pe_ltp" in telemetry:
        universal_exit_check(telemetry["ce_ltp"], telemetry["pe_ltp"])

    # =========================
    # CANDLE LOGIC
    # =========================
    if candle:

        if token == CE_ID:
            print("8 reentry CE",token)
            print(candle)
            handle_leg("CE", token, candle, ce_state, ltp)

        if token == PE_ID:
            print("8 reentry PE",token)
            print(candle)
            handle_leg("PE", token, candle, pe_state, ltp)

    # =========================
    # TELEMETRY (REAL-TIME PnL)
    # =========================
    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (telemetry["ce_ltp"] - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (telemetry["pe_ltp"] - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    telemetry["ce_pnl"] = ce_state["pnl"] + ce_running
    telemetry["pe_pnl"] = pe_state["pnl"] + pe_running
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]


# =====================
# START WS 
# =====================


TOKENS = [CE_ID , PE_ID]

def on_tick(token, msg):

    if token not in TOKENS:
        return  

    on_message(msg)

for t in TOKENS:
    subscribe(t, on_tick)