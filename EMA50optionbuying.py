import time
import pytz
import requests
from datetime import datetime, time as dtime
from datetime import timedelta
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from signal_emitter import emit_signal
from dispatcher import subscribe
from queue import Queue
import asyncio
from find_instrument import FindInstrument
from option_chain_cache import set_option_chain, get_option_chain


combined_pnl = 0

DAILY_TARGET = 3000
DAILY_STOPLOSS = -3000

strategy_stopped = False

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

COMMON_ID = "cb5a60af-883a-4005-b203-f908b23cfdc3"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "NIFTY_OPTION_BUYING_50_reentry"
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()


IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 16)
TRADE_END   = dtime(15, 20)

CE_TARGET_POINTS = 25
TARGET_POINTS = 47
PE_TARGET_POINTS = 25
LOTSIZE = 65

today = datetime.now(IST).strftime("%Y-%m-%d")


# =========================
# LOGIN
# =========================

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)
fno_df = load_fno_master()

strategy_id = "cb5a60af-883a-4005-b203-f908b23cfdc3"

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

def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users,  strike):

    strike = int(float(strike))
    
    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"NIFTY{day}{month}{year}{strike}{name}"
    expiry = expiry_date.strftime("%Y-%m-%d")

    print("Building payload with symbol:", symbol)
    print("Payload details - Name:", name, "Side:", side, "Token:", token, "Reason:", reason, "Event Type:", event_type, "LTP:", ltp, "PnL:", pnl, "Cum PnL:", cum_pnl, "Lot:", lot, "Strike:", strike)


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
        "symbol": str(symbol),
        "exchange": "NFO",
        "expiry":expiry,
        "strike": str(strike),
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


def get_ema_bootstrap_window(minutes=51):

    now = datetime.now(IST)

    end_time = now.replace(second=0, microsecond=0)

    start_time = end_time - timedelta(minutes=minutes)

    return start_time, end_time


def load_history(security_id):

    start_time, end_time = get_ema_bootstrap_window()

    data = dhan.intraday_minute_data(
        security_id=security_id,
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start_time.strftime("%Y-%m-%d"),
        to_date=end_time.strftime("%Y-%m-%d")
    )

    candles = []

    raw = data.get("data", {})

    opens = raw.get("open", [])
    highs = raw.get("high", [])
    lows = raw.get("low", [])
    closes = raw.get("close", [])
    volumes = raw.get("volume", [])
    timestamps = raw.get("timestamp", [])

    for i in range(len(timestamps)):

        ts = datetime.fromtimestamp(timestamps[i], IST)

        if start_time <= ts <= end_time:

            candles.append({
                "timestamp": timestamps[i],
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i],
                "volume": volumes[i]
            })

    return candles[-51:]


def get_next_expiry():
    """
    Returns current/next NIFTY expiry date
    directly from Dhan expiry list API
    """
    expiries = dhan.expiry_list(
        under_security_id=13,
        under_exchange_segment="IDX_I"
    )
    expiry_list = expiries["data"]
    # first expiry is always nearest expiry
    next_expiry = expiry_list["data"][0]

    return next_expiry



def detect_touch(state, candle):

    ema = state["ema50"]

    if ema is None:
        return

    high = float(candle["high"])
    low = float(candle["low"])

    if low <= ema <= high:

        state["touch_armed"] = True

        print("✅ EMA TOUCH")



def update_ema(state, candle):

    state["candles"].append(candle)
    if len(state["candles"]) > 200:
        state["candles"].pop(0)
    closes = [
        float(c["close"])
        for c in state["candles"]
    ]
    state["ema50"] = calculate_ema(closes)
    return state["ema50"]


def handle_leg(name, token, candle, state, ltp):
    global combined_pnl

    now = datetime.now(IST).time()

    close = float(candle["close"])

    avg = (
        float(candle["open"])
        + float(candle["high"])
        + float(candle["low"])
        + float(candle["close"])
    ) / 4

    # =========================
    # TIME EXIT
    # =========================
    if now >= TRADE_END:

        telemetry["status"] = "CLOSED"

        if state["position"]:

            exit_price = ltp

            pnl = (
                exit_price - state["entry_price"]
            ) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            run_async(
                emit_signal(
                    build_payload(
                        name,
                        "SELL",
                        token,
                        "time_exit",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        state["lot"],
                        users,
                        state["strike"]
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TIME EXIT",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            print(f"⏰ {name} TIME EXIT @ {exit_price}")

            state["position"] = False

        state["trading_disabled"] = True
        return

    # =========================
    # STOP TRADING
    # =========================
    if state["trading_disabled"]:
        return

    # =========================
    # EMA UPDATE
    # =========================
    state["candles"].append(candle)

    if len(state["candles"]) > 100:
        state["candles"].pop(0)

    closes = [float(c["close"]) for c in state["candles"]]

    state["ema50"] = calculate_ema(closes, 50)

    if state["ema50"] is None:
        return

    ema50 = state["ema50"]

    # =========================
    # EMA TOUCH DETECTION
    # =========================
    if (
        candle["low"] <= ema50 <= candle["high"]
    ):

        if not state["ema_touched"]:

            state["ema_touched"] = True

            print(
                f"📍 {name} EMA TOUCH | EMA={round(ema50,2)}"
            )

    # =========================
    # TARGET EXIT
    # =========================
    if state["position"]:

        if ltp >= state["entry_price"] + TARGET_POINTS:

            exit_price = ltp

            pnl = (
                exit_price - state["entry_price"]
            ) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            run_async(
                emit_signal(
                    build_payload(
                        name,
                        "SELL",
                        token,
                        "target_hit",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        state["lot"],
                        users,
                        state["strike"]
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TARGET HIT",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            print(f"🎯 {name} TARGET HIT @ {exit_price}")

            state["position"] = False
            state["entry_price"] = None
            state["ema_touched"] = False

            return

    # =========================
    # EMA EXIT
    # =========================
    if state["position"]:

        if close < ema50:

            exit_price = ltp

            pnl = (
                exit_price - state["entry_price"]
            ) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            run_async(
                emit_signal(
                    build_payload(
                        name,
                        "SELL",
                        token,
                        "ema_exit",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        state["lot"],
                        users,
                        state["strike"]
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="EMA EXIT",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            print(
                f"❌ {name} EMA EXIT | Close={close} EMA={round(ema50,2)}"
            )

            state["position"] = False
            state["entry_price"] = None
            state["ema_touched"] = False

            return

    # =========================
    # ENTRY
    # =========================
    if (
        not state["position"]
        and state["ema_touched"]
    ):

        if (
            close > ema50
            and avg > ema50
        ):

            entry_price = ltp

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()
            state["position"] = True

            # require fresh touch after entry
            state["ema_touched"] = False

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            run_async(
                emit_signal(
                    build_payload(
                        name,
                        "BUY",
                        token,
                        "entry",
                        "ENTRY",
                        ltp,
                        state["pnl"],
                        combined_pnl,
                        state["lot"],
                        users,
                        state["strike"]
                    )
                )
            )

            log_trade_event(
                event_type="ENTRY",
                leg_name=name,
                token=token,
                symbol=SYMBOL,
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="EMA50 ENTRY",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            print(
                f"🟢 BUY {name} @ {entry_price} | EMA={round(ema50,2)}"
            )
    

next_expiry = get_next_expiry()


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
        "strike": None,

        "rearm_required": False,

        # EMA strategy
        "ema50": None,
        "ema_touched": False,
        "candles": []
    }



def calculate_ema(closes, period=50):

    if len(closes) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(closes[:period]) / period

    for close in closes[period:]:
        ema = ((close - ema) * multiplier) + ema

    return ema

# =========================
# START
# =========================

wait_for_start()

print("\n🚀 NIFTY OPTION BUYING 50 CUMULATIVE LTP STARTED\n")

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

# =========================
# OPTION CHAIN
# =========================

atm = ATM

oc = dhan.option_chain(
    under_security_id=13,
    under_exchange_segment="IDX_I",
    expiry=str(next_expiry)  
)


option_data = oc["data"]["data"]["oc"]

target = 210

best_ce = None
best_pe = None

best_ce_ltp = float("inf")
best_pe_ltp = float("inf")


for strike, strike_data in option_data.items():

    strike = float(strike)

    # ================= CE =================
    # ONLY ATM OR ITM CE
    if strike <= atm and "ce" in strike_data:

        ce_ltp = strike_data["ce"]["last_price"]

        if ce_ltp >= target and ce_ltp < best_ce_ltp:

            best_ce_ltp = ce_ltp

            best_ce = {
                "strike": strike,
                "ltp": ce_ltp,
                "security_id": strike_data["ce"]["security_id"]
                }

    # ================= PE =================
    # ONLY ATM OR ITM PE
    # ================= PE =================
    
    if strike >= atm and "pe" in strike_data:

        pe_ltp = strike_data["pe"]["last_price"]

        if pe_ltp >= target and pe_ltp < best_pe_ltp:

            best_pe_ltp = pe_ltp

            best_pe = {
                "strike": strike,
                "ltp": pe_ltp,
                "security_id": strike_data["pe"]["security_id"]
            }    # FINAL VALUES


ce_strike = best_ce["strike"]
CE_ID = str(best_ce["security_id"])

pe_strike = best_pe["strike"]
PE_ID = str(best_pe["security_id"])


finder=FindInstrument()

ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")


AngelCE = finder.get_option("NIFTY" , int(ce_strike) , "CE")
AngelPE = finder.get_option("NIFTY" , int(pe_strike) , "PE")

print("angel tokens" , AngelCE , AngelPE)


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
    f"NIFTY CE {ce_strike}",
    str(ce_strike),
    str(today),
    CE_ID
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {pe_strike}",
    str(pe_strike),
    str(today),
    PE_ID
)

start_time, end_time = get_ema_bootstrap_window()


print(start_time)
print(end_time)

ce_history = load_history(CE_ID)
pe_history = load_history(PE_ID)

print("CE History:", len(ce_history))
print("PE History:", len(pe_history))

ce_state = init_state()
pe_state = init_state()

ce_state["candles"] = ce_history.copy()
pe_state["candles"] = pe_history.copy()

ce_closes = [float(c["close"]) for c in ce_state["candles"]]
pe_closes = [float(c["close"]) for c in pe_state["candles"]]

ce_state["ema50"] = calculate_ema(ce_closes)
pe_state["ema50"] = calculate_ema(pe_closes)

print("CE EMA50:", ce_state["ema50"])
print("PE EMA50:", pe_state["ema50"])

ce_state["strike"] = ce_strike
pe_state["strike"] = pe_strike

ce_state["ema_touched"] = False
pe_state["ema_touched"] = False

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

    #if "ce_ltp" in telemetry and "pe_ltp" in telemetry:
    #    universal_exit_check(telemetry["ce_ltp"], telemetry["pe_ltp"])

    # =========================
    # CANDLE LOGIC
    # =========================
    if candle:

        if token == CE_ID:
            #print("50 reentry CE",token)
            #print(candle)
            ema = update_ema(ce_state, candle)
            #handle_leg("CE", token, candle, ce_state, ltp)

        if token == PE_ID:
            #print("50 reentry PE",token)
            #print(candle)
            ema = update_ema(pe_state, candle)
            #handle_leg("PE", token, candle, pe_state, ltp)





instruments = [
    (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Quote),
]

feed = MarketFeed(dhan_context, instruments, "v2")


while True:
    try:

        feed.run_forever()
        msg = feed.get_data()

        if msg:  
            on_message(msg)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()



""" 
TOKENS = [CE_ID , PE_ID]

def on_tick(token, msg):

    if token not in TOKENS:
        return  

    on_message(msg)

for t in TOKENS:
    subscribe(t, on_tick)

"""