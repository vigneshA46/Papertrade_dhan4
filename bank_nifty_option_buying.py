from datetime import datetime, time as dtime
import time
import pytz
from dhanhq import MarketFeed
import threading
from dotenv import load_dotenv
import os
import requests
import pandas as pd
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
from io import StringIO
from queue import Queue
import threading
from dhanhq import DhanContext, dhanhq
from signal_emitter import emit_signal
import asyncio
from find_instrument import FindInstrument
from dispatcher import subscribe



load_dotenv()

LOTSIZE = 30

INTERVAL=1
ATM=None
CE_ID  =None
PE_ID  =None
ACCESS_TOKEN=get_access_token()
CLIENT_ID=os.getenv("CLIENT_ID")
SYMBOL = 'BANKNIFTY'

TRADE_END   = dtime(15, 20)

access_token = get_access_token()
dhan_context = DhanContext(CLIENT_ID, access_token)
dhan = dhanhq(dhan_context)



COMMON_ID = 'f3190e0b-c44f-4242-9eb4-474967faf1f2'
PE_TARGET_POINTS = 100
CE_TARGET_POINTS = 100 
TARGET_POINTS = 100

MAX_LOT = 5

BASE_URL = "https://api.dhan.co/v2"
FNO_MASTER_URL = f"{BASE_URL}/instrument/NSE_FNO"
IDX_INTRADAY_URL="https://api.dhan.co/v2/charts/intraday"

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"



IST = pytz.timezone("Asia/Kolkata")

HEADERS = {
    "Accept":"application/json",
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN,
    "client-id": "1107425275"
}

IDXHEADERS = {
    "Content-Type": "application/json",
    "access-token": ACCESS_TOKEN,
}

# ================= STRATEGY CONFIG =================

TSL_TRIGGER = 40
SL_GAP = 15
TRAIL_STEP = 10

TICK_ENTRY_BUFFER = 25
TICK_EXIT_BUFFER = 25

MAX_LOT = 5
TARGET_PNL = 100

current_lot = 1
trading_enabled = True


cumulative_pnl = 0
max_profit = 0
max_dd = 0
total_trades = 0
ce_trades = 0
pe_trades = 0
engine_start_time = datetime.now(IST).strftime("%H:%M:%S")



strategy_id = "f3190e0b-c44f-4242-9eb4-474967faf1f2"

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

def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users):

    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    symbol = f"BANKNIFTY{day}{month}{year}{ATM}{name}"
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
        "zebusymbol": "BANKNIFTY",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "BANKNIFTY",
        "reason":reason
    }

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

threading.Thread(target=trade_log_worker, daemon=True).start()



telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "ACTIVE",
    "pnl": 0,
    "pnl_percentage": 0,
    "ce_ltp": 0,
    "pe_ltp": 0,
    "ce_pnl": 0,
    "pe_pnl": 0
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



def wait_for_start():
    print("⏳ Waiting for 09:16:00 ...")
    while True:
        now = datetime.now(IST).time()
        if now >= dtime(9, 16):
            print("✅ Market Start Triggered")
            break
        time.sleep(1)

def calculate_atm(price, step=100):
    return int(round(price / step) * step)

def fetch_index_intraday(trade_date: str):
    payload = {
        "securityId": "25",
        "exchangeSegment": "IDX_I",
        "instrument": "INDEX",
        "interval": INTERVAL,
        "fromDate": f"{trade_date} 09:14:00",
        "toDate": f"{trade_date} 09:16:00"
    }

    r = requests.post(IDX_INTRADAY_URL, headers=IDXHEADERS, json=payload)
    r.raise_for_status()
    data = r.json()

    df = pd.DataFrame({
        "timestamp": data["timestamp"],
        "open": data["open"],
        "high": data["high"],
        "low": data["low"],
        "close": data["close"]
    })

    dt = pd.to_datetime(df["timestamp"], unit="s", utc=True)
    df["datetime"] = dt.dt.tz_convert(IST)
    df.sort_values("datetime", inplace=True)
    df.reset_index(drop=True, inplace=True)

    return df

def find_option_security(df, strike, option_type, trade_date):
    trade_date = pd.to_datetime(trade_date)

    opt = df[
        (df["INSTRUMENT"] == "OPTIDX") &
        (df["UNDERLYING_SYMBOL"] == "BANKNIFTY") &
        (df["STRIKE_PRICE"] == strike) &
        (df["OPTION_TYPE"] == option_type) &
        (df["SM_EXPIRY_DATE"] >= trade_date)
    ]

    if opt.empty:
        raise ValueError(f"❌ No {option_type} found for strike {strike}")

    return opt.sort_values("SM_EXPIRY_DATE").iloc[0]


def get_banknifty_atm(trade_date):
    df = fetch_index_intraday(trade_date)

    first_candle = df.iloc[0]
    close_price = first_candle["close"]

    atm = calculate_atm(close_price)

    print(f"📌 BANKNIFTY Close @09:15 = {close_price}")
    print(f"🎯 ATM Strike = {atm}")

    return atm


def discover_options(atm, trade_date):
    df = load_fno_master()

    ce = find_option_security(df, atm, "CE", trade_date)
    pe = find_option_security(df, atm, "PE", trade_date)

    print(f"✅ CE -> {ce['SECURITY_ID']} {ce['DISPLAY_NAME']}")
    print(f"✅ PE -> {pe['SECURITY_ID']} {pe['DISPLAY_NAME']}")

    return ce, pe



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


def logtradeleg(strategyid, leg, symbol, strike_price, date, token):
    url = "https://algoapi.dreamintraders.in/api/tradelegs/create"
    
    payload = {
        "strategy_id": strategyid,
        "leg": leg,
        "symbol": symbol,
        "strike_price": strike_price,
        "date": date,
        "token": token
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


def log_trade_event(
    event_type,
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
        "trade_id": COMMON_ID,

        "event_type": event_type,
        "leg_name": leg_name,
        "token": int(token),
        "symbol": symbol,

        "side": side,
        "lots": lot,
        "quantity": lot * LOTSIZE,

        "price": float(price),  # 🔥 safety

        "reason": reason,
        "deployed_by": COMMON_ID,
        "pnl": str(pnl),
        "cum_pnl": str(cum_pnl),
    }

    # 🔥 NON-BLOCKING
    trade_log_queue.put(payload)



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
        "tsl": None,
        "sl": None,
        "trailing_active": False,
        "moment":0.0
    }


def on_message(msg):

    global combined_pnl , current_lot

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0)or 0)

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
    # Entry +8 Breakout
    # =========================

    if token == CE_ID:
        state = ce_state
        leg_name = "CE"
    elif token == PE_ID:
        state = pe_state
        leg_name = "PE"
    else:
        state = None

    if state and state["marked"] is None:
        return


    if state and not state["position"] and not state["trading_disabled"]:

        if state["rearm_required"]:
            if ltp < state["marked"]:
                state["rearm_required"] = False
            else:
                return

        if ltp >= state["marked"] + 25:


            entry_price = ltp

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()

            state["position"] = True
            state["tsl"] = entry_price + 40
            state["sl"] = entry_price + 25
            state["trailing_active"] = False

            entry_lot = current_lot   

            if current_lot < MAX_LOT:
                current_lot += 1
            else:
                current_lot = 1

            state["lot"] = entry_lot

            
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            print("🟢 BUY (TICK +25)", leg_name, entry_price)
            run_async(emit_signal(build_payload(leg_name, "BUY", token, "entry", "ENTRY", ltp, state["pnl"], combined_pnl,state["lot"],users)))

            log_trade_event(
                event_type="ENTRY",
                leg_name=leg_name,
                token=token,
                symbol="BANKNIFTY",
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="TICK +25 ENTRY",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            #increment_lot()


    # =========================
    # TSL EXIT (TICK BASED)
    # =========================
    if state["position"] and state["trailing_active"]:

        if ltp <= state["sl"]:

            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            current_moment = exit_price - state["entry_price"]
            state["moment"] = current_moment

            state["pnl"] += pnl
            combined_pnl += pnl

                        
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            print("🔴 TSL EXIT (TICK)", leg_name, exit_price)
            run_async(emit_signal(build_payload(leg_name, "SELL", token, "exit", "EXIT", ltp, state["pnl"], combined_pnl,state["lot"],users)))

            log_trade_event(
                event_type="EXIT",
                leg_name=leg_name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TSL HIT (TICK)",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            state["position"] = False
            current_lot = 1
            state["lot"] = 1
            #reset_lot()
            state["rearm_required"] = True

            return
    # =========================
    # -8 EXIT (TICK LEVEL)
    # =========================
    if state and state["position"]:

        # =========================
        # TSL ACTIVATION (TICK)
        # =========================
        if not state["trailing_active"]:

            if ltp >= state["tsl"]:
                state["trailing_active"] = True

                print("⚡ TSL ACTIVATED (TICK)", leg_name, "TSL:", state["tsl"], "SL:", state["sl"])


        # =========================
        # TSL TRAILING (TICK)
        # =========================
        if state["trailing_active"]:

            if ltp >= state["tsl"] + TRAIL_STEP:

                move_steps = int((ltp - state["tsl"]) // TRAIL_STEP)

                state["tsl"] += move_steps * TRAIL_STEP
                state["sl"] += move_steps * TRAIL_STEP

                print("📈 TSL MOVED (TICK)", leg_name, "TSL:", state["tsl"], "SL:", state["sl"])


        if ltp <= state["marked"]:

            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl
            current_moment = exit_price - state["entry_price"]
            state["moment"] = current_moment

            
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)


            print("🔴 EXIT (-25 TICK)", leg_name, exit_price)
            run_async(emit_signal(build_payload(leg_name, "SELL", token, "exit", "EXIT", ltp, state["pnl"], combined_pnl,state["lot"],users)))

            log_trade_event(
                event_type="EXIT",
                leg_name=leg_name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TICK EXIT -25",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )
            state["rearm_required"] = True
            state["position"] = False   
            return  


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
            print("CE",token)
            print(candle)
            handle_leg("CE", token, candle, ce_state, ltp)

        if token == PE_ID:
            print("PE",token)
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


def handle_leg(name, token, candle, state, ltp):

    global combined_pnl , current_lot

    now = datetime.now(IST).time()

    close = candle["close"]
    avg = (candle["open"] + candle["high"] +
           candle["low"] + candle["close"]) / 4

    timestamp = candle["timestamp"]

    # =========================
    # TIME EXIT (15:20)
    # =========================
    if now >= TRADE_END:

        telemetry["status"] = 'CLOSED'

        if state["position"]:
            exit_price = ltp 

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

            
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            run_async(emit_signal(build_payload(name, "SELL", token, "exit", "EXIT", ltp, state["pnl"], combined_pnl,state["lot"],users)))
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

    if state["rearm_required"]:
        if close < state["marked"]:
            state["rearm_required"] = False
        else:
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
            state["tsl"] = entry_price + 40
            state["sl"] = entry_price + 25
            state["trailing_active"] = False

            entry_lot = current_lot

            if current_lot <= MAX_LOT:
                current_lot += 1
            else:
                current_lot = 1

            state["lot"] = entry_lot

            
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            print("🟢 BUY", name, entry_price)
            run_async(emit_signal(build_payload(name, "BUY", token, "entry", "ENTRY", ltp, state["pnl"], combined_pnl,state["lot"],users)))

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
                cum_pnl= combined_pnl
                )

            log_event(f"{name} BUY", token, "ENTRY_EXECUTED", entry_price, "Trade opened")
           


def universal_exit_check(ce_ltp, pe_ltp):

    global combined_pnl , current_lot

    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (ce_ltp - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

    if ce_state["position"] or pe_state["position"]:
        telemetry["status"] = 'RUNNING'

    total = ce_state["pnl"] + pe_state["pnl"] + ce_running + pe_running


    if ce_state["position"] and ce_state["moment"] >= CE_TARGET_POINTS and not ce_state["trading_disabled"]:

        print("🏁 CE 100 points hit")

        
        deployments = get_today_deployments()

        users = group_users_by_broker(deployments)

        print("FORMATTED USERS:", users)
        
        exit_price = ce_ltp
        pnl = (exit_price - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

        current_moment = exit_price - ce_state["entry_price"]
        ce_state["moment"] =0.0

        ce_state["pnl"] += pnl
        combined_pnl += pnl
            
        run_async(emit_signal(build_payload("CE", "SELL", CE_ID, "exit", "EXIT", ce_ltp, ce_state["pnl"], combined_pnl,ce_state["lot"],users)))
        log_trade_event(
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=ce_state["lot"],
                price=exit_price,
                reason="COMBINED EXIT",
                pnl=ce_state["pnl"],
                cum_pnl=combined_pnl
            )
            #increment_lot()
        current_lot = 1
        ce_state["lot"] = 1
        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True
        ce_state["rearm_required"] = True
        ce_state["position"] = False

        return
        
            
            

    if pe_state["position"] and pe_state["moment"] >= PE_TARGET_POINTS and not pe_state["trading_disabled"]:

        print("🏁 PE 50 points hit")
        
        deployments = get_today_deployments()

        users = group_users_by_broker(deployments)

        print("FORMATTED USERS:", users)
        
        exit_price = pe_ltp
        pnl = (exit_price - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

        current_moment = exit_price - pe_state["entry_price"]
        pe_state["moment"] =0.0

        pe_state["pnl"] += pnl
        combined_pnl += pnl
            
        run_async(emit_signal(build_payload("PE", "SELL", PE_ID, "exit", "EXIT", pe_ltp, pe_state["pnl"], combined_pnl,pe_state["lot"],users)))
        log_trade_event(
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=pe_state["lot"],
                price=exit_price,
                reason="COMBINED EXIT",
                pnl=pe_state["pnl"],
                cum_pnl=combined_pnl
            )

        current_lot = 1
        pe_state["lot"] = 1
        pe_state["trading_disabled"] = True
        ce_state["trading_disabled"] = True
        pe_state["rearm_required"] = True
        pe_state["position"] = False


        return   # 🚨 prevent further checks








today = datetime.now(IST).strftime("%Y-%m-%d")

wait_for_start()

ATM = get_banknifty_atm(today)

ce, pe = discover_options(ATM, today)

CE_ID = str(ce["SECURITY_ID"])
PE_ID = str(pe["SECURITY_ID"])   # <-- FIXED

finder=FindInstrument()

AngelCE = finder.get_option("BANKNIFTY" , int(ATM) , "CE")
AngelPE = finder.get_option("BANKNIFTY" , int(ATM) , "PE")

print("angel tokens" , AngelCE , AngelPE)

print("security ids")
print(CE_ID, PE_ID)



builders = {
    CE_ID: OneMinuteCandleBuilder(),
    PE_ID: OneMinuteCandleBuilder()
        }

# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"BANK NIFTY CE {str(ATM)}",
    ATM,
    str(today),
    str(CE_ID)
    )   

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"BANK NIFTY PE {str(ATM)}",
    str(ATM),
    str(today),
    str(PE_ID)
    )

    
# =========================
# STATE
# =========================

ce_state = init_state()
pe_state = init_state()

combined_pnl = 0

ce_state["marked"] = get_first_candle_mark(str(CE_ID))
pe_state["marked"] = get_first_candle_mark(str(PE_ID))


instruments = [
    (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Quote)
    ]


feed = MarketFeed(dhan_context, instruments, "v2")

TOKENS = [CE_ID , PE_ID]



def on_tick(token, msg):
    if token not in TOKENS:
        return  

    on_message(msg)

for t in TOKENS:
    subscribe(t, on_tick)



""" 
while True:
    try:
        feed.run_forever()
        data = feed.get_data()

        if data:
                
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
 """