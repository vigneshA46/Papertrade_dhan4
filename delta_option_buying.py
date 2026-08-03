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
from dispatcher import subscribe
from queue import Queue
import asyncio
from signal_emitter import emit_signal
from find_instrument import FindInstrument
from option_chain_cache import set_option_chain, get_option_chain


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

threading.Thread(target=trade_log_worker, daemon=True).start()

ATM = None 
TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

COMMON_ID = "a6bbea5c-ee7e-41c2-b39a-fb4703422d36"
SYMBOL = "NIFTY"

load_dotenv()

STRATEGY_NAME = "DELTA_OPTION_BUYING_100"

CLIENT_ID = os.getenv("CLIENT_ID")

IST = pytz.timezone("Asia/Kolkata")
DHAN_OPTION_CHAIN_URL = "https://api.dhan.co/v2/optionchain"


TRADE_START = dtime(9, 16)
TRADE_END   = dtime(15, 20)

TARGET_POINTS = 100
CE_TARGET_POINTS = 100
PE_TARGET_POINTS = 100
LOTSIZE = 65

today = datetime.now(IST).strftime("%Y-%m-%d")
#today = "2026-04-06"
strategy_id = "a6bbea5c-ee7e-41c2-b39a-fb4703422d36"

access_token = get_access_token()
dhan_context = DhanContext(CLIENT_ID, access_token)
dhan = dhanhq(dhan_context)

fno_df=load_fno_master()


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


def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users , strike):

    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    if name == "CE":
        symbol = f"NIFTY{day}{month}{year}{ce_strike}{name}"
    else:
        symbol = f"NIFTY{day}{month}{year}{pe_strike}{name}"

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

telemetry = {
    "strategy_id": COMMON_ID,
    "run_id": COMMON_ID,
    "status": "RUNNING",
    "pnl": 0,
    "pnl_percentage": 0,
    "ce_ltp": 0,
    "pe_ltp": 0,
    "ce_pnl": 0,
    "pe_pnl": 0
}

def get_first_candle_mark(security_id):

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

        if ts.hour == 9 and 15 <= ts.minute <= 17:
            mark = float(closes[i])
            print(f"📍 HIST MARK {security_id} @ {mark}")
            return mark

    print("❌ 09:15 candle not found")
    return None


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

next_expiry = get_next_expiry()

def get_high_delta_strikes(access_token, client_id):
    payload = {
        "UnderlyingScrip": 13,   # NIFTY
        "UnderlyingSeg": "IDX_I",
        "Expiry": get_next_expiry()
    }

    headers = {
        "access-token": access_token,
        "client-id": client_id,
        "Content-Type": "application/json"
    }

    response = requests.post(DHAN_OPTION_CHAIN_URL, json=payload, headers=headers)
    data = response.json()

    if data.get("status") != "success":
        raise Exception(f"Option chain fetch failed: {data}")

    oc = data["data"]["oc"]

    best_ce_strike = None
    best_pe_strike = None

    min_ce_diff = float("inf")
    min_pe_diff = float("inf")

    for strike, strike_data in oc.items():
        strike = float(strike)

        # ---------- CE ----------
        ce = strike_data.get("ce")
        if ce and "greeks" in ce:
            delta = ce["greeks"].get("delta")

            if delta is not None and delta >= 0.85:
                diff = abs(delta - 0.85)

                if diff < min_ce_diff:
                    min_ce_diff = diff
                    best_ce_strike = strike

        # ---------- PE ----------
        pe = strike_data.get("pe")
        if pe and "greeks" in pe:
            delta = pe["greeks"].get("delta")

            if delta is not None and delta <= -0.85:
                diff = abs(abs(delta) - 0.85)

                if diff < min_pe_diff:
                    min_pe_diff = diff
                    best_pe_strike = strike

    if best_ce_strike is None or best_pe_strike is None:
        raise Exception("No suitable strikes found with delta ≥ 0.85")

    return best_ce_strike, best_pe_strike


# =========================
# LOGIN
# =========================

access_token = get_access_token()
dhan_context = DhanContext(CLIENT_ID, access_token)
dhan = dhanhq(dhan_context)

fno_df=load_fno_master()


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

        
def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)


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
        "moment":0.0,
        "strike":None
    }

wait_for_start()

print("next expiry")
print(get_next_expiry())

#ce_strike, pe_strike = get_high_delta_strikes(access_token, CLIENT_ID)

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




CE_STRIKE = ATM - 400
PE_STRIKE = ATM + 400




# =========================
# OPTION SELECTION
# =========================


atm = ATM

#oc = dhan.option_chain(
#    under_security_id=13,
#    under_exchange_segment="IDX_I",
#    expiry=str(next_expiry)  
#)

TOKENS = []




print("🚀 starting delta setup")

    #oc = dhan.option_chain(
    #    under_security_id=13,
    #    under_exchange_segment="IDX_I",
    #    expiry=str(next_expiry)
    #)

    # cache set
    #set_option_chain(oc)

    #oc = get_option_chain()

    #option_data = oc["data"]["data"]["oc"]

    #print("option chain ready")



    #target = 400

best_ce = None
best_pe = None


ce_strike = str(CE_STRIKE)
pe_strike = str(PE_STRIKE)


finder=FindInstrument()

ce_row = find_option_security(fno_df, CE_STRIKE, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, PE_STRIKE, "PE", today, "NIFTY")

CE_ID = str(ce_row["SECURITY_ID"])
PE_ID = str(pe_row["SECURITY_ID"])


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


    # =========================
    # STATE
    # =========================

ce_state = init_state()
pe_state = init_state()

ce_state["strike"] = float(ce_strike)
pe_state["strike"] = float(pe_strike)

combined_pnl = 0

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

        telemetry["status"]='CLOSED'

        if state["position"]:
            exit_price = ltp 

            pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

            state["pnl"] += pnl
            combined_pnl += pnl

                
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)


            run_async(emit_signal(build_payload(name, "SELL", token , "exit","EXIT", ltp, pnl, combined_pnl,state["lot"],users , state["strike"])))
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
            state["tsl"] = entry_price + 30
            state["sl"] = entry_price + 20
            state["trailing_active"] = False

                
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            print("🟢 BUY ", name, entry_price)
            run_async(emit_signal(build_payload(name, "BUY", token, "entry", "ENTRY", ltp, state["pnl"], combined_pnl,state["lot"],users , state["strike"])))

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

    global combined_pnl , CE_TARGET_POINTS , PE_TARGET_POINTS

    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (ce_ltp - ce_state["entry_price"]) * LOTSIZE * ce_state["lot"]

    if pe_state["position"]:
        pe_running = (pe_ltp - pe_state["entry_price"]) * LOTSIZE * pe_state["lot"]

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

        pe_state["lot"] = 1
        pe_state["trading_disabled"] = True
        ce_state["trading_disabled"] = True
        pe_state["rearm_required"] = True
        pe_state["position"] = False


        return   # 🚨 prevent further checks


def tick_tsl_exit(name, token, state, ltp):
    global combined_pnl

    if not state["position"]:
        return

    if state["position"]:

        if not state["trailing_active"] and ltp >= state["tsl"]:
            state["trailing_active"] = True
            state["sl"] = state["tsl"] - 10
            print(f"🔥 {name} TSL ACTIVATED")

        if state["trailing_active"]:
            if ltp >= state["tsl"] + 10:
                state["tsl"] += 10
                state["sl"] += 10
                print(f"🔁 {name} TRAIL -> TSL:{state['tsl']} SL:{state['sl']}")



    if state["trailing_active"] and ltp <= state["sl"]:

        exit_price = ltp

        pnl = (exit_price - state["entry_price"]) * LOTSIZE * state["lot"]

        state["pnl"] += pnl
        combined_pnl += pnl

        current_moment = exit_price - state["entry_price"]
        state["moment"] = current_moment

            
        deployments = get_today_deployments()

        users = group_users_by_broker(deployments)

        print("FORMATTED USERS:", users)

        print("⚡ TSL TICK EXIT", name, exit_price)
        run_async(emit_signal(build_payload(name, "SELL", token, "exit", "EXIT", ltp, state["pnl"], combined_pnl,state["lot"],users , state["strike"])))

        log_trade_event(
            event_type="EXIT",
            leg_name=name,
            token=token,
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=exit_price,
            reason="TSL HIT (Tick)",
            pnl=state["pnl"],
            cum_pnl=combined_pnl
        )

        state["position"] = False
        state["lot"] = 1
        state["rearm_required"] = True


    # =========================
    # CALLBACKS
    # =========================


def on_message(msg):

    global combined_pnl

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
        tick_tsl_exit("CE", token, ce_state, ltp)
        telemetry["ce_ltp"] = float(ltp or 0)

    if token == PE_ID:
        tick_tsl_exit("PE", token, pe_state, ltp)
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

        if ltp >= state["marked"] + 8:

            entry_price = ltp

            state["entry_price"] = entry_price
            state["entry_time"] = datetime.now(IST).isoformat()

            state["position"] = True
            state["tsl"] = entry_price + 30
            state["sl"] = entry_price + 20
            state["trailing_active"] = False

                
            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)

            print("🟢 BUY (TICK +8)", leg_name, entry_price)
            run_async(emit_signal(build_payload(leg_name, "BUY", token, "entry", "ENTRY", ltp, state["pnl"], combined_pnl,state["lot"],users , state["strike"])))

            log_trade_event(
                event_type="ENTRY",
                leg_name=leg_name,
                token=token,
                symbol="NIFTY",
                side="BUY",
                lot=state["lot"],
                price=entry_price,
                reason="TICK +8 ENTRY",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

        # =========================
        # -8 EXIT (TICK LEVEL)
        # =========================
    if state and state["position"]:

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


            print("🔴 EXIT (marked TICK)", leg_name, exit_price)
            run_async(emit_signal(build_payload(leg_name, "SELL", token, "exit", "EXIT", ltp, state["pnl"], combined_pnl,state["lot"],users , state["strike"])))

            log_trade_event(
                event_type="EXIT",
                leg_name=leg_name,
                token=token,
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=exit_price,
                reason="TICK EXIT -8",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            state["position"] = False
            state["rearm_required"]=True
            if state["lot"] < 15:
                state["lot"] += 1

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
            print("Delta CE",token)
            print(candle)
            handle_leg("CE", token, candle, ce_state, ltp)

        if token == PE_ID:
            print("Delta PE",token)
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

    if ce_state["position"] or pe_state["position"]:
        telemetry["status"] = 'RUNNING'

    telemetry["ce_pnl"] = ce_state["pnl"] + ce_running
    telemetry["pe_pnl"] = pe_state["pnl"] + pe_running
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]

    # ========================= 
    # START WEBSOCKET
    # =========================

instruments = [
    (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Quote),
    (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Quote)
]

TOKENS = [
str(CE_ID) , str(PE_ID)
]

MY_TOKENS = [CE_ID , PE_ID]


def on_tick(token, msg):

    if token not in TOKENS:
        return  
    on_message(msg)

        
for t in TOKENS:
    subscribe(t, on_tick)