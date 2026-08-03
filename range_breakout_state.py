import time
import pytz
import requests
import pandas as pd
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import dhanhq,DhanContext
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from dispatcher import subscribe
from find_security import load_fno_master, find_option_security
from queue import Queue
import threading
from signal_emitter import emit_signal
import asyncio
from find_instrument import FindInstrument
# =========================
# CONFIG
# =========================



ATM = None 

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

COMMON_ID = "bbfe888c-60f9-4968-acf1-2320ce69ce8d"
SYMBOL = "NIFTY"
symbol="NIFTY"

load_dotenv()


IST = pytz.timezone("Asia/Kolkata")


INDEX_TOKEN = "13"

TRADE_START = dtime(10, 1)
TRADE_END   = dtime(15, 20)

LOT_QTY = 1
DAY_TARGET = 38
LOT = 1
LOTSIZE= 65

today = datetime.now(IST).strftime("%Y-%m-%d")
#today = "2026-04-01"

# =========================
# LOGIN
# =========================
access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

builder = OneMinuteCandleBuilder()
fno_df = load_fno_master()

strategy_id = "bbfe888c-60f9-4968-acf1-2320ce69ce8d"
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


def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users , strike=ATM):

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

idx_builder = OneMinuteCandleBuilder()
#opt_builder = OneMinuteCandleBuilder()

CE_ID = None
PE_ID = None



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
        "pnl": str(pnl * 65),
        "cum_pnl": str(cum_pnl * 65),
    }

    # 🔥 NON-BLOCKING
    trade_log_queue.put(payload)


# =========================
# HELPERS
# =========================

def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)


def calculate_atm(price, step=50):
    return int(round(price / step) * step)

def mark_range():
    global top_line, bottom_line, CE_ID, PE_ID, ce_strike, pe_strike,today,AngelCE,AngelPE,ce_row,ATM

    #today = datetime.now(IST).strftime("%Y-%m-%d")
    idx = dhan.intraday_minute_data(
        security_id=13,   
        exchange_segment="IDX_I",
        instrument_type="INDEX",
        from_date=f"{today} 9:50:00",
        to_date=f"{today} 10:15:00",
        interval=5
    )

    print(idx)

    data = idx.get("data", {})
    opens = data.get("open", [])
    highs = data.get("high", [])
    lows = data.get("low", [])
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    candle = None

    for i in range(len(timestamps)):
        ts = datetime.fromtimestamp(timestamps[i], IST)
        print(ts)

        if ts.hour == 9 and ts.minute == 55:
            candle = {
                "open": opens[i],
                "high": highs[i],
                "low": lows[i],
                "close": closes[i]
            }
            break

    if candle:
        c = candle

        top_line = max(c["open"], c["close"])
        bottom_line = min(c["open"], c["close"])

        ATM = calculate_atm(c["close"])

    else:
        print("Waiting for 09:55 candle...")

    print(candle)
    
    finder = FindInstrument()

    ce_strike = ATM - 400
    pe_strike = ATM + 400

    ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
    pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")

    CE_ID = str(ce_row["SECURITY_ID"])
    PE_ID = str(pe_row["SECURITY_ID"])

    AngelCE = finder.get_option("NIFTY" , int(ATM) , "CE")
    AngelPE = finder.get_option("NIFTY" , int(ATM) , "PE")

    
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

    print("legs logged successfully")



    print("\n📏 RANGE MARKED")
    print("TOP    :", top_line)
    print("BOTTOM :", bottom_line)
    print("ATM    :", ATM)
    print(" Range CE     :", ce_strike, CE_ID)
    print(" Range PE     :", pe_strike, PE_ID)





def on_tick_index(msg):
    # convert scaled price → real price
    #msg["last_price"] = float(msg["LTP"]) * 3.67
    #msg["LTP"] = msg["last_price"]

    global top_line , bottom_line ,pe_state , ce_state

    ltp = float(msg.get("LTP", 0))


    candle = idx_builder.process_tick(msg)

    if pe_state["position"] and ltp < top_line:
        print("❌ PE EXIT (Index crossed below top line)")

        pe_state["force_exit"] = True
        pe_state["exit_reason"] = "INDEX_EXIT"

    # CE EXIT → close above bottom line
    if ce_state["position"] and ltp > bottom_line:
        print("❌ CE EXIT (Index crossed above bottom line)")

        ce_state["force_exit"] = True
        ce_state["exit_reason"] = "INDEX_EXIT"


    # =========================================================
    # 🔁 REARM LOGIC
    # =========================================================

    # PE rearm → price back below top_line
    if pe_state["rearm_required"] and ltp < top_line:
        print("🔁 PE REARMED")
        pe_state["rearm_required"] = False

    # CE rearm → price back above bottom_line
    if ce_state["rearm_required"] and ltp > bottom_line:
        print("🔁 CE REARMED")
        ce_state["rearm_required"] = False



    if candle:
        on_index_candle(msg["security_id"], datetime.now(IST), candle)

def init_state():
    return {
        "position": False,
        "trading_disabled": False,
        "enter_now":False,

        "entry_price": None,
        "entry_time": None,

        "sl": None,
        "tsl": None,
        "tsl_active": False,
        "force_exit":False,

        "lot": 1,
        "pnl": 0.0,

        "rearm_required": False,   # prevents immediate re-entry
        "last_exit_reason": None
    }


def on_index_candle(token, timestamp, candle):
    global ce_state, pe_state, top_line, bottom_line

    current_time = timestamp.time()

    if current_time < TRADE_START or current_time > TRADE_END:
        return

    o = candle["open"]
    h = candle["high"]
    l = candle["low"]
    c = candle["close"]

    avg_price = (o + h + l + c) / 4

    print(f"\n🕯 Candle | O:{o} H:{h} L:{l} C:{c} AVG:{avg_price} TIME: {current_time}")

    # =========================================================
    # 🔴 EXIT LOGIC (INDEX BASED)
    # =========================================================

    # PE EXIT → close below top line


    # =========================================================
    # 🚨 SIGNAL GENERATION
    # =========================================================

    # PE SELL SIGNAL (breakout above)
    if (
        c > top_line and
        avg_price > top_line and
        avg_price < c and

        not pe_state["position"] and
        not pe_state["trading_disabled"] and
        not pe_state["rearm_required"]
    ):
        print("🚨 PE SELL SIGNAL")

        pe_state["enter_now"] = True
        pe_state["signal_time"] = timestamp

    # CE SELL SIGNAL (breakout below)
    if (
        c < bottom_line and
        avg_price < bottom_line and
        avg_price > c and

        not ce_state["position"] and
        not ce_state["trading_disabled"] and
        not ce_state["rearm_required"]
    ):
        print("🚨 CE SELL SIGNAL")

        ce_state["enter_now"] = True
        ce_state["signal_time"] = timestamp



def on_option_tick(msg):
    global ce_state, pe_state, telemetry

    if msg["type"] != 'Quote Data':
        return
    
    now = datetime.now(IST).time()

    token = str(msg["security_id"])
    ltp = float(msg["LTP"])


    # =========================
    # SELECT STATE
    # =========================
    if token == CE_ID:
        state = ce_state
        leg_name = "CE"
    elif token == PE_ID:
        state = pe_state
        leg_name = "PE"
    else:
        return

    # update LTP telemetry
    if leg_name == "CE":
        telemetry["ce_ltp"] = ltp
    else:
        telemetry["pe_ltp"] = ltp

    # =========================
    # 🟢 ENTRY
    # =========================
    if state["enter_now"] and not state["position"]:

        state["entry_price"] = ltp
        state["position"] = True
        state["enter_now"] = False

        # SL / TSL init
        state["tsl"] = ltp - 40  # profit trigger
        state["sl"] = ltp - 30   # loss side (SELL)
        state["tsl_active"] = False

        print(f"✅ {leg_name} ENTRY @ {ltp}")


        #log_event(leg_name, token, "ENTRY", ltp, "Breakout Entry")
        print(f"{leg_name}, {token}, {SYMBOL}, {state['lot']}, {ltp},{telemetry['pnl']}")

        deployments = get_today_deployments()
        users = group_users_by_broker(deployments)
        print("FORMATTED USERS:", users)

        run_async(
            emit_signal(
                build_payload(
                    leg_name,
                    "SELL",
                    token,
                    "entry",
                    "ENTRY",
                    ltp,
                    0,
                    telemetry["pnl"],
                    state["lot"],
                    users,
                    strike = ce_strike if leg_name == "CE" else pe_strike
                )
            )
        )
        log_trade_event(
                event_type="ENTRY",
                leg_name=str(leg_name),
                token=int(token),
                symbol=SYMBOL,
                side="SELL",
                lot=state["lot"],
                price=ltp,
                reason="TIME EXIT",
                pnl= 0,
                cum_pnl=telemetry["pnl"]
                )


    # =========================
    # 🔴 POSITION MANAGEMENT
    # =========================
    if state["position"]:


        if now >= TRADE_END:

            telemetry["status"] = "CLOSED"

            print(f"⏰ {leg_name} TIME EXIT @ {ltp}")

            exit_price = ltp
            final_pnl = state["entry_price"] - exit_price

            state["pnl"] = final_pnl
            telemetry["pnl"] += final_pnl

            state["position"] = False
            state["enter_now"] = False
            state["rearm_required"] = False
            state["trading_disabled"] = True
            state["force_exit"] = False
            state["tsl_active"] = False

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)
            print("FORMATTED USERS:", users)

            run_async(
                emit_signal(
                    build_payload(
                        leg_name,
                        "BUY",
                        token,
                        "time exit",
                        "EXIT",
                        ltp,
                        final_pnl,
                        telemetry["pnl"],
                        state["lot"],
                        users,
                        strike = ce_strike if leg_name == "CE" else pe_strike
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name=str(leg_name),
                token=token,
                symbol=SYMBOL,
                side="BUY",
                lot=state["lot"],
                price=exit_price,
                reason="TIME EXIT",
                pnl=float(final_pnl),
                cum_pnl=telemetry["pnl"]
            )

            return

        # =========================
        # ⚡ FORCE EXIT (INDEX BASED)
        # =========================
        if state.get("force_exit"):

            print(f"⚡ {leg_name} FORCE EXIT @ {ltp}")

            exit_price = ltp
            final_pnl = state["entry_price"] - exit_price

            telemetry["pnl"] += final_pnl

            state["position"] = False
            state["rearm_required"] = True
            state["force_exit"] = False

            #log_event(leg_name, token, "EXIT", ltp, "INDEX EXIT")
            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)
            print("FORMATTED USERS:", users)

            run_async(
                emit_signal(
                    build_payload(
                        leg_name,
                        "BUY",
                        token,
                        "index exit",
                        "EXIT",
                        ltp,
                        final_pnl,
                        telemetry["pnl"],
                        state["lot"],
                        users,
                        strike = ce_strike if leg_name == "CE" else pe_strike
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name=str(leg_name),
                token=token,
                symbol=SYMBOL,
                side="BUY",
                lot=state["lot"],
                price=ltp,
                reason="INDEX EXIT",
                pnl= float(final_pnl),
                cum_pnl=telemetry["pnl"]
                )

              

        entry = state["entry_price"]

        # SELL PnL
        pnl = entry - ltp
        state["pnl"] = pnl

        # update telemetry
        if leg_name == "CE":
            telemetry["ce_pnl"] = pnl
        else:
            telemetry["pe_pnl"] = pnl

        # =========================
        # 🔥 ACTIVATE TSL
        # =========================
        if not state["tsl_active"] and ltp <= state["tsl"]:
            state["tsl_active"] = True
            print(f"🔥 {leg_name} TSL ACTIVATED")

        # =========================
        # 🔁 TRAILING LOGIC (STEP BASED)
        # =========================
        if state["tsl_active"]:

        # 🔽 Trail as long as price keeps moving
            if ltp <= state["tsl"] - 10:
                state["tsl"] -= 10
                state["sl"]  -= 10

                print(f"🔁 {leg_name} TRAIL -> TSL:{state['tsl']} SL:{state['sl']}")

            # =========================
            # ❌ SL HIT
            # =========================
            if ltp >= state["sl"]:
                print(f"❌ {leg_name} SL HIT @ {ltp}")

                exit_price = ltp
                final_pnl = state["entry_price"] - exit_price

                telemetry["pnl"] += final_pnl

                state["position"] = False
                state["rearm_required"] = True
                state["tsl_active"] = False

                deployments = get_today_deployments()
                users = group_users_by_broker(deployments)
                print("FORMATTED USERS:", users)

                run_async(
                    emit_signal(
                        build_payload(
                            leg_name,
                            "BUY",
                            token,
                            "sl hit",
                            "EXIT",
                            ltp,
                            final_pnl,
                            telemetry["pnl"],
                            state["lot"],
                            users,
                            strike = ce_strike if leg_name == "CE" else pe_strike
                        )
                    )
                )

                log_trade_event(
                    event_type="EXIT",
                    leg_name=str(leg_name),
                    token=token,
                    symbol=SYMBOL,
                    side="BUY",
                    lot=state["lot"],
                    price=exit_price,
                    reason="SL",
                    pnl=final_pnl,
                    cum_pnl=telemetry["pnl"]
                )
# =========================
# MAIN
# =========================

load_fno_master()

ce_state = init_state()
pe_state = init_state()
def start_strategy():

    global ce_state, pe_state, TOKENS

    mark_range()

    ce_state = init_state()
    pe_state = init_state()

    TOKENS = [CE_ID, PE_ID, INDEX_TOKEN]
    for t in TOKENS:
        subscribe(t, on_tick)

    threading.Thread(target=trade_log_worker, daemon=True).start()

    print("\n🚀 Range Breakout Paper Engine Running...\n")

def on_tick(token, msg):

    if token not in TOKENS:
        return  

    if msg:

        if str(msg["security_id"]) == INDEX_TOKEN:
            on_tick_index(msg)

        elif str(msg["security_id"]) in (CE_ID, PE_ID):
            on_option_tick(msg)




"""
while True:
    try:

        feed.run_forever()
        msg = feed.get_data()

        if msg:

            if str(msg["security_id"]) == INDEX_TOKEN:
                on_tick_index(msg)

            elif str(msg["security_id"]) in (CE_ID, PE_ID):
                on_option_tick(msg)
    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
"""
        
            
""" 
def on_tick(token, msg):

    if token not in [CE_ID , PE_ID , INDEX_TOKEN]:
        return  
            
    if msg:
        print(msg)
        if str(msg["security_id"]) == INDEX_TOKEN:       
            on_tick_index(msg)

        elif str(msg["security_id"]) in (CE_ID, PE_ID):
            on_option_tick(msg)   

for t in TOKENS:
    subscribe(t, on_tick)
 """