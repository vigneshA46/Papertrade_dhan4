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

DAY_SL = -2500
DAY_STOP = False


ATM = None 

TRADE_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/event"
EVENT_LOG_URL = "https://algoapi.dreamintraders.in/api/paperlogger/paperlogger"

COMMON_ID = "4352b6b6-abd4-459d-8d00-a30b3bc70e4e"
SYMBOL = "NIFTY"
symbol="NIFTY"

load_dotenv()


IST = pytz.timezone("Asia/Kolkata")


INDEX_TOKEN = "13"

TRADE_START = dtime(9, 31)
TRADE_END   = dtime(15, 20)

LOT_QTY = 1


LOT = 1
LOTSIZE= 65

today = datetime.now(IST).strftime("%Y-%m-%d")
#today = "2026-05-07"

# =========================
# LOGIN
# =========================
access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

builder = OneMinuteCandleBuilder()
fno_df = load_fno_master()

strategy_id = "4352b6b6-abd4-459d-8d00-a30b3bc70e4e"
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


def build_payload(name, side, token , reason, event_type, ltp, pnl, cum_pnl, lot,users , strike=ATM):

    if name == "CE":
        row = AngelCE
    else:
        row = AngelPE

    expiry_date = ce_row["SM_EXPIRY_DATE"]

    day = expiry_date.strftime("%d")
    month = expiry_date.strftime("%b").upper()
    year = expiry_date.strftime("%y")

    if name == "CE":
        symbol = f"NIFTY{day}{month}{year}{int(ce_strike)}{name}"
    else:
        symbol = f"NIFTY{day}{month}{year}{int(pe_strike)}{name}"

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
    global top_line, bottom_line, CE_ID, PE_ID, ce_strike, pe_strike,today,ce_row,pe_row,AngelCE,AngelPE,ATM

    today = datetime.now(IST).strftime("%Y-%m-%d")
    idx = dhan.intraday_minute_data(
        security_id=13,   
        exchange_segment="IDX_I",
        instrument_type="INDEX",
        from_date=f"{today}",
        to_date=f"{today}",
        interval=1
    )

    data = idx.get("data", {})

    opens = data.get("open", [])
    closes = data.get("close", [])
    timestamps = data.get("timestamp", [])

    open_915 = None
    close_930 = None

    for i in range(len(timestamps)):

        ts = datetime.fromtimestamp(timestamps[i], IST)

        # =========================
        # 09:15 OPEN
        # =========================

        if ts.hour == 9 and ts.minute == 15:

            open_915 = float(opens[i])

            print(f"📍 09:15 OPEN -> {open_915}")

        # =========================
        # 09:30 CLOSE
        # =========================

        if ts.hour == 9 and ts.minute == 30:

            close_930 = float(closes[i])

            print(f"📍 09:30 CLOSE -> {close_930}")

    # =========================
    # SAFETY CHECK
    # =========================

    if open_915 is None:

        print("❌ 09:15 candle not found")
        return

    if close_930 is None:

        print("❌ 09:30 candle not found")
        return

    
    top_line = max(open_915, close_930)
    bottom_line = min(open_915, close_930)


    print("\n📏 RANGE MARKED")
    print("TOP    :", top_line)
    print("BOTTOM :", bottom_line)

    atm = float(bottom_line)
    print("ATM", atm)

    ATM = calculate_atm(atm)
    print("📌 ATM:", ATM)

    # =========================
    # OPTION CHAIN
    # =========================

    oc = dhan.option_chain(
        under_security_id=13,
        under_exchange_segment="IDX_I",
        expiry=get_next_expiry()   # change expiry dynamically
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
    CE_ID = best_ce["security_id"]

    pe_strike = best_pe["strike"]
    PE_ID = best_pe["security_id"]

    finder = FindInstrument()

    ce_row = find_option_security(fno_df,ce_strike,"CE",today,"NIFTY")

    pe_row = find_option_security(fno_df,pe_strike,"PE",today,"NIFTY")

    AngelCE = finder.get_option("NIFTY",int(ce_strike),"CE")

    AngelPE = finder.get_option("NIFTY",int(pe_strike),"PE")

    print(f"Selected CE Strike: {ce_strike}")
    print(f"CE LTP: {best_ce['ltp']}")
    print(f"CE Security ID: {CE_ID}")

    print(f"Selected PE Strike: {pe_strike}")
    print(f"PE LTP: {best_pe['ltp']}")
    print(f"PE Security ID: {PE_ID}")
    
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








def on_tick_index(msg):

    candle = idx_builder.process_tick(msg)
    ltp = float(msg["LTP"])


    # =========================================================
    # 🔴 EXIT LOGIC (INDEX BASED)
    # =========================================================

    # PE EXIT → close below top line
    if pe_state["position"] and ltp > bottom_line:
        print("❌ PE EXIT (Index crossed below top line)")

        pe_state["force_exit"] = True
        pe_state["exit_reason"] = "INDEX_EXIT"

    # CE EXIT → close above bottom line
    if ce_state["position"] and ltp < top_line:
        print("❌ CE EXIT (Index crossed above bottom line)")

        ce_state["force_exit"] = True
        ce_state["exit_reason"] = "INDEX_EXIT"

    # =========================================================
    # 🔁 REARM LOGIC
    # =========================================================

    # PE rearm → price back below top_line
    if pe_state["rearm_required"] and ltp > bottom_line:
        print("🔁 PE REARMED")
        pe_state["rearm_required"] = False

    # CE rearm → price back above bottom_line
    if ce_state["rearm_required"] and ltp < top_line:
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
        "force_exit":False,

        "lot": 1,
        "pnl": 0.0,
        "cum_pnl": 0.0,
        "target_points":50,

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
    # 🚨 SIGNAL GENERATION
    # =========================================================

    # PE BUY SIGNAL (breakout above)
    if (
        c < bottom_line and
        avg_price < bottom_line and
        avg_price > c and

        not pe_state["position"] and
        not pe_state["trading_disabled"] and
        not pe_state["rearm_required"]
    ):
        print("🚨 PE BUY SIGNAL")

        pe_state["enter_now"] = True
        pe_state["signal_time"] = timestamp

    # CE BUY SIGNAL (breakout below)
    if (
        c > top_line and
        avg_price > top_line and
        avg_price < c and

        not ce_state["position"] and
        not ce_state["trading_disabled"] and
        not ce_state["rearm_required"]
    ):
        print("🚨 CE BUY SIGNAL")

        ce_state["enter_now"] = True
        ce_state["signal_time"] = timestamp



def on_option_tick(msg):
    global ce_state, pe_state, telemetry ,DAY_STOP

    if msg["type"] != 'Quote Data':
        return
    
    now = datetime.now(IST).time()

    token = str(msg["security_id"])
    ltp = float(msg["LTP"])


    # =========================
    # SELECT STATE
    # =========================
    if token == str(CE_ID):
        state = ce_state
        leg_name = "CE"
    elif token == str(PE_ID):
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
    if (
    state["enter_now"]
    and not state["position"]
    and not DAY_STOP
    ):

        state["entry_price"] = ltp
        state["position"] = True
        state["enter_now"] = False


        state["sl"] = ltp + 25   # loss side (SELL)

        print(f"✅ {leg_name} ENTRY @ {ltp}")


        #log_event(leg_name, token, "ENTRY", ltp, "Breakout Entry")
        print(f"{leg_name}, {token}, {SYMBOL}, {state['lot']}, {ltp},{telemetry['pnl']}")

        deployments = get_today_deployments()
        users = group_users_by_broker(deployments)

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
                    strike=str(ce_strike) if leg_name == "CE" else str(pe_strike)
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


        current_mtm = telemetry["pnl"] * LOTSIZE        

        state["entry_price"] = float(state["entry_price"])
        state["sl"] = float(state["sl"])


        if now >= TRADE_END:

            telemetry["status"] = "CLOSED"

            print(f"⏰ {leg_name} TIME EXIT @ {ltp}")

            exit_price = ltp
            final_pnl = exit_price - state["entry_price"]

            state["pnl"] = final_pnl
            state["cum_pnl"] += final_pnl
            telemetry["pnl"] += final_pnl

            state["position"] = False
            state["enter_now"] = False
            state["rearm_required"] = False
            state["trading_disabled"] = True
            state["force_exit"] = False
            
            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

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
                        strike=str(ce_strike) if leg_name == "CE" else str(pe_strike)
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
            final_pnl = exit_price - state["entry_price"]

            state["cum_pnl"] += final_pnl

            telemetry["pnl"] += final_pnl

            state["position"] = False
            state["rearm_required"] = True
            state["force_exit"] = False

            #log_event(leg_name, token, "EXIT", ltp, "INDEX EXIT")
            
            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

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
                        strike=str(ce_strike) if leg_name == "CE" else str(pe_strike)
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

              
            state["lot"]+=1
            
        entry = state["entry_price"]

        # SELL PnL
        pnl = ltp - entry
        state["pnl"] = pnl

        # update telemetry
        if leg_name == "CE":
            telemetry["ce_pnl"] = pnl
        else:
            telemetry["pe_pnl"] = pnl
      
        current_side_mtm=state["cum_pnl"]+pnl

        if current_side_mtm >= state["target_points"]:

            print(f"{leg_name} POINTS TARGET HIT")

            exit_price = ltp

            final_pnl = exit_price - state["entry_price"]

            state["cum_pnl"] += final_pnl

            telemetry["pnl"] += final_pnl

            state["position"] = False

            state["rearm_required"] = True
   
            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            run_async(
                emit_signal(
                    build_payload(
                        leg_name,
                        "BUY",
                        token,
                        "points exit",
                        "EXIT",
                        ltp,
                        final_pnl,
                        telemetry["pnl"],
                        state["lot"],
                        users,  
                        strike=str(ce_strike) if leg_name == "CE" else str(pe_strike)
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
                reason="POINTS EXIT",
                pnl=float(final_pnl),
                cum_pnl=telemetry["pnl"]
            )

            state["lot"] = 1
            state["target_points"] += 50

            # =========================
            # ❌ SL HIT
            # =========================
        #if ltp <= state["sl"]:
        #    print(f"❌ {leg_name} SL HIT @ {ltp}")

            #exit_price = ltp
            #final_pnl = exit_price - state["entry_price"]
            #state["cum_pnl"] += final_pnl

            #telemetry["pnl"] += final_pnl

            #state["position"] = False
            #state["rearm_required"] = True

            #log_trade_event(
                    #event_type="EXIT",
                    #leg_name=str(leg_name),
                    #token=token,
                    #symbol=SYMBOL,
                    #side="BUY",
                    #lot=state["lot"],
                    #price=exit_price,
                    #reason="SL",
                    #pnl=final_pnl,
                    #cum_pnl=telemetry["pnl"]
                #)
            #state["lot"] += 1
            
# =========================
# MAIN
# =========================

wait_for_start()

load_fno_master()

ce_state = init_state()
pe_state = init_state()

mark_range()


#TOKENS = [CE_ID, PE_ID, INDEX_TOKEN]

threading.Thread(target=trade_log_worker, daemon=True).start()

print("\n🚀 Range Breakout Paper Engine Running(CUMULATIVE)...\n")

instruments = [
    (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Quote),
    (MarketFeed.IDX, str(INDEX_TOKEN), MarketFeed.Quote),

]

"""
feed = MarketFeed(dhan_context, instruments, "v2")


while True:
    try:

        feed.run_forever()
        msg = feed.get_data()

        if msg:

            if str(msg["security_id"]) == INDEX_TOKEN:
                on_tick_index(msg)

            elif str(msg["security_id"]) in (str(CE_ID), str(PE_ID)):
                
                on_option_tick(msg)
    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()

        
            
""" 
TOKENS = [str(CE_ID), str(PE_ID),str(INDEX_TOKEN)]

def on_tick(token, msg):
     
    token = str(token)
    
    if token not in TOKENS:
        return  

    if msg:

        if str(msg["security_id"]) == str(INDEX_TOKEN):
            on_tick_index(msg)

        elif str(msg["security_id"]) in (str(CE_ID), str(PE_ID)):
            on_option_tick(msg)

for t in TOKENS:
    subscribe(t, on_tick)