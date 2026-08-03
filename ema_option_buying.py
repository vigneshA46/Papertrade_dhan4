import time
import pytz
import requests
import pandas as pd
from datetime import datetime, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import dhanhq, DhanContext
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder
from dispatcher import subscribe
from find_security import load_fno_master, find_option_security
from queue import Queue
import threading
from vwap_engine import VWAPManager
from ema5_indicator import EMA5

vwap_manager = VWAPManager()
ema5 = EMA5()

COMMON_ID = "d302ce81-0247-405e-ba5c-60cebe7987bc"

TRADE_START = dtime(9, 19)
TRADE_END   = dtime(15, 20)


cross_happened = False
current_ema = None
current_vwap = None

strategy_activated = False
cross_detected = False

last_relation = None

load_dotenv()

IST = pytz.timezone("Asia/Kolkata")
today = datetime.now(IST).strftime("%Y-%m-%d")

access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)



idx_builder = OneMinuteCandleBuilder()

fno_df = load_fno_master()

INDEX_TOKEN = "13"
CE_ID = None
PE_ID = None
LOTSIZE = 65

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



def on_tick_index(msg):

    global current_vwap

    # =========================
    # VWAP UPDATE (TICK BASED)
    # =========================

    _, current_vwap = vwap_manager.on_tick(msg)

    # =========================
    # BUILD 1-MIN CANDLE
    # =========================

    candle = idx_builder.process_tick(msg)
    print(msg)
    print("vwap" , current_vwap)


    if candle:
        print(candle)
        on_index_candle(
            msg["security_id"],
            datetime.now(IST),
            candle
        )


def init_state():
    return {

        "position": False,
        "enter_now": False,

        "entry_price": None,
        "entry_time": None,

        "sl": None,
        "tsl": None,

        "tsl_active": False,

        "force_exit": False,
        "exit_reason": None,

        "rearm_required": False,

        "last_exit_reason": None,

        "lot": 1,
        "pnl": 0.0
    }



def find_ce_pe_strikes():
    # =========================
    # OPTION CHAIN
    # =========================

    global CE_ID , PE_ID

    oc = dhan.option_chain(
        under_security_id=13,
        under_exchange_segment="IDX_I",
        expiry=get_next_expiry()   # change expiry dynamically
    )


    option_data = oc["data"]["data"]["oc"]

    target = 250

    best_ce = None
    best_pe = None

    best_ce_ltp = float("inf")
    best_pe_ltp = float("inf")

    for strike, strike_data in option_data.items():

        strike = float(strike)

        # ================= CE =================
        if "ce" in strike_data:

            ce_ltp = strike_data["ce"]["last_price"]

            # only premiums ABOVE target
            if ce_ltp >= target and ce_ltp < best_ce_ltp:

                best_ce_ltp = ce_ltp

                best_ce = {
                    "strike": strike,
                    "ltp": ce_ltp,
                    "security_id": strike_data["ce"]["security_id"]
                }

        # ================= PE =================
        if "pe" in strike_data:

            pe_ltp = strike_data["pe"]["last_price"]

            # only premiums ABOVE target
            if pe_ltp >= target and pe_ltp < best_pe_ltp:

                best_pe_ltp = pe_ltp

                best_pe = {
                    "strike": strike,
                    "ltp": pe_ltp,
                    "security_id": strike_data["pe"]["security_id"]
                }

    # FINAL VALUES

    ce_strike = best_ce["strike"]
    CE_ID = str(best_ce["security_id"])

    pe_strike = best_pe["strike"]
    PE_ID = str(best_pe["security_id"])

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


def on_index_candle(token, timestamp, candle):

    global current_ema
    global current_vwap
    global strategy_activated
    global cross_detected
    global last_relation

    global ce_state
    global pe_state

    now = timestamp.time()


    if now < TRADE_START or now > TRADE_END:
        print("out of time")
        return

    # =========================
    # EMA UPDATE
    # =========================



    current_ema = ema5.update(candle)

    if current_ema is None:
        print("no EMA")
        return

    if current_vwap is None:
        print("no vwap")
        return

    close = candle["close"]

    print(
        f"\n🕯 {timestamp}"
        f" CLOSE:{close}"
        f" EMA:{current_ema}"
        f" VWAP:{round(current_vwap,2)}"
    )

    # =====================================================
    # EMA ↔ VWAP RELATION
    # =====================================================

    current_relation = (
        "ABOVE"
        if current_ema > current_vwap
        else "BELOW"
    )

    # =====================================================
    # CROSS DETECTION
    # =====================================================

    if last_relation and current_relation != last_relation:

        print("🔄 EMA VWAP CROSSOVER DETECTED")

        cross_detected = True

        strategy_activated = True

        # Rearm both sides
        ce_state["rearm_required"] = False
        pe_state["rearm_required"] = False

    last_relation = current_relation

    # =====================================================
    # 09:45 ACTIVATION
    # =====================================================

    if not strategy_activated and now >= dtime(9, 45):

        print("⏰ 09:45 Activation Enabled")

        strategy_activated = True

    if not strategy_activated:
        return

    # =====================================================
    # INDEX EXIT CONDITIONS
    # =====================================================

    # CE EXIT
    if ce_state["position"]:

        if close <= current_vwap or current_ema <= current_vwap:

            print("❌ CE INDEX EXIT")

            ce_state["force_exit"] = True
            ce_state["exit_reason"] = "INDEX_EXIT"

    # PE EXIT
    if pe_state["position"]:

        if close >= current_vwap or current_ema >= current_vwap:

            print("❌ PE INDEX EXIT")

            pe_state["force_exit"] = True
            pe_state["exit_reason"] = "INDEX_EXIT"

    # =====================================================
    # REARM CONDITIONS
    # =====================================================

    if ce_state["rearm_required"]:

        if (
            abs(close - current_vwap) <= 5
            or current_relation != last_relation
        ):

            print("🔁 CE REARMED")

            ce_state["rearm_required"] = False

    if pe_state["rearm_required"]:

        if (
            abs(close - current_vwap) <= 5
            or current_relation != last_relation
        ):

            print("🔁 PE REARMED")

            pe_state["rearm_required"] = False

    # =====================================================
    # ENTRY CONDITIONS
    # =====================================================

    # ================= CE =================

    if (

        close > current_vwap
        and current_ema > current_vwap

        and not ce_state["position"]
        and not ce_state["rearm_required"]

    ):

        print("🚀 CE ENTRY SIGNAL")

        ce_state["enter_now"] = True

    # ================= PE =================

    if (

        close < current_vwap
        and current_ema < current_vwap

        and not pe_state["position"]
        and not pe_state["rearm_required"]

    ):

        print("🚀 PE ENTRY SIGNAL")

        pe_state["enter_now"] = True



def on_option_tick(msg):

    global ce_state
    global pe_state
    global telemetry

    if msg["type"] != "Quote Data":
        return


    now = datetime.now(IST).time()

    token = str(msg["security_id"])
    ltp = float(msg["LTP"])

    # =====================================================
    # SELECT LEG
    # =====================================================

    if token == str(CE_ID):

        state = ce_state
        leg_name = "CE"

    elif token == str(PE_ID):

        state = pe_state
        leg_name = "PE"

    else:
        return

    # =====================================================
    # TELEMETRY UPDATE
    # =====================================================

    if leg_name == "CE":
        telemetry["ce_ltp"] = ltp
    else:
        telemetry["pe_ltp"] = ltp

    # =====================================================
    # MTM CHECK
    # =====================================================

    total_pnl = telemetry["pnl"] * LOTSIZE

    if total_pnl >= 5000:

        print("🎯 DAY TARGET HIT")

        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True

        if ce_state["position"]:
            ce_state["force_exit"] = True
            ce_state["exit_reason"] = "DAY_TARGET"

        if pe_state["position"]:
            pe_state["force_exit"] = True
            pe_state["exit_reason"] = "DAY_TARGET"

    if total_pnl <= -3000:

        print("🛑 DAY STOPLOSS HIT")

        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True

        if ce_state["position"]:
            ce_state["force_exit"] = True
            ce_state["exit_reason"] = "DAY_STOPLOSS"

        if pe_state["position"]:
            pe_state["force_exit"] = True
            pe_state["exit_reason"] = "DAY_STOPLOSS"

    # =====================================================
    # ENTRY EXECUTION
    # =====================================================

    if (

        state["enter_now"]
        and not state["position"]
        and not state.get("trading_disabled", False)

    ):

        state["position"] = True

        state["enter_now"] = False

        state["entry_price"] = ltp
        state["entry_time"] = datetime.now(IST)

        # ============================================
        # BUYING STRATEGY SL / TSL
        # ============================================

        state["sl"] = ltp - 25

        # trigger level
        state["tsl"] = ltp + 35

        state["tsl_active"] = False

        print(f"✅ {leg_name} ENTRY @ {ltp}")

        log_trade_event(
            event_type="ENTRY",
            leg_name=leg_name,
            token=int(token),
            symbol=SYMBOL,
            side="BUY",
            lot=state["lot"],
            price=ltp,
            reason="EMA_VWAP_ENTRY",
            pnl=0,
            cum_pnl=telemetry["pnl"]
        )

    # =====================================================
    # POSITION MANAGEMENT
    # =====================================================

    if not state["position"]:
        return

    entry = state["entry_price"]

    # =====================================================
    # LIVE PNL
    # =====================================================

    pnl = ltp - entry

    state["pnl"] = pnl

    if leg_name == "CE":
        telemetry["ce_pnl"] = pnl
    else:
        telemetry["pe_pnl"] = pnl

    # =====================================================
    # TIME EXIT
    # =====================================================

    if now >= TRADE_END:

        print(f"⏰ {leg_name} TIME EXIT @ {ltp}")

        final_pnl = ltp - entry

        telemetry["pnl"] += final_pnl

        state["position"] = False
        state["enter_now"] = False
        state["force_exit"] = False
        state["tsl_active"] = False
        state["trading_disabled"] = True

        log_trade_event(
            event_type="EXIT",
            leg_name=leg_name,
            token=int(token),
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=ltp,
            reason="TIME_EXIT",
            pnl=final_pnl,
            cum_pnl=telemetry["pnl"]
        )

        return

    # =====================================================
    # FORCE EXIT
    # =====================================================

    if state["force_exit"]:

        print(f"⚡ {leg_name} FORCE EXIT @ {ltp}")

        final_pnl = ltp - entry

        telemetry["pnl"] += final_pnl

        state["position"] = False

        state["force_exit"] = False

        state["rearm_required"] = True

        state["tsl_active"] = False

        log_trade_event(
            event_type="EXIT",
            leg_name=leg_name,
            token=int(token),
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=ltp,
            reason=state["exit_reason"],
            pnl=final_pnl,
            cum_pnl=telemetry["pnl"]
        )

        return

    # =====================================================
    # TSL ACTIVATION
    # =====================================================

    if (

        not state["tsl_active"]
        and ltp >= state["tsl"]

    ):

        state["tsl_active"] = True

        # lock profit
        state["sl"] = entry + 10

        print(
            f"🔥 {leg_name} TSL ACTIVATED "
            f"SL:{state['sl']}"
        )

    # =====================================================
    # TRAILING LOGIC
    # =====================================================

    if state["tsl_active"]:

        # keep trailing every +10 move

        while ltp >= state["tsl"] + 10:

            state["tsl"] += 10
            state["sl"] += 10

            print(
                f"🔁 {leg_name} TRAILED "
                f"TSL:{state['tsl']} "
                f"SL:{state['sl']}"
            )

    # =====================================================
    # STOPLOSS CHECK
    # =====================================================

    if ltp <= state["sl"]:

        print(f"❌ {leg_name} SL EXIT @ {ltp}")

        final_pnl = ltp - entry

        telemetry["pnl"] += final_pnl

        state["position"] = False

        state["rearm_required"] = True

        state["tsl_active"] = False

        log_trade_event(
            event_type="EXIT",
            leg_name=leg_name,
            token=int(token),
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=ltp,
            reason="SL_EXIT",
            pnl=final_pnl,
            cum_pnl=telemetry["pnl"]
        )


threading.Thread(target=trade_log_worker, daemon=True).start()

load_fno_master()


find_ce_pe_strikes()

ce_state = init_state()
pe_state = init_state()





instruments = [
    (MarketFeed.NSE_FNO, str(CE_ID), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(PE_ID), MarketFeed.Quote),
    (MarketFeed.IDX, str(INDEX_TOKEN), MarketFeed.Quote)
]

print("instruments" , instruments)

feed = MarketFeed(dhan_context, instruments, "v2")


while True:
    try:

        feed.run_forever()
        msg = feed.get_data()

        if msg:

            if str(msg["security_id"]) == INDEX_TOKEN:
                on_tick_index(msg)

            elif str(msg["security_id"]) in (str(CE_ID),str(PE_ID)):
                on_option_tick(msg)
    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
