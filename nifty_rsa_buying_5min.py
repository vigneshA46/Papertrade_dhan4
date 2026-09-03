import time
import pytz
import requests
from datetime import datetime,date, time as dtime
from dotenv import load_dotenv
import os
from dhanhq import MarketFeed
from dhanhq import DhanContext, dhanhq
from datetime import timedelta
from dhan_token import get_access_token
from candle_builder import FiveMinuteCandleBuilder , OneMinuteCandleBuilder
from find_security import load_fno_master, find_option_security
import threading
from dispatcher import subscribe
from queue import Queue
from signal_emitter import emit_signal
#from tests.test_order import get_today_deployments, group_users_by_broker
import asyncio
from find_instrument import FindInstrument
import pandas as pd


NSE_HOLIDAYS = {
    date(2026, 1, 15),
    date(2026, 1, 26),
    date(2026, 3, 3),
    date(2026, 3, 26),
    date(2026, 3, 31),
    date(2026, 4, 3),
    date(2026, 4, 14),
    date(2026, 5, 1),
    date(2026, 5, 28),
    date(2026, 6, 26),
    date(2026, 9, 14),
    date(2026, 10, 2),
    date(2026, 10, 20),
    date(2026, 11, 10),
    date(2026, 11, 24),
    date(2026, 12, 25),
}


# ==========================================================
# TRADE LIMIT
# ==========================================================

MAX_TRADES_PER_DAY = 2
trades_today = 0

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

COMMON_ID = "3a498465-c022-4bca-9315-68ae18296208"
SYMBOL = "NIFTY"

CE_ID = None
PE_ID = None

combined_pnl = 0
load_dotenv()

CE_ID = None
PE_ID = None

STRATEGY_NAME = "NIFTY_OPTION_BUYING_50 no reentry"
MARKET_OPEN = dtime(9, 15)
MARKET_CLOSE = dtime(15, 14)

CE_TARGET_POINTS = 50
PE_TARGET_POINTS = 50

IST = pytz.timezone("Asia/Kolkata")

TRADE_START = dtime(9, 15)
TRADE_END   = dtime(15, 14)

TARGET_POINTS = 50
LOTSIZE = 65
OPTION_SELECTION_LTP = 200

strategy_id = "3a498465-c022-4bca-9315-68ae18296208"

today = datetime.now(IST).strftime("%Y-%m-%d")

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

# =========================
# LOGIN
# =========================

access_token = get_access_token()
CLIENT_ID = os.getenv("CLIENT_ID")
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


def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users, strike):

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
        "strike": strike,
        "price":ltp,
        "pnl":pnl,
        "cum_pnl":cum_pnl,
        "zebusymbol": "NIFTY",
        "is_ce": True if name == "CE" else False,
        "is_fno": True,
        "antsymbol": "NIFTY",
        "reason":reason
    }


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


def select_option_contracts(oc, max_ltp=OPTION_SELECTION_LTP):
    """
    Selects the CE and PE contracts whose LTP is
    closest to max_ltp without exceeding it.

    Returns:
        ce_strike, ce_security_id, pe_strike, pe_security_id
    """

    ce_candidate = None
    pe_candidate = None

    option_chain = oc["data"]["data"]["oc"]

    for strike, contracts in option_chain.items():

        strike = int(float(strike))

        # ---------------- CE ----------------
        ce = contracts.get("ce", {})
        ce_ltp = ce.get("last_price", 0)
        ce_sid = ce.get("security_id", 0)

        if (
            ce_sid != 0
            and ce_ltp > 0
            and ce_ltp <= max_ltp
        ):
            if ce_candidate is None or ce_ltp > ce_candidate["ltp"]:
                ce_candidate = {
                    "strike": strike,
                    "security_id": ce_sid,
                    "ltp": ce_ltp,
                }

        # ---------------- PE ----------------
        pe = contracts.get("pe", {})
        pe_ltp = pe.get("last_price", 0)
        pe_sid = pe.get("security_id", 0)

        if (
            pe_sid != 0
            and pe_ltp > 0
            and pe_ltp <= max_ltp
        ):
            if pe_candidate is None or pe_ltp > pe_candidate["ltp"]:
                pe_candidate = {
                    "strike": strike,
                    "security_id": pe_sid,
                    "ltp": pe_ltp,
                }

    if ce_candidate is None:
        raise Exception("No valid CE contract found.")

    if pe_candidate is None:
        raise Exception("No valid PE contract found.")

    return (
        ce_candidate["strike"],
        ce_candidate["security_id"],
        pe_candidate["strike"],
        pe_candidate["security_id"],
    )


def load_history(security_id, candle_count=200):

    print("Load history called")

    start_time, end_time = get_market_history_window(
        candle_count=candle_count,
        interval=5
    )

    #print("\n========== HISTORY WINDOW ==========")
    #print("From :", start_time)
    #print("To   :", end_time)
    #print("====================================\n")

    data = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start_time.strftime("%Y-%m-%d %H:%M:%S"),
        to_date=end_time.strftime("%Y-%m-%d %H:%M:%S"),
        interval=5
    )

    raw = data.get("data", {})

    opens = raw.get("open", [])
    highs = raw.get("high", [])
    lows = raw.get("low", [])
    closes = raw.get("close", [])
    volumes = raw.get("volume", [])
    timestamps = raw.get("timestamp", [])

    candles = []

    for i in range(len(timestamps)):

        ts = datetime.fromtimestamp(timestamps[i], IST)

        candles.append({
            "timestamp": timestamps[i],
            "datetime": ts,
            "open": float(opens[i]),
            "high": float(highs[i]),
            "low": float(lows[i]),
            "close": float(closes[i]),
            "volume": float(volumes[i])
        })

    #print(f"Loaded {len(candles)} historical candles")

    return candles[-candle_count-1:-1]


def update_ema(state, candle):

    multiplier = 2 / (9 + 1)

    previous_ema = state["ema9"]

    close = candle["close"]

    new_ema = (
        (close - previous_ema) * multiplier
    ) + previous_ema

    state["ema9"] = new_ema

    state["candles"].append(candle)

    if len(state["candles"]) > 200:
        state["candles"].pop(0)

    return new_ema


def calculate_ema(closes, period=9):

    if len(closes) < period:
        return None

    multiplier = 2 / (period + 1)

    ema = sum(closes[:period]) / period

    for close in closes[period:]:
        ema = ((close - ema) * multiplier) + ema

    return ema


def calculate_rsi(closes, period=14):
    """
    Calculates RSI-14 using Wilder's smoothing method.

    Returns:
        rsi, avg_gain, avg_loss
    """

    if len(closes) < period + 1:
        return None, None, None

    gains = []
    losses = []

    # ---------------------------------
    # Calculate gains and losses
    # ---------------------------------
    for i in range(1, len(closes)):

        change = closes[i] - closes[i - 1]

        if change > 0:
            gains.append(change)
            losses.append(0.0)

        else:
            gains.append(0.0)
            losses.append(abs(change))

    # ---------------------------------
    # Initial Wilder average
    # First 14 changes
    # ---------------------------------
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    # ---------------------------------
    # Wilder smoothing
    # Remaining changes
    # ---------------------------------
    for i in range(period, len(gains)):

        avg_gain = (
            (avg_gain * (period - 1)) + gains[i]
        ) / period

        avg_loss = (
            (avg_loss * (period - 1)) + losses[i]
        ) / period

    # ---------------------------------
    # Calculate final RSI
    # ---------------------------------
    if avg_loss == 0:
        rsi = 100.0

    else:
        rs = avg_gain / avg_loss

        rsi = 100.0 - (
            100.0 / (1.0 + rs)
        )

    print("RSI from local function ", rsi)
    return rsi, avg_gain, avg_loss


def calculate_live_rsi(state, current_ltp, period=14):

    if state["avg_gain"] is None or state["avg_loss"] is None:
        return None

    previous_close = state["candles"][-1]["close"]

    change = current_ltp - previous_close

    gain = max(change, 0)
    loss = max(-change, 0)

    live_avg_gain = (
        (state["avg_gain"] * (period - 1)) + gain
    ) / period

    live_avg_loss = (
        (state["avg_loss"] * (period - 1)) + loss
    ) / period

    if live_avg_loss == 0:
        live_rsi = 100.0
    else:
        rs = live_avg_gain / live_avg_loss
        live_rsi = 100.0 - (
            100.0 / (1.0 + rs)
        )

    state["live_rsi14"] = live_rsi

    return live_rsi


def update_rsi(state, candle, period=14):

    if state["avg_gain"] is None or state["avg_loss"] is None:
        return None

    previous_close = state["candles"][-1]["close"]
    current_close = candle["close"]

    change = current_close - previous_close

    if change > 0:
        gain = change
        loss = 0.0
    else:
        gain = 0.0
        loss = abs(change)

    avg_gain = (
        (state["avg_gain"] * (period - 1)) + gain
    ) / period

    avg_loss = (
        (state["avg_loss"] * (period - 1)) + loss
    ) / period

    state["avg_gain"] = avg_gain
    state["avg_loss"] = avg_loss

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1.0 + rs))

    state["rsi14"] = rsi

    return rsi


def is_market_holiday(check_date):
    """
    Returns True if the given date is
    a weekend or NSE holiday.
    """

    if isinstance(check_date, datetime):
        check_date = check_date.date()

    # Saturday = 5, Sunday = 6
    if check_date.weekday() >= 5:
        return True

    return check_date in NSE_HOLIDAYS


def get_previous_trading_day(current_date):
    """
    Returns the previous market trading day.
    """

    if isinstance(current_date, datetime):
        current_date = current_date.date()

    current_date -= timedelta(days=1)

    while is_market_holiday(current_date):
        current_date -= timedelta(days=1)

    return current_date


def count_market_minutes_back(end_time, minutes):
    """
    Walk backwards through MARKET trading minutes only.
    Skips weekends, NSE holidays and non-market hours.
    """

    current = end_time
    remaining = minutes

    while remaining > 0:

        market_open = current.replace(
            hour=9,
            minute=15,
            second=0,
            microsecond=0
        )

        available = int(
            (current - market_open).total_seconds() / 60
        )

        if available >= remaining:
            return current - timedelta(minutes=remaining)

        remaining -= available

        prev_day = get_previous_trading_day(current)

        current = IST.localize(
            datetime.combine(prev_day, MARKET_CLOSE)
        )

    return current


def get_last_market_time():
    """
    Returns the latest valid market timestamp.

    Handles:
    - Before market open
    - During market
    - After market
    - Weekends
    - NSE holidays
    """

    now = datetime.now(IST)

    # Holiday / Weekend
    if is_market_holiday(now):

        prev_day = get_previous_trading_day(now)

        return IST.localize(
            datetime.combine(prev_day, MARKET_CLOSE)
        )

    market_open = now.replace(
        hour=9,
        minute=15,
        second=0,
        microsecond=0
    )

    market_close = now.replace(
        hour=15,
        minute=30,
        second=0,
        microsecond=0
    )

    # Before market opens
    if now < market_open:

        prev_day = get_previous_trading_day(now)

        return IST.localize(
            datetime.combine(prev_day, MARKET_CLOSE)
        )

    # During market
    if market_open <= now <= market_close:
        return now.replace(second=0, microsecond=0)

    # After market closes
    return market_close


def get_market_history_window(candle_count=200, interval=5):
    """
    Returns the history window required
    to fetch the last completed market candles.
    """

    end_time = get_last_market_time()

    required_minutes = candle_count * interval

    start_time = count_market_minutes_back(
        end_time,
        required_minutes
    )

    print("Start time ", start_time)
    print("end time" , end_time)

    return start_time, end_time


def get_previous_day_ohlc(security_id):
    """
    Fetches previous trading day's OHLC from 5-minute candles.
    This is much more reliable than requesting a single day's window.
    """

    today = datetime.now(IST).date()
    previous_day = get_previous_trading_day(today)

    # Fetch last 3 calendar days
    from_date = previous_day - timedelta(days=2)

    start = datetime.combine(from_date, MARKET_OPEN)
    end = datetime.combine(today, MARKET_CLOSE)

    print("\n========== FETCHING PREVIOUS DAY DATA ==========")
    print("From :", start)
    print("To   :", end)
    print("===============================================\n")

    data = dhan.intraday_minute_data(
        security_id=str(security_id),
        exchange_segment="NSE_FNO",
        instrument_type="OPTIDX",
        from_date=start.strftime("%Y-%m-%d %H:%M:%S"),
        to_date=end.strftime("%Y-%m-%d %H:%M:%S"),
        interval=5
    )

    if data.get("status") != "success":
        print(data)
        return None

    raw = data["data"]

    highs = raw["high"]
    lows = raw["low"]
    closes = raw["close"]
    timestamps = raw["timestamp"]

    previous_day_high = []
    previous_day_low = []
    previous_day_close = []

    for i in range(len(timestamps)):

        candle_time = datetime.fromtimestamp(
            timestamps[i],
            IST
        )

        if candle_time.date() == previous_day:

            previous_day_high.append(float(highs[i]))
            previous_day_low.append(float(lows[i]))
            previous_day_close.append(float(closes[i]))

    if len(previous_day_close) == 0:

        print("No previous day candles found.")
        return None

    ohlc = {

        "high": max(previous_day_high),

        "low": min(previous_day_low),

        "close": previous_day_close[-1]

    }

    return ohlc

def detect_ema_bullish_crossover(state):

    leg = "CE" if state == ce_state else "PE"

    if (
        state["previous_ema9"] is None or
        state["previous_ema21"] is None
    ):
        return False

    if not state["crossover_happened"]:

        bullish_cross = (
            state["previous_ema9"] <= state["previous_ema21"]
            and
            state["ema9"] > state["ema21"]
        )

        if bullish_cross:

            state["crossover_happened"] = True

            print("🟢 BULLISH EMA CROSSOVER DETECTED" , leg)

            return True

    return False


def detect_ema_bearish_crossover(state):

    leg = "CE" if state == ce_state else "PE"

    if (
        state["previous_ema9"] is None or
        state["previous_ema21"] is None
    ):
        return False

    # --------------------------------------------------
    # Only check bearish crossover when a bullish
    # crossover is currently active.
    # --------------------------------------------------
    if state["crossover_happened"]:

        bearish_cross = (
            state["previous_ema9"] >= state["previous_ema21"]
            and
            state["ema9"] < state["ema21"]
        )

        if bearish_cross:

            state["crossover_happened"] = False
            state["signal_candle"] = None
            state["waiting_for_breakout"] = False

            print("🔴 BEARISH EMA CROSSOVER DETECTED" , leg)

            return True

    return False


def init_state():
    return {
        "marked": None,
        "position": False,
        "trading_disabled": False,

        "entry_price": None,
        "entry_time": None,

        "stoploss": None,
        "highest_price": None,

        "lot": 1,
        "pnl": 0.0,
        "symbol": None,

        "rearm_required": False,
        "moment": 0.0,

        # EMA
        "candles": [],
        "ema9": None,
        "ema21": None,
        "pivot": None,
        
        "r1": None,
        "r2": None,
        "r3": None,
        "s1": None,
        "s2": None,
        "s3": None,

        # Strategy State
        "signal_state": "IDLE",      # IDLE -> WAITING_RETEST -> IN_POSITION
        "signal_candle": None,       # Candle which closed above EMA
        "target": None,              # Fibonacci target
        "stoploss": None,   
        "waiting_retest": False,
        "waiting_for_breakout" : False,
        "trend": None,
        "last_ltp": None,

        "rsi14": None,
        "live_rsi14": None,
        "avg_gain": None,
        "avg_loss": None,

        "previous_ema9": None,
        "previous_ema21": None,

        "crossover_happened" : False,

        "leg_name": None,
        "token": None,
        "strike": None,
    }


def handle_leg(state, candle):

    # Check EMA crossover
    if not state["crossover_happened"]:
        return

    # Check RSI confirmation
    if state["rsi14"] <= 50:
        return

    if state["signal_candle"] is None:

        # Save signal candle
        state["signal_candle"] = {
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "time": candle["timestamp"]
        }

        # Wait for breakout
        state["waiting_for_breakout"] = True

        print("✅ Signal Candle Created")
        print("Signal candle high:", state["signal_candle"]["high"])


def manage_positions(state, ltp):
    
    """
    Handles:
    1. Entry
    2. Target Exit
    3. Stop Loss Exit
    """
    global combined_pnl , trades_today , MAX_TRADES_PER_DAY
    now = datetime.now(IST).time()

    name = "CE" if state == ce_state else "PE"
    token = CE_ID if state == ce_state else PE_ID

    # =========================
    # TIME EXIT (15:20)
    # =========================
    if now >= TRADE_END:
        telemetry["status"] = 'CLOSED'

        if state["position"]:
            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * 65

            state["pnl"] += pnl
            combined_pnl += pnl

            deployments = get_today_deployments()

            users = group_users_by_broker(deployments)

            print("FORMATTED USERS:", users)


            run_async(emit_signal(build_payload(name, "SELL", token , "exit","EXIT", ltp, pnl,combined_pnl,state["lot"],users , state["strike"])))
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

    if state["trading_disabled"]:
        return

    # ==========================
    # ENTRY
    # ==========================
    if (
        not state["position"]
        and state["crossover_happened"]
        and state["rsi14"] >50
        and state["waiting_for_breakout"]
        and ltp >= state["signal_candle"]["high"] + 2
        and trades_today < MAX_TRADES_PER_DAY
        ):

        entry_price = ltp

        # Store entry details
        state["position"] = True
        state["entry_price"] = entry_price

        state["stoploss"] = entry_price - 20
        state["highest_price"] = entry_price

        trades_today += 1

        print(
            f"ENTRY COUNT: {trades_today}/{MAX_TRADES_PER_DAY}"
        )

        # Reset signal
        state["waiting_for_breakout"] = False
        state["crossover_happened"] = False
        state["signal_candle"] = None

        print(f"{state['leg_name']} BUY @ {entry_price}")
        
        deployments = get_today_deployments()
        users = group_users_by_broker(deployments)



        # ==========================
        # ENTRY TELEMETRY / SIGNAL
        # ==========================
        run_async(
            emit_signal(
                build_payload(
                    state["leg_name"],
                    "BUY",
                    state["token"],
                    "entry",
                    "ENTRY",
                    entry_price,
                    state["pnl"],
                    combined_pnl,
                    state["lot"],
                    users,
                    state["strike"]
                )
            )
        )

        # ==========================
        # ENTRY TRADE LOG
        # ==========================
        log_trade_event(
            event_type="ENTRY",
            leg_name=state["leg_name"],
            token=state["token"],
            symbol=SYMBOL,
            side="BUY",
            lot=state["lot"],
            price=entry_price,
            reason="EMA CROSSOVER + RSI > 50 + BREAKOUT",
            pnl=state["pnl"],
            cum_pnl=combined_pnl
        )

        return

    # ==========================
    # PROFIT TRAILING
    # ==========================

    if state["position"]:

        profit_points = ltp - state["entry_price"]

        # Start trailing once profit reaches +17
        if profit_points >= 17:

            # Update highest price reached
            if ltp > state["highest_price"]:
                state["highest_price"] = ltp

            # Keep SL 2 points below highest price
            new_stoploss = state["highest_price"] - 2

            # SL should only move UP, never down
            if new_stoploss > state["stoploss"]:
                state["stoploss"] = new_stoploss

                print(
                    f"{state['leg_name']} TRAILING SL MOVED TO "
                    f"{state['stoploss']}"
                )



    # ==========================
    # TARGET EXIT
    # ==========================
    if (
        state["position"]
        and ltp <= state["stoploss"]

    ):

        exit_price = ltp

        pnl = (
            (exit_price - state["entry_price"])
            * 65
            * state["lot"]
        )

        state["pnl"] += pnl

        
        combined_pnl += pnl

        print(
            f"{state['leg_name']} TARGET HIT @ {exit_price} "
            f"PNL: {pnl}"
        )

        deployments = get_today_deployments()
        users = group_users_by_broker(deployments)

        # ==========================
        # EXIT TELEMETRY / SIGNAL
        # ==========================
        run_async(
            emit_signal(
                build_payload(
                    state["leg_name"],
                    "SELL",
                    state["token"],
                    "exit",
                    "EXIT",
                    exit_price,
                    state["pnl"],
                    combined_pnl,
                    state["lot"],
                    users,
                    state["strike"]
                )
            )
        )

        # ==========================
        # EXIT TRADE LOG
        # ==========================
        log_trade_event(
            event_type="EXIT",
            leg_name=state["leg_name"],
            token=state["token"],
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=exit_price,
            reason="TARGET HIT",
            pnl=state["pnl"],
            cum_pnl=combined_pnl
        )

        # Reset position
        state["position"] = False
        state["entry_price"] = None

        state["stoploss"] = None
        state["highest_price"] = None

        return


    # ==========================
    # STOP LOSS EXIT
    # ==========================
    if (
        state["position"]
        and ltp <= state["entry_price"] - 20
    ):

        exit_price = ltp

        pnl = (
            (exit_price - state["entry_price"])
            * 65
            * state["lot"]
        )

        state["pnl"] += pnl

        combined_pnl += pnl

        print(
            f"{state['leg_name']} STOP LOSS HIT @ {exit_price} "
            f"PNL: {pnl}"
        )

        deployments = get_today_deployments()
        users = group_users_by_broker(deployments)
        # ==========================
        # EXIT TELEMETRY / SIGNAL
        # ==========================
        run_async(
            emit_signal(
                build_payload(
                    state["leg_name"],
                    "SELL",
                    state["token"],
                    "exit",
                    "EXIT",
                    exit_price,
                    state["pnl"],
                    combined_pnl,
                    state["lot"],
                    users,
                    state["strike"]
                )
            )
        )

        # ==========================
        # EXIT TRADE LOG
        # ==========================
        log_trade_event(
            event_type="EXIT",
            leg_name=state["leg_name"],
            token=state["token"],
            symbol=SYMBOL,
            side="SELL",
            lot=state["lot"],
            price=exit_price,
            reason="STOP LOSS HIT",
            pnl=state["pnl"],
            cum_pnl=combined_pnl
        )

        # Reset position
        state["position"] = False
        state["entry_price"] = None

        return


def on_message(msg):

    global telemetry, ce_state, pe_state , CE_ID, PE_ID, combined_pnl

    if msg.get("type") != "Quote Data":
        return

    now = datetime.now(IST).time()

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0))

    state = ce_state if token == CE_ID else pe_state

    builder = builders.get(token)

    if not builder:
        return

    candle = builder.process_tick(msg)

    # =========================
    # TELEMETRY (REAL-TIME PnL)
    # =========================
    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (telemetry["ce_ltp"] - ce_state["entry_price"]) * LOTSIZE

    if pe_state["position"]:
        pe_running = (telemetry["pe_ltp"] - pe_state["entry_price"]) * LOTSIZE

    telemetry["ce_pnl"] = ce_state["pnl"] + ce_running
    telemetry["pe_pnl"] = pe_state["pnl"] + pe_running
    telemetry["pnl"] = telemetry["ce_pnl"] + telemetry["pe_pnl"]





    # ==========================================================
    # CE
    # ==========================================================

    if token == CE_ID:
        
        telemetry["ce_ltp"] = ltp
        manage_positions(ce_state, ltp)

        # Every completed 5-minute candle
        if candle:

            print("\n========== CE 5 MIN CANDLE ==========")
            print("CE CANDLE :" , candle)
            print("=====================================\n")

            #print("RSI CE ", ce_state["live_rsi14"])
            #ce_state["rsi14"] = ce_state["live_rsi14"]

            ce_state["previous_ema9"] = ce_state["ema9"]
            ce_state["previous_ema21"] = ce_state["ema21"]


            ce_state["candles"].append(candle)

            print("CE candles loaded")

            ema_candles = ce_state["candles"]

            """
            current_minute = datetime.now(IST).replace(
                second=0,
                microsecond=0
            )

            last_candle_time = ema_candles[-1]["timestamp"].replace(
                second=0,
                microsecond=0
            )

            print("current minute:", current_minute)
            print("last candle time:", last_candle_time)

            if current_minute == last_candle_time:
                print("MATCH - removing last candle")
                ema_candles = ema_candles[:-1]
            else:
                print("NO MATCH - keeping last candle")
            """
            ce_state["ema9"] = calculate_ema(
                [c["close"] for c in ema_candles],
                period=9
            )

            print("CE EMA9 :", ce_state["ema9"])

            ce_state["ema21"] = calculate_ema(
                [c["close"] for c in ema_candles],
                period=21
            )

            print("CE EMA21 :", ce_state["ema21"])


            ce_state["rsi14"], ce_state["avg_gain"], ce_state["avg_loss"] = calculate_rsi(
                [c["close"] for c in ema_candles],
                period=14
            )

            print(
                f"CE RSI14: {ce_state['rsi14']:.2f} "
            )

            if not ce_state["crossover_happened"]:
                detect_ema_bullish_crossover(ce_state)
            else:
                detect_ema_bearish_crossover(ce_state)


            handle_leg(ce_state, candle)

    # ==========================================================
    # PE
    # ==========================================================

    elif token == PE_ID:

 
        telemetry["pe_ltp"] = ltp
        manage_positions(pe_state, ltp)

        # Every completed 5-minute candle
        if candle:

            print("\n========== PE 5 MIN CANDLE ==========")
            print("PE CANDLE :" , candle)
            print("=====================================\n")

            #print("RSI PE ", pe_state["live_rsi14"])
            #pe_state["rsi14"] = pe_state["live_rsi14"]

            pe_state["previous_ema9"] = pe_state["ema9"]
            pe_state["previous_ema21"] = pe_state["ema21"]

            pe_state["candles"].append(candle)

            peema_candles = pe_state["candles"]

            """ 
            current_minute = datetime.now(IST).replace(
                second=0,
                microsecond=0
            )

            last_candle_time = peema_candles[-1]["timestamp"].replace(
                second=0,
                microsecond=0
            )

            print("current minute:", current_minute)
            print("last candle time:", last_candle_time)

            if current_minute == last_candle_time:
                print("MATCH - removing last candle")
                peema_candles = peema_candles[:-1]
            else:
                print("NO MATCH - keeping last candle")

            """
            pe_state["ema9"] = calculate_ema(
                [c["close"] for c in peema_candles],
                period=9
            )

            print("PE EMA9 :", pe_state["ema9"])

            pe_state["ema21"] = calculate_ema(
                [c["close"] for c in peema_candles],
                period=21
            )

            print("PE EMA21 :", pe_state["ema21"])

            pe_state["rsi14"], pe_state["avg_gain"], pe_state["avg_loss"] = calculate_rsi(
                [c["close"] for c in peema_candles],
                period=14
            )

            print("PE RSI",pe_state["rsi14"])

            if not pe_state["crossover_happened"]:
                detect_ema_bullish_crossover(pe_state)
            else:
                detect_ema_bearish_crossover(pe_state)
    
            handle_leg(pe_state, candle)


threading.Thread(target=trade_log_worker, daemon=True).start()


wait_for_start()
next_expiry = get_next_expiry()

print("Next expiry:", next_expiry)


oc = dhan.option_chain(
    under_security_id=13,                       # Nifty
    under_exchange_segment="IDX_I",
    expiry=str(next_expiry)
)



ce_strike, ce_security_id, pe_strike, pe_security_id = select_option_contracts(oc)

print("CE Strike      :", ce_strike)
print("CE Security ID :", ce_security_id)

print("PE Strike      :", pe_strike)
print("PE Security ID :", pe_security_id)

CE_ID = str(ce_security_id)
PE_ID = str(pe_security_id)

ce_state = init_state()
pe_state = init_state()

builders = {
    CE_ID: FiveMinuteCandleBuilder(),
    PE_ID: FiveMinuteCandleBuilder()
}


finder=FindInstrument()

ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")

AngelCE = finder.get_option("NIFTY" , int(ce_strike) , "CE")
AngelPE = finder.get_option("NIFTY" , int(pe_strike) , "PE")

#print("angel tokens" , AngelCE , AngelPE)

ce_state["leg_name"] = "CE"
ce_state["token"] = CE_ID
ce_state["strike"] = ce_strike

pe_state["leg_name"] = "PE"
pe_state["token"] = PE_ID
pe_state["strike"] = pe_strike



# Log CE leg
logtradeleg(
    COMMON_ID,
    "CE",
    f"NIFTY CE {ce_strike}",
    str(ce_strike),
    str(today),
    str(ce_security_id)
)

# Log PE leg
logtradeleg(
    COMMON_ID,
    "PE",
    f"NIFTY PE {pe_strike}",
    str(pe_strike),
    str(today),
    str(pe_security_id)
)


ce_state["candles"] = load_history(
    ce_security_id,
    candle_count=200
)


pe_state["candles"] = load_history(
    pe_security_id,
    candle_count=200
)


ema_candles = ce_state["candles"]

ce_state["ema9"] = calculate_ema(
    [c["close"] for c in ema_candles],
    period=9
)


print("CE EMA9 :", ce_state["ema9"])

ce_state["ema21"] = calculate_ema(
    [c["close"] for c in ema_candles],
    period=21
)


print("CE EMA21 :", ce_state["ema21"])


current_minute = datetime.now(IST).replace(
    second=0,
    microsecond=0
)

last_candle_time = ema_candles[-1]["datetime"].replace(
    second=0,
    microsecond=0
)

print("current minute:", current_minute)
print("last candle time:", last_candle_time)

if current_minute == last_candle_time:
    print("MATCH - removing last candle")
    ema_candles = ema_candles[:-1]
else:
    print("NO MATCH - keeping last candle")

ce_state["live_rsi14"], ce_state["avg_gain"], ce_state["avg_loss"] = calculate_rsi(
    [c["close"] for c in ema_candles],
    period=14
)

print(
    f"CE RSI14: {ce_state['live_rsi14']:.2f} "
    
)

print("CE RSI14 :", ce_state["live_rsi14"])

peema_candles = pe_state["candles"]


pe_state["ema9"] = calculate_ema(
    [c["close"] for c in peema_candles],
    period=9
)

print("PE EMA9 :", pe_state["ema9"])

pe_state["ema21"] = calculate_ema(
    [c["close"] for c in peema_candles],
    period=21
)

print("PE EMA21 :", pe_state["ema21"])


current_minute = datetime.now(IST).replace(
    second=0,
    microsecond=0
)

last_candle_time = peema_candles[-1]["datetime"].replace(
    second=0,
    microsecond=0
)

print("current minute:", current_minute)
print("last candle time:", last_candle_time)

if current_minute == last_candle_time:
    print("MATCH - removing last candle")
    peema_candles = peema_candles[:-1]
else:
    print("NO MATCH - keeping last candle")


pe_state["live_rsi14"], pe_state["avg_gain"], pe_state["avg_loss"] = calculate_rsi(
    [c["close"] for c in peema_candles],
    period=14
)

print(
    f"PE RSI14: {pe_state['live_rsi14']:.2f} "
    
)
print("PE RSI14 :", pe_state["live_rsi14"])


instruments = [
    (MarketFeed.NSE_FNO, CE_ID, MarketFeed.Quote),
    (MarketFeed.NSE_FNO, PE_ID, MarketFeed.Quote)
]

feed = MarketFeed(dhan_context, instruments, "v2")

TOKENS = [
  str(ce_security_id) , str(pe_security_id)
]



#TOKENS = [CE_ID , PE_ID]
""" 
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
        msg = feed.get_data()


        if msg:
            on_message(msg)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
 
