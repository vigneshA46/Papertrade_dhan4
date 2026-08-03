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
from signal_emitter import emit_signal
import asyncio
from find_instrument import FindInstrument



vwap_manager = VWAPManager()

ce_strike = None
pe_strike = None

# =========================
# CONFIG
# =========================

COMMON_ID = '136b0559-76dc-435c-bb08-3e6a584a46d0'
strategy_id = '136b0559-76dc-435c-bb08-3e6a584a46d0'
trade_log_queue = Queue()

finder = FindInstrument()



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
TRADE_END   = dtime(15, 14)

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



def build_payload(name, side, token , reason,event_type,ltp,pnl,cum_pnl,lot,users , strike = ATM):

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
def wait_for_start():
    print("⏳ Waiting for market...")
    while True:
        if datetime.now(IST).time() >= TRADE_START:
            print("✅ Market Started")
            return
        time.sleep(1)

def calculate_strikes(fut_price, step=50):
    atm = round(fut_price / step) * step
    return atm



def get_today_deployments():
    url = f"https://algoapi.dreamintraders.in/api/deployments/today/{COMMON_ID}"

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



def check_mtm_and_kill_switch():
    global combined_exit_active , combined_pnl , CE_ID , PE_ID

    if combined_exit_active:
        return

    ce_ltp = telemetry.get("ce_ltp", 0)
    pe_ltp = telemetry.get("pe_ltp", 0)

    ce_running = 0
    pe_running = 0

    if ce_state["position"]:
        ce_running = (ce_ltp - ce_state["entry_price"]) * LOTSIZE 

    if pe_state["position"]:
        pe_running = (pe_ltp - pe_state["entry_price"]) * LOTSIZE



    ce_total = ce_state["pnl"] + ce_running
    pe_total = pe_state["pnl"] + pe_running

    total_pnl = ce_total + pe_total
    combined_pnl = total_pnl
    
    telemetry["ce_pnl"] = float(ce_total)
    telemetry["pe_pnl"] = float(pe_total)
    telemetry["pnl"] = combined_pnl



    if total_pnl >= 3000 or total_pnl <= -3000:

        print("🚨 MTM LIMIT HIT — FORCE EXIT ALL")

        combined_exit_active = True

        # CE FORCE EXIT
        if ce_state["position"]:
            print(f"🔴 CE FORCE EXIT | TOKEN: {CE_ID} | LTP: {telemetry.get('ce_ltp')} | TOTAL PNL: {ce_state['pnl']:.2f}")

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)


            run_async(
                emit_signal(
                    build_payload(
                        "CE",
                        "SELL",
                        str(CE_ID),
                        "PROFIT EXIT",
                        "EXIT",
                        str(telemetry.get('ce_ltp')),
                        ce_state["pnl"],
                        combined_pnl,
                        ce_state["lot"],
                        users,
                        strike = ce_strike
                    )
                )
            )

            log_trade_event(
                
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=1,
                price=telemetry.get('ce_ltp'),
                reason="FORCE EXIT MTM",
                pnl= ce_state["pnl"],
                cum_pnl=combined_pnl
                )

            ce_state["position"] = False
            ce_state["entry_price"] = None
            ce_state["last_price"] = None

        # PE FORCE EXIT
        if pe_state["position"]:
            print(f"🔴 PE FORCE EXIT | TOKEN: {PE_ID} | LTP: {telemetry.get('pe_ltp')} | TOTAL PNL: {pe_state['pnl']:.2f}")

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)


            run_async(
                emit_signal(
                    build_payload(
                        "PE",
                        "SELL",
                        str(PE_ID),
                        "PROFIT EXIT",
                        "EXIT",
                        str(telemetry.get('pe_ltp')),
                        pe_state["pnl"],
                        combined_pnl,
                        pe_state["lot"],
                        users,
                        strike = pe_strike
                    )
                )
            )

            log_trade_event(
                
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=1,
                price=telemetry.get('pe_ltp'),
                reason="FORCE EXIT MTM",
                pnl= pe_state["pnl"],
                cum_pnl=combined_pnl
                )

            pe_state["position"] = False
            pe_state["entry_price"] = None
            pe_state["last_price"] = None

        ce_state["trading_disabled"] = True
        pe_state["trading_disabled"] = True



def handle_leg(name, token, candle, state, ltp, vwap):

    global combined_pnl , ce_strike , pe_strike

    now = datetime.now(IST).time()

    close = candle["close"]

    timestamp = candle["timestamp"]

    # =========================
    # TIME EXIT (15:20)
    # =========================
    if now >= TRADE_END:

        telemetry["status"] = "CLOSED"

        if state["position"]:

            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * LOTSIZE

            state["pnl"] += pnl

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            print(
                f"🔴 {name} TIME EXIT | TOKEN: {token} | "
                f"LTP: {ltp} | PNL: {pnl:.2f}"
            )

            run_async(
                emit_signal(
                    build_payload(
                        name,
                        "SELL",
                        token,
                        "TIME EXIT",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        state["lot"],
                        users,
                        strike = ce_strike if name == "CE" else pe_strike
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


            

            state["position"] = False
            state["entry_price"] = None
            state["last_price"] = None

        state["trading_disabled"] = True
        return

    # =========================
    # STOP TRADING
    # =========================
    if state["trading_disabled"]:
        return


    if state["position"]:

        # candle close above VWAP
        if close < vwap:
            exit_price = ltp

            pnl = (exit_price - state["entry_price"]) * LOTSIZE

            state["pnl"] += pnl

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)


            print(f"🔴 CE VWAP CLOSE EXIT | {ltp} < {vwap} | PNL: {pnl:.2f}")

            run_async(
                emit_signal(
                    build_payload(
                        name,
                        "SELL",
                        token,
                        "VWAP EXIT",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        state["lot"],
                        users,
                        strike = ce_strike if name == "CE" else pe_strike
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
                price=ltp,
                reason="VWAP TICK EXIT",
                pnl=state["pnl"],
                cum_pnl=combined_pnl
            )

            state["position"] = False
            state["entry_price"] = None
            state["last_price"] = None



    # =========================
    # ENTRY SIGNAL GENERATION
    # =========================
    if not state["position"]:

        # candle close above VWAP
        if close > vwap:
            state["entry_signal"] = True

        else:
            state["entry_signal"] = False

    # =========================
    # ENTRY EXECUTION
    # =========================
    if state["entry_signal"] and not state["position"] and not state["trading_disabled"]:

        entry_price = ltp

        state["entry_price"] = entry_price
        state["entry_time"] = datetime.now(IST).isoformat()

        state["position"] = True
        state["entry_signal"] = False
        state["last_price"] = ltp

        deployments = get_today_deployments()
        users = group_users_by_broker(deployments)

        print(
            f"🟢 {name} BUY | TOKEN: {token} | "
            f"LTP: {ltp} | VWAP: {round(vwap,2)}"
        )

        run_async(
            emit_signal(
                build_payload(
                    name,
                    "BUY",
                    token,
                    "VWAP ENTRY",
                    "ENTRY",
                    ltp,
                    state["pnl"],
                    combined_pnl,
                    state["lot"],
                    users,
                    strike = ce_strike if name == "CE" else pe_strike
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
            reason="VWAP ENTRY",
            pnl=state["pnl"],
            cum_pnl=combined_pnl
        )


def on_message(msg):
    global combined_pnl

    if msg.get("type") != "Quote Data":
        return

    token = str(msg["security_id"])
    ltp = float(msg.get("LTP", 0))

    # =========================
    # UPDATE PNL TICKWISE
    # =========================
    if token == str(CE_ID):
        telemetry["ce_ltp"] = ltp

    elif token == str(PE_ID):
        telemetry["pe_ltp"] = ltp


    # =========================
    # VWAP UPDATE
    # =========================
    _, vwap = vwap_manager.on_tick(msg)

    if token == str(CE_ID):

        if ce_state["position"] and ltp < vwap - 8:

            exit_price = ltp

            pnl = (exit_price - ce_state["entry_price"]) * LOTSIZE

            ce_state["pnl"] += pnl
            #combined_pnl += pnl
            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)


            print(f"🔴 CE VWAP TICK EXIT | {ltp} < {vwap} | PNL: {pnl:.2f}")

            run_async(
                emit_signal(
                    build_payload(
                        "CE",
                        "SELL",
                        token,
                        "VWAP EXIT",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        ce_state["lot"],
                        users,
                        strike = ce_strike
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name="CE",
                token=CE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=ce_state["lot"],
                price=ltp,
                reason="VWAP TICK EXIT",
                pnl=ce_state["pnl"],
                cum_pnl=combined_pnl
            )

            ce_state["position"] = False
            ce_state["entry_price"] = None
            ce_state["last_price"] = None

    if token == str(PE_ID):

        if pe_state["position"] and ltp < vwap - 8:

            exit_price = ltp

            pnl = (exit_price - pe_state["entry_price"]) * LOTSIZE

            pe_state["pnl"] += pnl
            #combined_pnl += pnl

            deployments = get_today_deployments()
            users = group_users_by_broker(deployments)

            print(f"🔴 PE VWAP TICK EXIT | {ltp} < {vwap} | PNL: {pnl:.2f}")

            run_async(
                emit_signal(
                    build_payload(
                        "PE",
                        "SELL",
                        token,
                        "TIME EXIT",
                        "EXIT",
                        ltp,
                        pnl,
                        combined_pnl,
                        pe_state["lot"],
                        users,
                        strike = pe_strike
                    )
                )
            )

            log_trade_event(
                event_type="EXIT",
                leg_name="PE",
                token=PE_ID,
                symbol=SYMBOL,
                side="SELL",
                lot=pe_state["lot"],
                price=ltp,
                reason="VWAP TICK EXIT",
                pnl=pe_state["pnl"],
                cum_pnl=combined_pnl
            )

            pe_state["position"] = False
            pe_state["entry_price"] = None
            pe_state["last_price"] = None

    check_mtm_and_kill_switch()
    # =========================
    # KILL SWITCH CHECK (EVERY TICK 🔥)
    # =========================

    if vwap is None:
        return

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



    if candle:

        current_time = datetime.fromisoformat(
                candle["timestamp"]
            ).time()

        print("CANDLE", candle)
        print("VWAP", vwap)

        if token == str(CE_ID):

            handle_leg(
                "CE",
                token,
                candle,
                ce_state,
                ltp,
                vwap
            )

        elif token == str(PE_ID):

            handle_leg(
                "PE",
                token,
                candle,
                pe_state,
                ltp,
                vwap
            )





#======================
#    MAIN
#======================


wait_for_start()
threading.Thread(target=trade_log_worker, daemon=True).start()


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


AngelCE = finder.get_option("NIFTY" , int(ce_strike) , "CE")
AngelPE = finder.get_option("NIFTY" , int(pe_strike) , "PE")

ce_row = find_option_security(fno_df, ce_strike, "CE", today, "NIFTY")
pe_row = find_option_security(fno_df, pe_strike, "PE", today, "NIFTY")


CE_ID = ce_row["SECURITY_ID"]
PE_ID = pe_row["SECURITY_ID"]


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


builders = {
    str(CE_ID): OneMinuteCandleBuilder(),
    str(PE_ID): OneMinuteCandleBuilder()
}


ce_state = init_state()
pe_state = init_state()


instruments = [
    (MarketFeed.NSE_FNO, str(ce_row["SECURITY_ID"]), MarketFeed.Quote),
    (MarketFeed.NSE_FNO, str(pe_row["SECURITY_ID"]), MarketFeed.Quote)
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

