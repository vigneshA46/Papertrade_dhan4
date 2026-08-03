import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from brokers.ant_broker import AntBroker
from TradeMaster.TradeSync import *
import hashlib

# 🔐 FROM YOUR URL
USER_ID = "2505610"
AUTH_CODE = "33WHA8PJPWBJR7B35W58"
SECRET_KEY = "5xWormtQR5tjK5ZB4Hcfksy3Z2LJ3WLHWBEjoyRegOdNOrp5lKEMOs0kyn3SQErcReitz7ZVmnEBJTJ1rTJE3sDdcCGlFIjJX7c3"   # from developer portal

# 🎯 ORDER CONFIG
SYMBOL = "NIFTY"
EXPIRY = "2026-04-21"   # ✅ use what SDK returned earlier
STRIKE = "23500"
QTY = 65

def generate_checksum(user_id, auth_code, secret_key):
    data = user_id + auth_code + secret_key
    return hashlib.sha256(data.encode()).hexdigest()

def main():

    # 1️⃣ Init broker
    broker = AntBroker(USER_ID, AUTH_CODE, SECRET_KEY)

    # 2️⃣ Generate checksum login
    checksum = generate_checksum(USER_ID, AUTH_CODE, SECRET_KEY)

    session = broker.trade.get_session_id(check_sum=checksum)

    print("Session:", session)

    # ❌ STOP if login failed
    if not session.get("userSession"):
        print("Login failed. Fix auth.")
        return

    print("✅ Login successful")
    

    # 3️⃣ Get instrument
    instrument = broker.get_fno_instrument(
        exchange=Exchange.NFO,
        symbol=SYMBOL,
        expiry=EXPIRY,
        strike=STRIKE,
        is_ce=True
    )


    print("Instrument:", instrument)
 
    # 4️⃣ Place order
    order = broker.place_market_order(
        instrument=instrument,
        qty=QTY,
        side=TransactionType.Buy,
        tag="TEST_ORDER"
    )

    print("Order Response:", order) 


    

def place_crudeoil_order():

    # 1️⃣ Init broker
    broker = AntBroker(USER_ID, AUTH_CODE, SECRET_KEY)

    # 2️⃣ Generate checksum login
    checksum = generate_checksum(USER_ID, AUTH_CODE, SECRET_KEY)

    session = broker.trade.get_session_id(check_sum=checksum)

    print("Session:", session)

    # ❌ STOP if login failed
    if not session.get("userSession"):
        print("Login failed. Fix auth.")
        return

    print("✅ Login successful")


    print("\n🚀 Placing MCX CRUDEOIL Order...")

    # 1️⃣ Get instrument
    instrument = broker.get_fno_instrument(
        exchange=Exchange.MCX,
        symbol="CRUDEOIL",
        expiry="2026-04-19"
        )

    print("Instrument:", instrument)

    # 2️⃣ Get LTP


    # 3️⃣ Place LIMIT order (IMPORTANT)
    order = broker.place_market_order(
        instrument=instrument,
        qty=100,
        side=TransactionType.Buy,
        tag="TEST_ORDER"
    )

    print("Order Response:", order)


if __name__ == "__main__":
    main()