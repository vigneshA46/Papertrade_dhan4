import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))




import upstox_client
from upstox_client.rest import ApiException
from brokers.upstox_api import UpstoxBroker

broker = UpstoxBroker(access_token="eyJ0eXAiOiJKV1QiLCJrZXlfaWQiOiJza192MS4wIiwiYWxnIjoiSFMyNTYifQ.eyJzdWIiOiIzWUNDNVMiLCJqdGkiOiI2OWRhNjdiMjI0NGFiMjY4ZjdiZThkMmIiLCJpc011bHRpQ2xpZW50IjpmYWxzZSwiaXNQbHVzUGxhbiI6ZmFsc2UsImlhdCI6MTc3NTkyMTA3NCwiaXNzIjoidWRhcGktZ2F0ZXdheS1zZXJ2aWNlIiwiZXhwIjoxNzc1OTQ0ODAwfQ.atp49yjvsGkljycByJBtdz5jrDcEwWAXjlR58CbuAZE")

res = broker.get_profile()
print(res)


inst = broker.get_option(symbol="NIFTY", option_type="CE", mode="STRIKE", strike=24000)

print(inst)


def test_buy_nifty_ce(broker):
    try:
        # Step 1: Get instrument
        inst = broker.get_option(
            symbol="NIFTY",
            option_type="CE",
            mode="STRIKE",
            strike=24000
        )

        if inst["status"] != "success":
            print("Instrument fetch failed:", inst)
            return

        print("Instrument:", inst)

        # Step 2: Calculate qty (1 lot)
        qty = inst["lot_size"] * 1

        # Step 3: Place order
        res = broker.place_order(
            instrument_token=inst["instrument_key"],
            qty=qty,
            side="BUY",
            order_type="MARKET",
            product="I"
        )

        print("Order Response:", res)

    except Exception as e:
        print("Error:", str(e))


test_buy_nifty_ce(broker)