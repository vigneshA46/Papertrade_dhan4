import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(__file__)))


from brokers.zebuclient import ZebuClient
from brokers.symbolresolver import SymbolResolver


client = ZebuClient(
    uid="Z64199",
    password="H@s@n33",
    api_key="6aMvkdQuGkYyHAp4puEHF6NCu9eC67D6",
    vendor_code="Z64199",
    factor2="03032003"
)


# Login
client.login()

# Get details
print(client.get_client_details())

# Place MARKET order
client.place_order(
    exch="NFO",
    tsym="NIFTY13APR26C22900",  # corrected expiry
    qty=65,                     # 1 lot
    trantype="B"
)

