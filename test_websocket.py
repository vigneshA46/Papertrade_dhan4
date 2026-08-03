from dhanhq import dhanhq,DhanContext
from dhanhq import MarketFeed
from dotenv import load_dotenv
import os
from dhan_token import get_access_token
from candle_builder import OneMinuteCandleBuilder


builder = OneMinuteCandleBuilder()


load_dotenv()




# Add your Dhan Client ID and Access Token
client_id = os.getenv("CLIENT_ID")
access_token = get_access_token()

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)


# Structure for subscribing is (exchange_segment, "security_id", subscription_type)

instruments = [(MarketFeed.NSE, "4306", MarketFeed.Quote),   # Ticker - Ticker Data
]

version = "v2"          # Mention Version and set to latest version 'v2'

# In case subscription_type is left as blank, by default Ticker mode will be subscribed.

""" nsequote = dhan.ohlc_data(
    securities = {"NSE_EQ":[1333]}
)

print(nsequote) """

try:
    data = MarketFeed(dhan_context, instruments, "v2")
    while True:
        data.run_forever()
        response = data.get_data()
        print(response)
        

        if response:
            candle = builder.process_tick(response)

            if candle:
                print("✅ 1-Min Candle Completed:")
                print(candle)
                print(response.get('LTP'))
        

except Exception as e:
    print(e)

# Close Connection
    
