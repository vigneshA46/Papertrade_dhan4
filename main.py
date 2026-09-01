from dispatcher import publish
from dhanhq import MarketFeed
from dhanhq import dhanhq,DhanContext
from datetime import datetime
from dhan_token import get_access_token
from dotenv import load_dotenv
import os
import pytz
import threading

import option_chain_manager

option_chain_manager.update_option_chain()

print("Option Chain Updated")
print("Option Chain:", option_chain_manager.get_option_chain())

import nifty_rsa_buying_5min as strategy7


#import paper_trade_niftyoption50_no_reentry as strategy1
#import nifty_option_buying_cumulative_ltp as straegy16


#import paper_trade_niftyoption50_reentry as strategy2
#import paper_trade_niftyoption35_reentry as strategy3
#import paper_trade_niftyoption35_reentry_point as strategy4
#import paper_trade_niftyoption50_reentry_point as strategy5
#import Nifty_option_buying3k as strategy9

#import nifty_option_buying_50_ltp as strategy15

#import delta_option_buying as strategy6
#import paper_trade_niftyoption8_no_reentry as strategy8
#import vwap_option_buying as strategy10



##################
##################
#import Nifty_option_buying_tsl as strategy11




rb_started = False
rb_buying=False

# collect all tokens
ALL_TOKENS = set()
""" if 'strategy1' in globals():
    ALL_TOKENS.update(strategy1.TOKENS)
 """


access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")
dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

print("ALL TOKENS",ALL_TOKENS)

instruments = [
    (MarketFeed.NSE_FNO, token, MarketFeed.Quote)
    for (token) in ALL_TOKENS
]

instruments.append((MarketFeed.IDX, "13", MarketFeed.Quote))

feed = MarketFeed(dhan_context, instruments, "v2")

def on_message(msg):

    global rb_started, rb_buying

    try:

        token = str(msg["security_id"])
        publish(token, msg)

    except Exception as e:

        print("ON MESSAGE ERROR:", e)
    
feed.run_forever()
while True:
    try:


            #ALL_TOKENS.update(strategy9.TOKENS)

            #instruments = [
                #(MarketFeed.NSE_FNO, token, MarketFeed.Quote)
                #for (token) in ALL_TOKENS
            #]

            #feed = MarketFeed(dhan_context, instruments, "v2")

            #feed.on_message = on_message

            #rb_started = True

        data = feed.get_data()

        if data:
            on_message(data)

    except Exception as e:
        print("WS ERROR:", e)
        feed.run_forever()
 