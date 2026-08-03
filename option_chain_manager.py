import threading
import os

from dhanhq import dhanhq, DhanContext
from dhan_token import get_access_token

access_token = get_access_token()
client_id = os.getenv("CLIENT_ID")

dhan_context = DhanContext(client_id, access_token)
dhan = dhanhq(dhan_context)

_option_chain = None
_lock = threading.Lock()


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



def update_option_chain():
    """
    Fetch latest NIFTY option chain and update cache.
    Can be called anytime.
    """

    global _option_chain

    next_expiry = get_next_expiry()

    oc = dhan.option_chain(
        under_security_id=13,
        under_exchange_segment="IDX_I",
        expiry=str(next_expiry)
    )

    with _lock:
        _option_chain = oc

    print("[OPTION CHAIN] Updated")


def get_option_chain():
    """
    Returns cached option chain.
    """

    with _lock:
        return _option_chain