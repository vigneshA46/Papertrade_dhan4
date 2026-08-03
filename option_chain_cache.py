import threading
import time

option_chain_data = None

lock = threading.Lock()


def set_option_chain(data):

    global option_chain_data

    with lock:
        option_chain_data = data


def get_option_chain():

    with lock:
        return option_chain_data