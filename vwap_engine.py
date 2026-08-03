
# vwap_engine.py
import datetime

class VWAPEngine:
    def __init__(self):
        self.cum_pv = 0.0
        self.cum_vol = 0
        self.current_vwap = None

    def update(self, price: float, qty: int):
        """
        Update VWAP using tick data
        :param price: Last traded price (LTP)
        :param qty: Last traded quantity (LTQ)
        """

        if qty <= 0:
            return self.current_vwap

        self.cum_pv += price * qty
        self.cum_vol += qty

        if self.cum_vol > 0:
            self.current_vwap = self.cum_pv / self.cum_vol

        return self.current_vwap

    def get(self):
        return self.current_vwap

    def reset(self):
        """Reset VWAP (use at market open)"""
        self.cum_pv = 0.0
        self.cum_vol = 0
        self.current_vwap = None


class VWAPManager:
    def __init__(self):
        self.engines = {}  # key: security_id

    def get_engine(self, security_id):
        if security_id not in self.engines:
            self.engines[security_id] = VWAPEngine()
        return self.engines[security_id]

    def on_tick(self, msg: dict):
        """
        Process incoming tick
        :param msg: Dhan websocket tick data
        """

        try:
            security_id = msg["security_id"]

            price = float(msg["LTP"])
            qty = int(msg["LTQ"])   # 🔥 IMPORTANT

            engine = self.get_engine(security_id)
            vwap = engine.update(price, qty)

            return security_id, vwap

        except Exception as e:
            print(f"VWAP ERROR: {e}")
            return None, None

    def reset_all(self):
        """Reset all instruments (call at 09:15)"""
        for engine in self.engines.values():
            engine.reset()


class MinuteVWAPSampler:
    def __init__(self):
        self.last_minute = {}

    def should_emit(self, security_id):
        now = datetime.datetime.now().replace(second=0, microsecond=0)

        if security_id not in self.last_minute:
            self.last_minute[security_id] = now
            return False

        if now != self.last_minute[security_id]:
            self.last_minute[security_id] = now
            return True

        return False