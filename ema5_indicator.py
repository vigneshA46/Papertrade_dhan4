from collections import deque


class EMA5:
    def __init__(self):
        self.period = 5
        self.multiplier = 2 / (self.period + 1)

        self.closes = deque(maxlen=self.period)

        self.ema = None

    def update(self, candle):
        """
        candle example:
        {
            'timestamp': '2026-05-08T11:20:00+05:30',
            'open': 265.15,
            'high': 273.5,
            'low': 264.05,
            'close': 272.2,
            'volume': 4530110
        }
        """

        close = candle["close"]

        # Collect first 5 closes
        if self.ema is None:
            self.closes.append(close)

            # Not enough candles yet
            if len(self.closes) < self.period:
                return None

            # Initial EMA = SMA of first 5 closes
            self.ema = sum(self.closes) / self.period

            return round(self.ema, 2)

        # EMA calculation
        self.ema = (
            (close * self.multiplier)
            + (self.ema * (1 - self.multiplier))
        )

        return round(self.ema, 2)