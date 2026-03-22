import time
from collections import defaultdict
import sys
import os
import logging
from providers._subscription import wall_clock_counts
from providers._volume import VolumeTracker

# Configure logging with file and line number
stdout_handler = logging.StreamHandler(sys.stdout)
stderr_handler = logging.StreamHandler(sys.stderr)
formatter = logging.Formatter('%(asctime)s - HighLowTicker  - %(message)s')
stdout_handler.setFormatter(formatter)
stderr_handler.setFormatter(formatter)
stdout_handler.setLevel(logging.DEBUG)
stdout_handler.addFilter(lambda record: record.levelno == logging.INFO)
stderr_handler.setLevel(logging.WARNING)

logging.basicConfig(
    level=logging.DEBUG,
    handlers=[stdout_handler, stderr_handler]
)
logger = logging.getLogger(__name__)

class HighLowTicker:
    def __init__(self):
        logger.info("Initializing HighLowTicker")
        self.new_highs = defaultdict(int)
        self.new_lows = defaultdict(int)
        self.last_high = {}
        self.last_low = {}
        self.last_pct_change = {}
        self.current_week52_highs = set()
        self.current_week52_lows = set()
        self.week52_high_prices = {}  # Track the actual 52-week high price for each symbol
        self.week52_low_prices = {}   # Track the actual 52-week low price for each symbol
        self.price_ranges = {"low": 0, "mid": 0, "high": 0}
        self.message_count = 0
        self.initialized_symbols = set()
        self.high_timestamps = []
        self.low_timestamps = []
        self.last_price = {}
        self._vol_tracker = VolumeTracker()
        self._last_volume: dict = {}
        self._volume_spikes: dict = {}

    def process_stock(self, stock):
        symbol = stock.get('key')
        price = stock.get('LAST_PRICE')
        daily_high = stock.get('HIGH_PRICE')
        daily_low = stock.get('LOW_PRICE')
        week52_high = stock.get('HIGH_PRICE_52_WEEK')
        week52_low = stock.get('LOW_PRICE_52_WEEK')
        percent_change = stock.get('NET_CHANGE_PERCENT')
        current_time = time.time()

        if not symbol:
            logger.warning(f"Missing symbol in stock message: {stock}")
            return False
        if price is None or price == 0:
            # logger.warning(f"Missing or invalid price: symbol={symbol}, price={price}, message={stock}")
            return False
        
        # Store the last price for the symbol
        self.last_price[symbol] = price

        # Volume spike detection (delta-based: Schwab sends cumulative day volume)
        total_volume = stock.get('TOTAL_VOLUME', 0) or 0
        prev_volume = self._last_volume.get(symbol, 0)
        bar_volume = max(0, total_volume - prev_volume)
        if total_volume > 0:
            self._last_volume[symbol] = total_volume
        if bar_volume > 0:
            ratio = self._vol_tracker.record(symbol, bar_volume, current_time)
            if ratio is not None:
                if ratio > 1.0:
                    self._volume_spikes[symbol] = ratio
                else:
                    self._volume_spikes.pop(symbol, None)

        # Fallback for daily high/low if 0 or None
        if daily_high == 0 or daily_high is None:
            fallback_price = stock.get('CLOSE_PRICE') or stock.get('REGULAR_MARKET_LAST_PRICE') or price
            daily_high = fallback_price
            # logger.info(f"High price is 0 or None for {symbol}, using fallback: {daily_high}")
        if daily_low == 0 or daily_low is None:
            fallback_price = stock.get('CLOSE_PRICE') or stock.get('REGULAR_MARKET_LAST_PRICE') or price
            daily_low = fallback_price
            # logger.info(f"Low price is 0 or None for {symbol}, using fallback: {daily_low}")

        # Handle None values for daily high and low
        daily_high = daily_high if daily_high is not None else self.last_high.get(symbol, price)
        daily_low = daily_low if daily_low is not None else self.last_low.get(symbol, price)
        
        # Handle None values for week52_high and week52_low
        if week52_high is None:
            week52_high = self.week52_high_prices.get(symbol, 1e19)
            # logger.info(f"{symbol} 52 WEEK HIGH ERROR, using stored value or default: {week52_high}")
        if week52_low is None:
            week52_low = self.week52_low_prices.get(symbol, -1)
            # logger.info(f"{symbol} 52 WEEK LOW ERROR, using stored value or default: {week52_low}")

        # Store the 52-week high and low prices
        if symbol not in self.week52_high_prices:
            self.week52_high_prices[symbol] = week52_high
        if symbol not in self.week52_low_prices:
            self.week52_low_prices[symbol] = week52_low

        if symbol not in self.initialized_symbols:
            self.last_high[symbol] = daily_high
            self.last_low[symbol] = daily_low
            self.last_pct_change[symbol] = percent_change
            self.initialized_symbols.add(symbol)
            # logger.info(f"INIT ${symbol} LAST: {price}, 52W HIGH: {week52_high}, 52W LOW: {week52_low}")

            # Check if the initial daily high/low matches the 52-week high/low
            if daily_high >= week52_high and symbol not in self.current_week52_highs:
                self.current_week52_highs.add(symbol)
                # logger.info(f"{symbol} WEEK 52 HIGH (initial): {daily_high}")
            if daily_low <= week52_low and symbol not in self.current_week52_lows:
                self.current_week52_lows.add(symbol)
                # logger.info(f"{symbol} WEEK 52 LOW (initial): {daily_low}")
        else:
            self.last_pct_change[symbol] = percent_change
            # Check for new daily highs
            if daily_high > self.last_high[symbol]:
                self.new_highs[symbol] += 1
                self.last_high[symbol] = daily_high
                self.high_timestamps.append((symbol, current_time))
                # logger.info(f"{symbol} NEW HIGHS {daily_high}")

                # Check if this new high exceeds the stored 52-week high
                if daily_high > self.week52_high_prices[symbol]:
                    # logger.info(f"{symbol} NEW 52-WEEK HIGH DETECTED: {daily_high}, previous: {self.week52_high_prices[symbol]}")
                    self.week52_high_prices[symbol] = daily_high
                    self.current_week52_highs.add(symbol)  # Reset 52-week high status
                # Check if this matches the 52-week high
                elif daily_high >= self.week52_high_prices[symbol] and symbol not in self.current_week52_highs:
                    self.current_week52_highs.add(symbol)
                    # logger.info(f"{symbol} WEEK 52 HIGH: {daily_high}")

            # Check for new daily lows
            if daily_low < self.last_low[symbol]:
                self.new_lows[symbol] += 1
                self.last_low[symbol] = daily_low
                self.low_timestamps.append((symbol, current_time))
                # logger.info(f"{symbol} NEW LOWS: {daily_low}")

                # Check if this new low is below the stored 52-week low
                if daily_low < self.week52_low_prices[symbol]:
                    # logger.info(f"{symbol} NEW 52-WEEK LOW DETECTED: {daily_low}, previous: {self.week52_low_prices[symbol]}")
                    self.week52_low_prices[symbol] = daily_low
                    self.current_week52_lows.add(symbol)  # Reset 52-week low status
                # Check if this matches the 52-week low
                elif daily_low <= self.week52_low_prices[symbol] and symbol not in self.current_week52_lows:
                    self.current_week52_lows.add(symbol)
                    # logger.info(f"{symbol} WEEK 52 LOW: {daily_low}")
            
        # Update price ranges
        if price < 10:
            self.price_ranges["low"] += 1
        elif 10 <= price <= 50:
            self.price_ranges["mid"] += 1
        else:
            self.price_ranges["high"] += 1

        self.message_count += 1
        return True

    def get_state(self):
        current_time = time.time()
        high_counts = wall_clock_counts([ts for _, ts in self.high_timestamps])
        low_counts  = wall_clock_counts([ts for _, ts in self.low_timestamps])
        self.high_timestamps = [(sym, ts) for sym, ts in self.high_timestamps if current_time - ts <= 1200]
        self.low_timestamps = [(sym, ts) for sym, ts in self.low_timestamps if current_time - ts <= 1200]

        # Save index prices 
        index_prices = {
            "SPY": self.last_price.get("SPY", 0),
            "DIA": self.last_price.get("DIA", 0),
            "QQQ": self.last_price.get("QQQ", 0),
        }

        state = {
            "newHighs": dict(self.new_highs),
            "newLows": dict(self.new_lows),
            "lastHigh": self.last_high,
            "lastLow": self.last_low,
            "week52Highs": list(self.current_week52_highs),
            "week52Lows": list(self.current_week52_lows),
            "priceRanges": self.price_ranges,
            "messageCount": self.message_count,
            "highCounts": high_counts,
            "lowCounts": low_counts,
            'percentChange': self.last_pct_change,
            "indexPrices": index_prices,
            "volumeSpikes": dict(self._volume_spikes),
        }

        # logger.info(f'IndexPrices: {index_prices}')
        # logger.info(f"Highs: {state['week52Highs']}")
        # logger.info(f"Lows: {state['week52Lows']}")
        return state
