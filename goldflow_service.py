"""
goldflow_service.py

Skeleton service for Jeff's Bot / Korrington's XAU/USD signal engine.

Runs on: Oracle Cloud free-tier VM
Talks to: MT5 account via MetaAPI (streaming quotes + trade execution)

NOTE: MetaAPI/MT5 gives the broker's own tick volume for XAU/USD, not a
real Depth-of-Market / order book like COMEX GC futures has. So the
"absorption" detector below is an approximation from tick-volume spikes +
candle rejection wicks, not the real institutional order book.
"""

import asyncio
import os
import logging
from collections import deque
from dataclasses import dataclass
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi, SynchronizationListener

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("goldflow")

METAAPI_TOKEN = os.environ["METAAPI_TOKEN"]
ACCOUNT_ID = os.environ["METAAPI_ACCOUNT_ID"]
SYMBOL = os.environ.get("SYMBOL", "XAUUSD")
RISK_USD = float(os.environ.get("RISK_PER_TRADE_USD", "10"))
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"


@dataclass
class Candle:
    time: str
    open: float
    high: float
    low: float
    close: float
    tick_volume: int


class VolumeWindow:
    def __init__(self, maxlen: int = 50):
        self.candles = deque(maxlen=maxlen)

    def add(self, c: Candle):
        self.candles.append(c)

    def avg_volume(self) -> float:
        if len(self.candles) < 5:
            return 0.0
        return sum(c.tick_volume for c in self.candles) / len(self.candles)

    def latest(self):
        return self.candles[-1] if self.candles else None


def get_signal(window: VolumeWindow):
    c = window.latest()
    avg = window.avg_volume()
    if c is None or avg == 0:
        return None

    is_big_print = c.tick_volume > avg * 2.5
    body = abs(c.close - c.open)
    full_range = c.high - c.low
    if full_range == 0:
        return None
    wick_ratio = 1 - (body / full_range)

    if is_big_print and wick_ratio > 0.6:
        upper_wick = c.high - max(c.open, c.close)
        lower_wick = min(c.open, c.close) - c.low
        if upper_wick > lower_wick:
            return "sell"
        else:
            return "buy"
    return None


async def place_trade(connection, side: str):
    if DRY_RUN:
        log.info(f"[DRY_RUN] Would place {side.upper()} on {SYMBOL} (risk ${RISK_USD})")
        return
    try:
        if side == "buy":
            result = await connection.create_market_buy_order(
                SYMBOL, volume=0.01, stop_loss=None, take_profit=None,
                options={"comment": "goldflow-signal"}
            )
        else:
            result = await connection.create_market_sell_order(
                SYMBOL, volume=0.01, stop_loss=None, take_profit=None,
                options={"comment": "goldflow-signal"}
            )
        log.info(f"Order result: {result}")
    except Exception as e:
        log.error(f"Trade execution failed: {e}")


async def main():
    api = MetaApi(METAAPI_TOKEN)
    account = await api.metatrader_account_api.get_account(ACCOUNT_ID)

    log.info("Deploying account if needed...")
    if account.state != "DEPLOYED":
        await account.deploy()
    await account.wait_connected()

    connection = account.get_streaming_connection()
    await connection.connect()
    await connection.wait_synchronized()

    window = VolumeWindow(maxlen=50)

    class CandleListener(SynchronizationListener):
        async def on_candles_updated(self, instance_index, candles, equity=None, margin=None,
                                       free_margin=None, margin_level=None,
                                       account_currency_exchange_rate=None):
            for raw in candles:
                if raw.get("symbol") != SYMBOL:
                    continue
                c = Candle(
                    time=raw["time"],
                    open=raw["open"],
                    high=raw["high"],
                    low=raw["low"],
                    close=raw["close"],
                    tick_volume=raw.get("tickVolume", 0),
                )
                window.add(c)
                signal = get_signal(window)
                if signal:
                    log.info(f"Signal fired: {signal.upper()} at {c.close}")
                    await place_trade(connection, signal)

    connection.add_synchronization_listener(CandleListener())
    await connection.subscribe_to_market_data(SYMBOL, [{"type": "candles", "timeframe": "1m"}])

    log.info(f"goldflow_service running on {SYMBOL}, DRY_RUN={DRY_RUN}")
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
