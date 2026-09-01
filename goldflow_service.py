"""
goldflow_service.py

Signal + risk-managed execution service for the XAU/USD signal engine.

Runs on: Oracle Cloud VPS (systemd service: goldflow.service)
Talks to: MT5 account via MetaAPI (streaming quotes + trade execution)

NOTE: MetaAPI/MT5 gives the broker's own tick volume for XAU/USD, not a
real Depth-of-Market like COMEX GC futures has. So the "absorption"
detector below is an approximation from tick-volume spikes + candle
rejection wicks, not the real institutional order book.

Env vars (.env, never committed):
    METAAPI_TOKEN=your_metaapi_token
    METAAPI_ACCOUNT_ID=your_mt5_account_id_in_metaapi
    SYMBOL=XAUUSD
    RISK_PER_TRADE_USD=10
    LOT_SIZE=0.01
    SL_DISTANCE_USD=3.0
    TP_DISTANCE_USD=6.0
    MAX_DAILY_LOSS_USD=20.0
    DRY_RUN=true
"""

import asyncio
import os
import logging
from collections import deque
from dataclasses import dataclass
from datetime import datetime, timezone
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
LOT_SIZE = float(os.environ.get("LOT_SIZE", "0.01"))
SL_DISTANCE = float(os.environ.get("SL_DISTANCE_USD", "3.0"))
TP_DISTANCE = float(os.environ.get("TP_DISTANCE_USD", "6.0"))
MAX_DAILY_LOSS_USD = float(os.environ.get("MAX_DAILY_LOSS_USD", "20.0"))


@dataclass
class Candle:
    time: str
    open: float
    high: float
    low: float
    close: float
    tick_volume: int


class VolumeWindow:
    """Rolling window used to spot abnormal tick-volume vs recent average."""

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


class RiskManager:
    """Tracks daily equity drawdown and halts new trades if the max daily
    loss is breached. Resets automatically at the start of a new UTC day."""

    def __init__(self, max_daily_loss_usd: float):
        self.max_daily_loss_usd = max_daily_loss_usd
        self.day = None
        self.day_start_equity = None
        self.trading_halted = False

    def _check_new_day(self, current_equity: float):
        today = datetime.now(timezone.utc).date()
        if self.day != today:
            self.day = today
            self.day_start_equity = current_equity
            self.trading_halted = False
            log.info(f"New trading day. Baseline equity: ${current_equity:.2f}")

    def update_equity(self, current_equity: float):
        self._check_new_day(current_equity)
        if self.day_start_equity is None:
            return
        loss = self.day_start_equity - current_equity
        if loss >= self.max_daily_loss_usd and not self.trading_halted:
            self.trading_halted = True
            log.warning(
                f"MAX DAILY LOSS HIT: down ${loss:.2f} (limit ${self.max_daily_loss_usd:.2f}). "
                f"Trading halted until the next UTC day."
            )

    def can_trade(self) -> bool:
        return not self.trading_halted


def calculate_sl_tp(entry_price: float, side: str):
    """Returns (stop_loss, take_profit) price levels for a given side."""
    if side == "buy":
        sl = entry_price - SL_DISTANCE
        tp = entry_price + TP_DISTANCE
    else:
        sl = entry_price + SL_DISTANCE
        tp = entry_price - TP_DISTANCE
    return round(sl, 2), round(tp, 2)


def get_signal(window: VolumeWindow):
    """
    Returns 'buy', 'sell', or None.

    Current logic (approximation): tick volume on the latest candle is a
    big multiple of the rolling average AND the candle shows rejection
    (long wick) rather than a clean breakout.
    """
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


async def place_trade(connection, side: str, entry_price: float, risk_manager: RiskManager):
    if not risk_manager.can_trade():
        log.warning(f"Signal fired ({side.upper()}) but trading is halted for today. Skipping.")
        return

    sl, tp = calculate_sl_tp(entry_price, side)

    if DRY_RUN:
        log.info(
            f"[DRY_RUN] Would place {side.upper()} on {SYMBOL} at ~{entry_price} "
            f"| SL={sl} TP={tp} | lot={LOT_SIZE}"
        )
        return

    try:
        if side == "buy":
            result = await connection.create_market_buy_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                options={"comment": "goldflow-signal"}
            )
        else:
            result = await connection.create_market_sell_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                options={"comment": "goldflow-signal"}
            )
        log.info(f"Order placed: {side.upper()} SL={sl} TP={tp} | Result: {result}")
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
    risk_manager = RiskManager(max_daily_loss_usd=MAX_DAILY_LOSS_USD)

    try:
        account_info = connection.terminal_state.account_information
        if account_info and "equity" in account_info:
            risk_manager.update_equity(account_info["equity"])
    except Exception:
        pass

    class MainListener(SynchronizationListener):
        async def on_candles_updated(self, instance_index, candles, equity=None, margin=None,
                                       free_margin=None, margin_level=None,
                                       account_currency_exchange_rate=None):
            if equity is not None:
                risk_manager.update_equity(equity)

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
                    await place_trade(connection, signal, c.close, risk_manager)

        async def on_account_information_updated(self, instance_index, account_information):
            equity = account_information.get("equity")
            if equity is not None:
                risk_manager.update_equity(equity)

    connection.add_synchronization_listener(MainListener())
    await connection.subscribe_to_market_data(SYMBOL, [{"type": "candles", "timeframe": "1m"}])

    log.info(
        f"goldflow_service running on {SYMBOL}, DRY_RUN={DRY_RUN}, "
        f"lot={LOT_SIZE}, SL=${SL_DISTANCE}, TP=${TP_DISTANCE}, "
        f"max_daily_loss=${MAX_DAILY_LOSS_USD}"
    )
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
