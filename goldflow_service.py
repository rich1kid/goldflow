"""
goldflow_service.py

Late-candle momentum scalper for XAU/USD.

Strategy (as specified):
  For both the M1 and H1 timeframes, watch the candle currently forming.
  In the final 25% of that candle's duration (last 15s of an M1 candle,
  last 15min of an H1 candle), if volume-so-far is unusually high for
  how much of the candle has elapsed, enter in the direction of the
  trend so far (price above/below that candle's open) and close the
  position automatically right when the candle closes.

Runs on: Oracle Cloud VPS (systemd service: goldflow.service)
Talks to: MT5 account via MetaAPI (streaming quotes + trade execution)

NOTE: MetaAPI/MT5 gives the broker's own tick volume, not real
Depth-of-Market. "Volume-so-far" here is approximated by counting price
ticks received during the forming candle and comparing to what a
proportional share of the timeframe's average completed-candle volume
would predict. It is a heuristic, not real order-book data.

Env vars (.env, never committed):
    METAAPI_TOKEN=your_metaapi_token
    METAAPI_ACCOUNT_ID=your_mt5_account_id_in_metaapi
    SYMBOL=XAUUSD
    LOT_SIZE=0.01
    EMERGENCY_SL_DISTANCE_USD=5.0
    SCALP_VOLUME_MULTIPLIER=1.5
    MAX_DAILY_LOSS_USD=20.0
    SCALP_M1_ENABLED=true
    SCALP_H1_ENABLED=true
    DRY_RUN=true
"""

import asyncio
import os
import logging
from collections import deque
from datetime import datetime, timezone
from dotenv import load_dotenv
from metaapi_cloud_sdk import MetaApi, SynchronizationListener

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("goldflow")

METAAPI_TOKEN = os.environ["METAAPI_TOKEN"]
ACCOUNT_ID = os.environ["METAAPI_ACCOUNT_ID"]
SYMBOL = os.environ.get("SYMBOL", "XAUUSD")
DRY_RUN = os.environ.get("DRY_RUN", "true").lower() == "true"
LOT_SIZE = float(os.environ.get("LOT_SIZE", "0.01"))
EMERGENCY_SL_DISTANCE = float(os.environ.get("EMERGENCY_SL_DISTANCE_USD", "5.0"))
SCALP_VOLUME_MULTIPLIER = float(os.environ.get("SCALP_VOLUME_MULTIPLIER", "1.5"))
MAX_DAILY_LOSS_USD = float(os.environ.get("MAX_DAILY_LOSS_USD", "20.0"))
SCALP_M1_ENABLED = os.environ.get("SCALP_M1_ENABLED", "true").lower() == "true"
SCALP_H1_ENABLED = os.environ.get("SCALP_H1_ENABLED", "true").lower() == "true"


def parse_broker_time(ts: str) -> datetime:
    """Parses MetaAPI's ISO8601 broker/eventGenerated timestamps."""
    if ts.endswith("Z"):
        ts = ts[:-1] + "+00:00"
    return datetime.fromisoformat(ts)


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


class PartialCandleTracker:
    """Tracks the candle currently forming for one timeframe, tick by
    tick, so we can act in its final 25% without waiting for it to close.

    The volume baseline is built from this tracker's OWN completed tick
    counts (not MetaAPI's candle stream), since MetaAPI sends repeated
    "intermediate" updates for the still-forming candle — using those
    directly would contaminate the baseline with a constantly-rising
    partial count instead of clean historical data."""

    def __init__(self, label: str, timeframe_seconds: int, history_len: int = 50):
        self.label = label
        self.timeframe_seconds = timeframe_seconds
        self.entry_window_seconds = timeframe_seconds * 0.25  # last 25% of the candle
        self.history = deque(maxlen=history_len)
        self.candle_start_epoch = None
        self.open_price = None
        self.tick_count = 0
        self.entered_this_candle = False

    def _avg_history_volume(self) -> float:
        if len(self.history) < 5:
            return 0.0
        return sum(self.history) / len(self.history)

    def on_tick(self, price: float, ts: datetime):
        """Returns (seconds_remaining, should_consider_entry: bool)."""
        epoch = ts.timestamp()
        start_epoch = (epoch // self.timeframe_seconds) * self.timeframe_seconds

        if self.candle_start_epoch != start_epoch:
            if self.candle_start_epoch is not None:
                self.history.append(self.tick_count)
            self.candle_start_epoch = start_epoch
            self.open_price = price
            self.tick_count = 0
            self.entered_this_candle = False

        self.tick_count += 1
        elapsed = epoch - start_epoch
        remaining = self.timeframe_seconds - elapsed

        if self.entered_this_candle or remaining > self.entry_window_seconds:
            return remaining, False

        avg_volume = self._avg_history_volume()
        if avg_volume == 0:
            return remaining, False

        elapsed_fraction = elapsed / self.timeframe_seconds
        expected_ticks_so_far = elapsed_fraction * avg_volume

        if self.tick_count > expected_ticks_so_far * SCALP_VOLUME_MULTIPLIER:
            return remaining, True

        return remaining, False

    def trend_direction(self, current_price: float):
        if self.open_price is None:
            return None
        if current_price > self.open_price:
            return "buy"
        elif current_price < self.open_price:
            return "sell"
        return None

    def candle_close_epoch(self) -> float:
        return self.candle_start_epoch + self.timeframe_seconds

    def mark_entered(self):
        self.entered_this_candle = True


async def close_position_later(connection, position_id: str, delay_seconds: float, label: str):
    """Waits until the candle closes, then flattens the scalp position."""
    if delay_seconds > 0:
        await asyncio.sleep(delay_seconds)
    if DRY_RUN:
        log.info(f"[DRY_RUN] Would close {label} scalp position {position_id} now (candle closed)")
        return
    try:
        result = await connection.close_position(position_id, {"comment": "goldflow-scalp-exit"})
        log.info(f"Closed {label} scalp position {position_id}: {result}")
    except Exception as e:
        log.error(f"Failed to close {label} scalp position {position_id}: {e}")


async def enter_scalp(connection, tracker: PartialCandleTracker, side: str,
                       current_price: float, risk_manager: RiskManager):
    if not risk_manager.can_trade():
        log.warning(f"{tracker.label} scalp signal ({side.upper()}) but trading is halted today. Skipping.")
        return

    tracker.mark_entered()
    seconds_until_close = max(tracker.candle_close_epoch() - datetime.now(timezone.utc).timestamp(), 0)

    if side == "buy":
        emergency_sl = round(current_price - EMERGENCY_SL_DISTANCE, 2)
    else:
        emergency_sl = round(current_price + EMERGENCY_SL_DISTANCE, 2)

    if DRY_RUN:
        log.info(
            f"[DRY_RUN] {tracker.label} scalp: would {side.upper()} at ~{current_price} "
            f"(emergency SL={emergency_sl}, lot={LOT_SIZE}), "
            f"would auto-close in {seconds_until_close:.1f}s"
        )
        asyncio.create_task(close_position_later(connection, "dry-run", seconds_until_close, tracker.label))
        return

    try:
        if side == "buy":
            result = await connection.create_market_buy_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=emergency_sl, take_profit=None,
                options={"comment": f"goldflow-scalp-{tracker.label}"}
            )
        else:
            result = await connection.create_market_sell_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=emergency_sl, take_profit=None,
                options={"comment": f"goldflow-scalp-{tracker.label}"}
            )
        log.info(f"{tracker.label} scalp opened: {side.upper()} emergency_SL={emergency_sl} | Result: {result}")

        position_id = result.get("positionId") if isinstance(result, dict) else None
        if position_id:
            asyncio.create_task(close_position_later(connection, position_id, seconds_until_close, tracker.label))
        else:
            log.warning(f"No positionId returned for {tracker.label} scalp — cannot schedule auto-close. "
                        f"Emergency SL is still in place as a safety net.")
    except Exception as e:
        log.error(f"{tracker.label} scalp order failed: {e}")


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

    m1_tracker = PartialCandleTracker("M1", 60)
    h1_tracker = PartialCandleTracker("H1", 3600)
    risk_manager = RiskManager(max_daily_loss_usd=MAX_DAILY_LOSS_USD)

    try:
        account_info = connection.terminal_state.account_information
        if account_info and "equity" in account_info:
            risk_manager.update_equity(account_info["equity"])
    except Exception:
        pass

    class MainListener(SynchronizationListener):
        async def on_symbol_price_updated(self, instance_index, price):
            if price.get("symbol") != SYMBOL:
                return
            bid = price.get("bid")
            ask = price.get("ask")
            if bid is None or ask is None:
                return
            mid = (bid + ask) / 2
            ts = parse_broker_time(price["time"])

            if SCALP_M1_ENABLED:
                remaining, should_check = m1_tracker.on_tick(mid, ts)
                if should_check:
                    direction = m1_tracker.trend_direction(mid)
                    if direction:
                        log.info(f"M1 scalp signal: {direction.upper()} at {mid} ({remaining:.1f}s left in candle)")
                        await enter_scalp(connection, m1_tracker, direction, mid, risk_manager)

            if SCALP_H1_ENABLED:
                remaining, should_check = h1_tracker.on_tick(mid, ts)
                if should_check:
                    direction = h1_tracker.trend_direction(mid)
                    if direction:
                        log.info(f"H1 scalp signal: {direction.upper()} at {mid} ({remaining/60:.1f}min left in candle)")
                        await enter_scalp(connection, h1_tracker, direction, mid, risk_manager)

        async def on_account_information_updated(self, instance_index, account_information):
            equity = account_information.get("equity")
            if equity is not None:
                risk_manager.update_equity(equity)

    connection.add_synchronization_listener(MainListener())
    await connection.subscribe_to_market_data(
        SYMBOL, [{"type": "candles", "timeframe": "1m"}, {"type": "candles", "timeframe": "1h"}]
    )

    log.info(
        f"goldflow scalper running on {SYMBOL}, DRY_RUN={DRY_RUN}, lot={LOT_SIZE}, "
        f"M1_enabled={SCALP_M1_ENABLED}, H1_enabled={SCALP_H1_ENABLED}, "
        f"emergency_SL=${EMERGENCY_SL_DISTANCE}, max_daily_loss=${MAX_DAILY_LOSS_USD}"
    )
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
