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
PATTERN_STRATEGY_ENABLED = os.environ.get("PATTERN_STRATEGY_ENABLED", "true").lower() == "true"
PATTERN_SL_DISTANCE = float(os.environ.get("PATTERN_SL_DISTANCE_USD", "3.0"))
PATTERN_TP_DISTANCE = float(os.environ.get("PATTERN_TP_DISTANCE_USD", "6.0"))
TEST_TRADE_ENABLED = os.environ.get("TEST_TRADE_ENABLED", "false").lower() == "true"
LONDON_BREAKOUT_ENABLED = os.environ.get("LONDON_BREAKOUT_ENABLED", "true").lower() == "true"
ASIAN_SESSION_START_HOUR = int(os.environ.get("ASIAN_SESSION_START_HOUR", "0"))
ASIAN_SESSION_END_HOUR = int(os.environ.get("ASIAN_SESSION_END_HOUR", "6"))
LONDON_SESSION_START_HOUR = int(os.environ.get("LONDON_SESSION_START_HOUR", "7"))
LONDON_SESSION_END_HOUR = int(os.environ.get("LONDON_SESSION_END_HOUR", "16"))
LONDON_SWEEP_BUFFER = float(os.environ.get("LONDON_SWEEP_BUFFER_USD", "0.5"))
LONDON_RR_RATIO = float(os.environ.get("LONDON_RR_RATIO", "2.0"))
LONDON_MAX_TRADES_PER_DAY = int(os.environ.get("LONDON_MAX_TRADES_PER_DAY", "1"))


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
        self.window_start_ticks = None
        self.window_start_epoch = None

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
            self.window_start_ticks = None
            self.window_start_epoch = None

        self.tick_count += 1
        elapsed = epoch - start_epoch
        remaining = self.timeframe_seconds - elapsed

        # Mark the moment we cross into the final 25% of the candle, so we
        # can measure volume RATE from that point forward only.
        if remaining <= self.entry_window_seconds and self.window_start_ticks is None:
            self.window_start_ticks = self.tick_count - 1
            self.window_start_epoch = epoch

        if self.entered_this_candle or remaining > self.entry_window_seconds:
            return remaining, False

        avg_volume = self._avg_history_volume()
        if avg_volume == 0 or self.window_start_ticks is None:
            return remaining, False

        window_ticks = self.tick_count - self.window_start_ticks
        window_elapsed = epoch - self.window_start_epoch
        if window_elapsed <= 0 or window_ticks < 3:
            # Require a few ticks in-window before judging rate, so the
            # very first tick after crossing into the window can't trigger
            # on its own (that would fire on every single candle).
            return remaining, False

        expected_rate_per_sec = avg_volume / self.timeframe_seconds
        expected_window_ticks = expected_rate_per_sec * window_elapsed

        if expected_window_ticks > 0 and window_ticks > expected_window_ticks * SCALP_VOLUME_MULTIPLIER:
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


class CandlePatternTracker:
    """Builds completed M1 candles from ticks (self-tracked, not trusting
    MetaAPI's intermediate candle stream) and checks for three consecutive
    same-direction candles with rising/falling closes — a much more common
    setup, used here mainly to confirm the execution pipeline fires."""

    def __init__(self, timeframe_seconds: int, history_len: int = 10):
        self.timeframe_seconds = timeframe_seconds
        self.candle_start_epoch = None
        self.open = None
        self.high = None
        self.low = None
        self.close = None
        self.completed = deque(maxlen=history_len)

    def on_tick(self, price: float, ts: datetime) -> bool:
        """Returns True the moment a candle has just completed."""
        epoch = ts.timestamp()
        start_epoch = (epoch // self.timeframe_seconds) * self.timeframe_seconds
        rolled_over = False

        if self.candle_start_epoch != start_epoch:
            if self.candle_start_epoch is not None and self.open is not None:
                self.completed.append({
                    "open": self.open, "high": self.high,
                    "low": self.low, "close": self.close,
                })
                rolled_over = True
            self.candle_start_epoch = start_epoch
            self.open = price
            self.high = price
            self.low = price

        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        return rolled_over

    def check_pattern(self):
        if len(self.completed) < 3:
            return None
        last3 = list(self.completed)[-3:]
        bullish = (
            all(c["close"] > c["open"] for c in last3)
            and last3[0]["close"] < last3[1]["close"] < last3[2]["close"]
        )
        bearish = (
            all(c["close"] < c["open"] for c in last3)
            and last3[0]["close"] > last3[1]["close"] > last3[2]["close"]
        )
        if bullish:
            return "buy"
        elif bearish:
            return "sell"
        return None


class LondonBreakoutTracker:
    """Asian-range liquidity sweep + structure-break confirmation, ported
    from the Elite London Breakout AI strategy's core mechanic:
      1. Mark the Asian session's high/low, lock it once Asian hours end.
      2. During London/NY hours, watch for a SWEEP: price pokes beyond
         that range (a likely stop-hunt), not a clean sustained break.
      3. Require the very next completed M1 candle to close back INSIDE
         the range (confirms it was a wick, not a real breakout) AND
         to break the prior candle's opposite extreme (a simple BOS) —
         only then is a reversal trade confirmed.
      4. If price closes beyond the sweep extreme instead of reverting,
         that invalidates the fade setup for the day (this module does
         not chase continuation breakouts — that's a different, untested
         strategy shape)."""

    def __init__(self):
        self.current_date = None
        self.asian_high = None
        self.asian_low = None
        self.asian_locked = False
        self.state = "waiting_range"
        self.sweep_extreme = None
        self.sweep_direction = None
        self.trades_today = 0
        self.prev_m1_low = None
        self.prev_m1_high = None

    def _reset_for_new_day(self, date):
        self.current_date = date
        self.asian_high = None
        self.asian_low = None
        self.asian_locked = False
        self.state = "waiting_range"
        self.sweep_extreme = None
        self.sweep_direction = None
        self.trades_today = 0

    def on_tick(self, price: float, ts: datetime):
        date = ts.date()
        if self.current_date != date:
            self._reset_for_new_day(date)

        hour = ts.hour
        in_asian = ASIAN_SESSION_START_HOUR <= hour < ASIAN_SESSION_END_HOUR
        in_london_ny = LONDON_SESSION_START_HOUR <= hour < LONDON_SESSION_END_HOUR

        if in_asian:
            self.asian_high = price if self.asian_high is None else max(self.asian_high, price)
            self.asian_low = price if self.asian_low is None else min(self.asian_low, price)
            return

        if not self.asian_locked and self.asian_high is not None:
            self.asian_locked = True
            self.state = "range_locked"
            log.info(f"Asian range locked: high={self.asian_high} low={self.asian_low}")

        if not self.asian_locked or not in_london_ny or self.trades_today >= LONDON_MAX_TRADES_PER_DAY:
            return

        if self.state == "range_locked":
            if price > self.asian_high + LONDON_SWEEP_BUFFER:
                self.state = "swept_high"
                self.sweep_extreme = price
                self.sweep_direction = "high"
                log.info(f"London sweep of Asian HIGH detected at {price} (range high {self.asian_high})")
            elif price < self.asian_low - LONDON_SWEEP_BUFFER:
                self.state = "swept_low"
                self.sweep_extreme = price
                self.sweep_direction = "low"
                log.info(f"London sweep of Asian LOW detected at {price} (range low {self.asian_low})")
        elif self.state == "swept_high" and price > self.sweep_extreme:
            self.sweep_extreme = price
        elif self.state == "swept_low" and price < self.sweep_extreme:
            self.sweep_extreme = price

    def on_m1_candle_closed(self, candle):
        """Call this each time an M1 candle completes. Returns 'buy',
        'sell', or None."""
        signal = None

        if self.state == "swept_high":
            if candle["close"] < self.asian_high:
                if self.prev_m1_low is not None and candle["low"] < self.prev_m1_low:
                    signal = "sell"
                    self.state = "done_today"
                    self.trades_today += 1
            elif candle["close"] > self.sweep_extreme:
                log.info("London sweep-high setup invalidated (price closed beyond the sweep — real breakout, not a fade).")
                self.state = "done_today"
        elif self.state == "swept_low":
            if candle["close"] > self.asian_low:
                if self.prev_m1_high is not None and candle["high"] > self.prev_m1_high:
                    signal = "buy"
                    self.state = "done_today"
                    self.trades_today += 1
            elif candle["close"] < self.sweep_extreme:
                log.info("London sweep-low setup invalidated (price closed beyond the sweep — real breakout, not a fade).")
                self.state = "done_today"

        self.prev_m1_low = candle["low"]
        self.prev_m1_high = candle["high"]
        return signal


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


async def enter_pattern_trade(connection, side: str, current_price: float, risk_manager: RiskManager):
    if not risk_manager.can_trade():
        log.warning(f"Pattern signal ({side.upper()}) but trading is halted today. Skipping.")
        return

    if side == "buy":
        sl = round(current_price - PATTERN_SL_DISTANCE, 2)
        tp = round(current_price + PATTERN_TP_DISTANCE, 2)
    else:
        sl = round(current_price + PATTERN_SL_DISTANCE, 2)
        tp = round(current_price - PATTERN_TP_DISTANCE, 2)

    if DRY_RUN:
        log.info(
            f"[DRY_RUN] Pattern (3 soldiers): would {side.upper()} on {SYMBOL} at ~{current_price} "
            f"| SL={sl} TP={tp} | lot={LOT_SIZE}"
        )
        return

    try:
        if side == "buy":
            result = await connection.create_market_buy_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                options={"comment": "goldflow-pattern"}
            )
        else:
            result = await connection.create_market_sell_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                options={"comment": "goldflow-pattern"}
            )
        log.info(f"Pattern trade opened: {side.upper()} SL={sl} TP={tp} | Result: {result}")
    except Exception as e:
        log.error(f"Pattern trade order failed: {e}")


async def enter_london_trade(connection, side: str, current_price: float,
                              sweep_extreme: float, risk_manager: RiskManager):
    if not risk_manager.can_trade():
        log.warning(f"London breakout signal ({side.upper()}) but trading is halted today. Skipping.")
        return

    if side == "sell":
        sl = round(sweep_extreme + LONDON_SWEEP_BUFFER, 2)
        risk_dist = sl - current_price
        tp = round(current_price - risk_dist * LONDON_RR_RATIO, 2)
    else:
        sl = round(sweep_extreme - LONDON_SWEEP_BUFFER, 2)
        risk_dist = current_price - sl
        tp = round(current_price + risk_dist * LONDON_RR_RATIO, 2)

    if DRY_RUN:
        log.info(
            f"[DRY_RUN] London breakout: would {side.upper()} on {SYMBOL} at ~{current_price} "
            f"| SL={sl} TP={tp} (R:R {LONDON_RR_RATIO}) | lot={LOT_SIZE}"
        )
        return

    try:
        if side == "buy":
            result = await connection.create_market_buy_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                options={"comment": "goldflow-london"}
            )
        else:
            result = await connection.create_market_sell_order(
                SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                options={"comment": "goldflow-london"}
            )
        log.info(f"London breakout trade opened: {side.upper()} SL={sl} TP={tp} | Result: {result}")
    except Exception as e:
        log.error(f"London breakout order failed: {e}")


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
    pattern_tracker = CandlePatternTracker(60)
    london_tracker = LondonBreakoutTracker()
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

            if PATTERN_STRATEGY_ENABLED or LONDON_BREAKOUT_ENABLED:
                rolled_over = pattern_tracker.on_tick(mid, ts)
                if rolled_over:
                    if PATTERN_STRATEGY_ENABLED:
                        direction = pattern_tracker.check_pattern()
                        if direction:
                            log.info(f"Pattern signal (3 soldiers): {direction.upper()} at {mid}")
                            await enter_pattern_trade(connection, direction, mid, risk_manager)

                    if LONDON_BREAKOUT_ENABLED and len(pattern_tracker.completed) > 0:
                        london_signal = london_tracker.on_m1_candle_closed(pattern_tracker.completed[-1])
                        if london_signal:
                            log.info(f"London breakout signal: {london_signal.upper()} at {mid} "
                                      f"(sweep extreme {london_tracker.sweep_extreme})")
                            await enter_london_trade(connection, london_signal, mid,
                                                      london_tracker.sweep_extreme, risk_manager)

            if LONDON_BREAKOUT_ENABLED:
                london_tracker.on_tick(mid, ts)

        async def on_account_information_updated(self, instance_index, account_information):
            equity = account_information.get("equity")
            if equity is not None:
                risk_manager.update_equity(equity)

    connection.add_synchronization_listener(MainListener())
    await connection.subscribe_to_market_data(
        SYMBOL, [{"type": "candles", "timeframe": "1m"}, {"type": "candles", "timeframe": "1h"}]
    )

    if TEST_TRADE_ENABLED:
        log.info("TEST_TRADE_ENABLED is on — firing one no-logic confirmation trade in 5s...")
        await asyncio.sleep(5)
        try:
            price_info = connection.terminal_state.price(SYMBOL)
            test_price = price_info["bid"] if price_info else None
        except Exception:
            test_price = None

        if test_price is None:
            log.warning("No price available yet for test trade — skipping. Try again once price data is flowing.")
        else:
            sl = round(test_price - PATTERN_SL_DISTANCE, 2)
            tp = round(test_price + PATTERN_TP_DISTANCE, 2)
            if DRY_RUN:
                log.info(f"[DRY_RUN] TEST TRADE: would BUY {SYMBOL} at ~{test_price} | SL={sl} TP={tp} | lot={LOT_SIZE}")
            else:
                try:
                    result = await connection.create_market_buy_order(
                        SYMBOL, volume=LOT_SIZE, stop_loss=sl, take_profit=tp,
                        options={"comment": "goldflow-test-trade"}
                    )
                    log.info(f"TEST TRADE placed: BUY at ~{test_price} SL={sl} TP={tp} | Result: {result}")
                except Exception as e:
                    log.error(f"TEST TRADE failed: {e}")

    log.info(
        f"goldflow scalper running on {SYMBOL}, DRY_RUN={DRY_RUN}, lot={LOT_SIZE}, "
        f"M1_enabled={SCALP_M1_ENABLED}, H1_enabled={SCALP_H1_ENABLED}, "
        f"emergency_SL=${EMERGENCY_SL_DISTANCE}, max_daily_loss=${MAX_DAILY_LOSS_USD}"
    )
    while True:
        await asyncio.sleep(60)


if __name__ == "__main__":
    asyncio.run(main())
