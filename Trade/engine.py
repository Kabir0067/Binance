"""
Муҳаррики асосӣ — ҳамаи қисмҳоро ба ҳам мепайвандад ва системаро 24/7 идора мекунад.

Ҷараён:  MarketStream → MarketAnalyzer → тақсими сармоя → TradeExecutor → OrderWatcher → TelegramBot

• Ҳар соат, вақте ки шамъи 1h-и ҳамаи ашёҳо баста шуд (як «batch»):
    1) мавқеъҳои кушода барои баромади оморӣ санҷида мешаванд (тренди 4h муҳим поён + зарар > 5%);
    2) ҳамаи сигналҳои қавӣ ҷамъ мешаванд ва сармоя БАРОБАР тақсим мешавад:
         1 сигнал → як ашё, 2–4 сигнал → 2–4 қисми баробар (≤ RISK.max_position_pct ба як ашё).
• Ҳар вазифа таҳти supervisor: агар афтад — худкор бо backoff аз нав оғоз мешавад ва огоҳӣ меояд.
• Барқароркунӣ баъди рестарт: мавқеъҳо аз state.json ё аз ордерҳои кушодаи фурӯш дар биржа.
• Хомӯшшавии бехатар (SIGTERM / SIGINT): ҳолат сабт мешавад, ордерҳои фурӯш дар биржа кор мекунанд.

Оғоз:  python Trade/engine.py
"""

from __future__ import annotations

import asyncio
import signal
import sys
import time
from datetime import datetime, timezone
from typing import Awaitable, Callable

from analyzer import MarketAnalyzer, TradeSignal, tf_ms
from config import LIVE_TRADING, MARKET, QUOTE_ASSET, RISK, TIMING, get_logger, settings
from market_stream import MarketData, MarketStream
from order_watcher import ClosedTrade, ExitReason, OrderWatcher, StateStore
from telegram_bot import TelegramBot, fdur, fu
from trade_executor import BinanceAPIError, TradeExecutor

log = get_logger("engine")

_AUTH_ERROR_CODES = {-2014, -2015, -1022}
HOUR_MS = tf_ms(MARKET.tf_trigger)


class TradingEngine:
    def __init__(self) -> None:
        self.started_at = time.time()
        self.store = StateStore()
        self.market = MarketData()
        self.stream = MarketStream(self.market)
        self.executor = TradeExecutor(self.store, self.market, live=LIVE_TRADING)
        self.analyzer = MarketAnalyzer(self.market)
        self.bot = TelegramBot(self)
        self.watcher = OrderWatcher(self.executor, self.market, self.store, on_exit=self._on_exit, notify=self.bot.notify)
        self._stop: asyncio.Event | None = None
        self._tasks: list[asyncio.Task[None]] = []
        self._last_info_at: dict[str, float] = {}
        self._market_down_since: float | None = None
        self._market_alerted = False
        self.last_batch_at = 0.0

    # ═════════════════════════════ Ҳаёт ═════════════════════════════
    async def run(self) -> None:
        self._stop = asyncio.Event()
        loop = asyncio.get_running_loop()
        loop.set_exception_handler(self._loop_exception)
        for sig in (signal.SIGTERM, signal.SIGINT):
            try:
                loop.add_signal_handler(sig, self._stop.set)
            except (NotImplementedError, RuntimeError, ValueError):
                pass  # Windows

        if LIVE_TRADING and not settings.has_binance_keys:
            log.critical("LIVE_TRADING=True, вале BINANCE_API_KEY/SECRET дар .env нест — режими PAPER фаъол шуд")
            self.bot.notify("🚨 <b>Калидҳои Binance нест</b> — бот дар режими PAPER кор мекунад.")

        self._spawn("telegram", self.bot.run)
        if not await self._start_executor():
            await self._shutdown()
            return
        self.analyzer.fee_buy = self.executor.taker_fee
        self.analyzer.fee_sell = self.executor.maker_fee
        await self._recover()

        self._spawn("market_stream", self.stream.run_forever)
        self._spawn("order_watcher", self.watcher.watch_forever)
        self._spawn("entry_loop", self._entry_loop)
        self._spawn("housekeeping", self._housekeeping)

        equity = await self._safe_equity()
        mode = "🟢 LIVE (пули воқеӣ)" if self.executor.live else "🧪 PAPER (виртуалӣ)"
        self.bot.notify(
            f"🚀 <b>Бот оғоз шуд</b>\nРежим: <b>{mode}</b>\n"
            f"Сармоя: <b>{fu(equity)} {QUOTE_ASSET}</b> · Мавқеъҳо: {len(self.store.positions)}/{RISK.max_positions}\n"
            f"Комиссия: харид {self.executor.taker_fee * 100:.3f}% · фурӯш {self.executor.maker_fee * 100:.3f}%\n"
            f"Ашёҳо: {', '.join(s.replace(QUOTE_ASSET, '') for s in self.market.symbols)}"
        )
        log.info("Муҳаррик оғоз шуд: режим=%s, сармоя=%.2f, мавқеъҳо=%d", self.executor.mode, equity, len(self.store.positions))

        try:
            await self._stop.wait()
        finally:
            await self._shutdown()

    async def _start_executor(self) -> bool:
        """True — омода. False — ҳангоми интизорӣ фармони хомӯшшавӣ омад."""
        assert self._stop is not None
        delay = 5.0
        while not self._stop.is_set():
            try:
                await self.executor.start()
                return True
            except asyncio.CancelledError:
                raise
            except BinanceAPIError as exc:
                if exc.code in _AUTH_ERROR_CODES:
                    log.critical("API-калидҳои Binance нодуруст / IP иҷозат надорад: %s", exc)
                    self.bot.notify(f"🚨 <b>Калиди Binance рад шуд</b>: <code>{exc.msg}</code>\nКалид ё IP whitelist-ро санҷед.")
                    delay = 300.0
                else:
                    log.error("Оғози executor ноком: %s — такрор баъди %.0f сония", exc, delay)
            except Exception as exc:
                log.error("Оғози executor ноком: %r — такрор баъди %.0f сония", exc, delay)
            try:
                await asyncio.wait_for(self._stop.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass
            delay = min(delay * 2.0, 300.0)
        return False

    async def _recover(self) -> None:
        for symbol, pos in list(self.store.positions.items()):
            if pos.mode != self.executor.mode:
                # Режим иваз шуд (PAPER ↔ LIVE): мавқеи режими дигар бо ин режим идора карда намешавад.
                del self.store.positions[symbol]
                if pos.mode == "PAPER":
                    self.store.paper_balance += pos.cost_usdt
                log.critical("Мавқеи %s (%s) аз режими дигар ҷудо шуд; режими ҷорӣ: %s", symbol, pos.mode, self.executor.mode)
                extra = " Ордери фурӯши он дар Binance боқӣ мемонад — дастӣ санҷед." if pos.mode == "LIVE" else ""
                self.bot.notify(f"⚠️ Мавқеи <b>{symbol}</b> аз режими {pos.mode} назорат намешавад.{extra}")
        self.store.save()
        if self.store.positions:
            names = ", ".join(self.store.positions)
            log.info("Мавқеъҳо барқарор шуданд: %s", names)
            self.bot.notify(f"♻️ Мавқеъҳо барқарор шуданд: <b>{names}</b> — назорат идома дорад.")
        try:
            orphans = await self.executor.recover_orphans()
        except Exception as exc:
            log.error("Ҷустуҷӯи ордерҳои бесоҳиб ноком: %r", exc)
            return
        for pos in orphans:
            await self.watcher.open(pos)
            self.bot.notify(f"♻️ Мавқеи <b>{pos.symbol}</b> аз биржа барқарор шуд (ордер #{pos.sell_order_id}).")

    async def _shutdown(self) -> None:
        log.info("Хомӯшшавӣ…")
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.wait(self._tasks, timeout=TIMING.shutdown_timeout_sec)
        self.store.save()
        n = len(self.store.positions)
        note = f"\nМавқеъҳо ({n}) сабт шуданд; ордерҳои фурӯш дар биржа фаъоланд." if n and self.executor.live else (
            f"\nМавқеъҳо ({n}) сабт шуданд." if n else "")
        try:
            await asyncio.wait_for(self.bot.send_now(f"🛑 <b>Бот хомӯш шуд</b>{note}"), timeout=5.0)
        except Exception:
            pass
        await self.bot.close()
        await self.executor.close()
        log.info("Бот хомӯш шуд")

    def _spawn(self, name: str, factory: Callable[[], Awaitable[None]]) -> None:
        self._tasks.append(asyncio.create_task(self._supervise(name, factory), name=name))

    async def _supervise(self, name: str, factory: Callable[[], Awaitable[None]]) -> None:
        backoff = 1.0
        while True:
            started = time.monotonic()
            try:
                await factory()
                log.error("Вазифаи '%s' бе сабаб қатъ шуд — аз нав оғоз", name)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.critical("Вазифаи '%s' афтод: %r — аз нав оғоз баъди %.0f сония", name, exc, backoff, exc_info=True)
                if name != "telegram":
                    self.bot.notify(f"🚨 Модули <b>{name}</b> афтод ва аз нав оғоз мешавад:\n<code>{exc!r}</code>"[:500])
            if time.monotonic() - started > 300:
                backoff = 1.0
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2.0, 60.0)

    def _loop_exception(self, _loop: asyncio.AbstractEventLoop, context: dict) -> None:
        exc = context.get("exception")
        log.error("Хатои дастгирнашудаи asyncio: %s %r", context.get("message"), exc, exc_info=exc)

    # ═════════════════════════════ Batch-и соатӣ ════════════════════
    def trading_gate(self) -> tuple[bool, str]:
        if self.store.paused:
            return False, "таваққуфи дастӣ (/resume)"
        if not self.market.is_healthy:
            return False, "маълумоти бозор дастнорас"
        return True, ""

    def _batch_ready(self) -> int | None:
        """open_time-и шамъи навбасташудаи 1h (ms), агар batch омода бошад ва ҳанӯз коркард нашуда бошад."""
        now_ms = int(time.time() * 1000)
        hour_open = now_ms // HOUR_MS * HOUR_MS - HOUR_MS   # шамъе ки ҳозир бояд баста шуда бошад
        if hour_open <= self.store.last_batch_hour:
            return None
        closed = [self.analyzer.last_closed_hour(s) >= hour_open for s in self.market.symbols]
        if all(closed) or (now_ms - (hour_open + HOUR_MS) >= TIMING.batch_grace_sec * 1000 and any(closed)):
            return hour_open
        return None

    async def _entry_loop(self) -> None:
        await self.market.ready.wait()
        log.info("Маълумоти бозор омода — таҳлили соатӣ фаъол шуд")
        while True:
            await asyncio.sleep(TIMING.entry_loop_sec)
            hour_open = self._batch_ready()
            if hour_open is None:
                continue
            self.store.last_batch_hour = hour_open
            self.store.save()
            self.last_batch_at = time.time()
            age_sec = time.time() - (hour_open + HOUR_MS) / 1000.0
            try:
                self._check_trend_exits()
                if age_sec <= TIMING.batch_max_age_sec:
                    await self._enter_batch()
                else:
                    log.info("Batch-и %s кӯҳна аст (%.0f сония) — вуруд то соати навбатӣ интизор мешавад",
                             datetime.fromtimestamp(hour_open / 1000, timezone.utc).strftime("%H:%M"), age_sec)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("Хатои batch-и соатӣ: %r", exc, exc_info=True)

    def _check_trend_exits(self) -> None:
        for symbol, pos in list(self.store.positions.items()):
            reason = self.analyzer.trend_exit(symbol, pos.entry_price)
            if reason:
                log.warning("Баромади оморӣ %s: %s", symbol, reason)
                self.bot.notify(f"📉 <b>{symbol}</b>: {reason} — мавқеъ баста мешавад, то сармоя дармонда нашавад.")
                self.watcher.request_close(symbol, ExitReason.TREND_EXIT)

    async def _enter_batch(self) -> None:
        allowed, why = self.trading_gate()
        if not allowed:
            self._info_once("gate", 3600, "Вуруди нав манъ: %s", why)
            return
        slots = RISK.max_positions - len(self.store.positions)
        if slots <= 0:
            return
        exclude = set(self.store.positions) | self.store.active_cooldowns()
        exclude |= {s for s in self.market.symbols if not self.executor.can_trade(s)}
        signals = self.analyzer.signals(exclude=exclude)[:slots]
        if not signals:
            return
        try:
            free = await self.executor.free_quote(force=True)
            equity = await self.executor.equity()
        except Exception as exc:
            log.error("Баланс гирифта нашуд — вуруд бекор: %r", exc)
            return

        per_trade = min(free * RISK.capital_usage_pct / 100.0 / len(signals), equity * RISK.max_position_pct / 100.0)
        if per_trade < RISK.min_trade_usdt:
            self._info_once("low_balance", 3600, "Баланс барои савдо кам: озод %.2f %s", free, QUOTE_ASSET)
            return
        log.info("Batch: %d сигнал (%s), ба ҳар кадом %.2f %s", len(signals),
                 ", ".join(s.symbol for s in signals), per_trade, QUOTE_ASSET)
        for sig in signals:
            await self._enter(sig, per_trade)

    async def _enter(self, sig: TradeSignal, amount: float) -> None:
        live_sig = self.analyzer.assess(sig.symbol).signal   # бо нархи навтарин аз нав санҷида мешавад
        if live_sig is None:
            log.info("%s: то лаҳзаи вуруд сигнал эътибор надорад — вуруд нест", sig.symbol)
            return
        pos = await self.executor.open_position(live_sig, amount)
        if pos is None:
            self.store.cooldowns[sig.symbol] = time.time() + RISK.entry_fail_cooldown_min * 60.0
            self.store.save()
            return
        await self.watcher.open(pos)
        self.bot.notify_buy(pos, live_sig)

    def _info_once(self, key: str, every_sec: float, msg: str, *args: object) -> None:
        now = time.monotonic()
        if now - self._last_info_at.get(key, -1e18) >= every_sec:
            self._last_info_at[key] = now
            log.info(msg, *args)

    async def _on_exit(self, trade: ClosedTrade) -> None:
        self.bot.notify_exit(trade)

    # ═════════════════════════════ Идора аз Telegram ════════════════
    def pause(self) -> None:
        self.store.paused = True
        self.store.save()
        log.info("Таваққуфи дастӣ фаъол шуд")

    def resume(self) -> None:
        self.store.paused = False
        self.store.save()
        log.info("Савдо аз нав фаъол шуд")

    def request_close(self, symbol: str | None = None) -> int:
        return self.watcher.request_close(symbol, ExitReason.MANUAL)

    # ═════════════════════════════ Хизматрасонӣ ═════════════════════
    async def _safe_equity(self) -> float:
        try:
            return await self.executor.equity()
        except Exception as exc:
            log.error("Ҳисоби сармоя ноком: %r", exc)
            return 0.0

    async def _housekeeping(self) -> None:
        last_time_sync = last_rules = time.monotonic()
        while True:
            await asyncio.sleep(TIMING.housekeeping_sec)
            now = time.monotonic()
            try:
                await self._roll_day()
                self.store.prune_cooldowns()
                self._check_market_health()
                if now - last_time_sync >= TIMING.time_sync_sec:
                    last_time_sync = now
                    await self.executor.client.sync_time()
                if now - last_rules >= TIMING.rules_refresh_sec:
                    last_rules = now
                    await self.executor.load_rules()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.error("Хатои хизматрасонӣ: %r", exc, exc_info=True)

    async def _roll_day(self) -> None:
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        if self.store.day == today:
            return
        previous = self.store.day
        self.store.day = today
        self.store.day_start_equity = await self._safe_equity()
        self.store.save()
        if previous:
            start = utc_midnight() - 86400.0
            day_trades = [t for t in self.store.trades if start <= t.closed_at < start + 86400.0]
            pnl = sum(t.pnl_usdt for t in day_trades)
            self.bot.notify(
                f"🗓 <b>Ҳисоботи рӯз ({previous})</b>\n"
                f"Савдоҳо: {len(day_trades)} · бо фоида: {sum(1 for t in day_trades if t.pnl_usdt > 0)}\n"
                f"PnL: <b>{pnl:+.2f} {QUOTE_ASSET}</b>\n"
                f"Сармоя: <b>{fu(self.store.day_start_equity)} {QUOTE_ASSET}</b> · "
                f"Мавқеъҳои кушода: {len(self.store.positions)} · Ҳамагӣ PnL: {self.store.stats()['pnl']:+.2f}"
            )

    def _check_market_health(self) -> None:
        if self.market.is_healthy:
            if self._market_alerted:
                self.bot.notify("✅ Маълумоти бозор барқарор шуд.")
                log.info("Маълумоти бозор барқарор шуд")
            self._market_down_since = None
            self._market_alerted = False
            return
        now = time.monotonic()
        if self._market_down_since is None:
            self._market_down_since = now
        elif not self._market_alerted and now - self._market_down_since >= TIMING.market_down_alert_sec:
            self._market_alerted = True
            down = fdur(now - self._market_down_since)
            log.error("Маълумоти бозор %s боз қатъ аст", down)
            self.bot.notify(f"⚠️ Маълумоти бозор {down} боз қатъ аст. Вуруди нав манъ; ордерҳои фурӯш дар биржа фаъоланд.")


def utc_midnight() -> float:
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def main() -> None:
    engine = TradingEngine()
    try:
        asyncio.run(engine.run())
    except KeyboardInterrupt:
        pass
    except Exception as exc:
        log.critical("Хатои марговари муҳаррик: %r", exc, exc_info=True)
        sys.exit(1)


if __name__ == "__main__":
    main()
