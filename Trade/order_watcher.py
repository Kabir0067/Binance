"""
Назоратчии мавқеъҳо ва журнали савдо.

• То RISK.max_positions мавқеъ ҳамзамон (як мавқеъ ба як ашё).
• Мавқеъҳо, таърихи савдоҳо ва ҳолати бот дар Data/state.json (навиштани атомарӣ) —
  баъд аз ҳар рестарт бот аз ҳамон ҷо идома медиҳад.
• Баромад:
    – TAKE_PROFIT: LIMIT-фурӯш дар ҳадаф (дар LIVE — ордери GTC дар худи биржа, ҳатто вақте ки бот хомӯш аст);
    – TREND_EXIT: танҳо вақте ки таҳлилгар тренди поёнравии аз ҷиҳати оморӣ муҳимро тасдиқ кунад;
    – MANUAL: аз Telegram.
  Дар поёнравии муваққатӣ мавқеъ НАМЕФУРӮШАД — интизор мешавад.
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from dataclasses import asdict, dataclass, fields
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Awaitable, Callable

from config import PAPER_START_BALANCE, STATE_FILE, TIMING, get_logger

if TYPE_CHECKING:
    from market_stream import MarketData, Ticker
    from trade_executor import TradeExecutor

log = get_logger("order_watcher")


class ExitReason(str, Enum):
    TAKE_PROFIT = "TAKE_PROFIT"
    TREND_EXIT = "TREND_EXIT"
    MANUAL = "MANUAL"
    EXTERNAL = "EXTERNAL"


def _from_dict(cls, data: dict[str, Any]):
    known = {f.name for f in fields(cls)}
    return cls(**{k: v for k, v in data.items() if k in known})


@dataclass
class Position:
    symbol: str
    mode: str                   # "LIVE" | "PAPER"
    qty: float                  # миқдори соф дар даст баъди комиссияи харид
    entry_price: float          # нархи миёнаи харид
    cost_usdt: float            # USDT-и сарфшуда (бо комиссия)
    entry_fee_usdt: float
    target_price: float         # нархи LIMIT-фурӯш
    net_target_pct: float       # фоидаи софи нақшавӣ
    opened_at: float            # epoch, сония
    hit_prob: float = 0.0
    sell_order_id: int | None = None
    sold_qty: float = 0.0       # аллакай фурӯхташуда (иҷрои қисмӣ)
    sold_quote: float = 0.0
    sold_fee_usdt: float = 0.0
    place_failures: int = 0
    min_price: float = 0.0      # пасттарин нарх дар давоми мавқеъ (барои омор)
    score: float = 0.0
    notes: str = ""

    @property
    def remaining_qty(self) -> float:
        return max(self.qty - self.sold_qty, 0.0)

    @property
    def held_sec(self) -> float:
        return time.time() - self.opened_at

    def value(self, price: float) -> float:
        return self.remaining_qty * price + self.sold_quote - self.sold_fee_usdt

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "Position":
        return _from_dict(cls, data)


@dataclass(frozen=True)
class ExitFill:
    price: float               # нархи миёнаи фурӯш (бо иҷроҳои қисмӣ)
    qty: float
    proceeds_usdt: float       # USDT-и гирифташуда баъди комиссия
    fee_usdt: float
    reason: ExitReason


@dataclass
class ClosedTrade:
    symbol: str
    mode: str
    qty: float
    entry_price: float
    exit_price: float
    cost_usdt: float
    proceeds_usdt: float
    pnl_usdt: float
    pnl_pct: float
    fees_usdt: float
    reason: str
    opened_at: float
    closed_at: float
    max_drawdown_pct: float = 0.0

    @property
    def hold_hours(self) -> float:
        return (self.closed_at - self.opened_at) / 3600.0

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> "ClosedTrade":
        return _from_dict(cls, data)


# ════════════════════════════════ Ҳолат (JSON) ════════════════════════════════
class StateStore:
    MAX_TRADES = 5000

    def __init__(self, path: Path = STATE_FILE):
        self.path = path
        self.positions: dict[str, Position] = {}
        self.trades: list[ClosedTrade] = []
        self.paused = False
        self.cooldowns: dict[str, float] = {}
        self.paper_balance = PAPER_START_BALANCE
        self.day = ""
        self.day_start_equity = 0.0
        self.last_batch_hour = 0
        self.load()

    def load(self) -> None:
        if not self.path.exists():
            return
        try:
            data = json.loads(self.path.read_text(encoding="utf-8"))
            raw = data.get("positions") or {}
            if not raw and data.get("position"):        # формати кӯҳна (як мавқеъ)
                old = data["position"]
                raw = {old["symbol"]: old}
            loaded: dict[str, Position] = {}
            for sym, p in raw.items():
                try:
                    loaded[sym] = Position.from_dict(p)
                except TypeError as exc:
                    log.critical("Мавқеи %s аз файли ҳолат хонда нашуд (%r) — дастӣ санҷед: %s", sym, exc, p)
            self.positions = loaded
            self.trades = []
            for t in data.get("trades", []):
                try:
                    self.trades.append(ClosedTrade.from_dict(t))
                except TypeError:
                    continue
            self.paused = bool(data.get("paused", False))
            self.cooldowns = {k: float(v) for k, v in data.get("cooldowns", {}).items()}
            self.paper_balance = float(data.get("paper_balance", PAPER_START_BALANCE))
            self.day = str(data.get("day", ""))
            self.day_start_equity = float(data.get("day_start_equity", 0.0))
            self.last_batch_hour = int(data.get("last_batch_hour", 0))
        except Exception as exc:
            backup = self.path.with_suffix(f".corrupt-{int(time.time())}.json")
            try:
                self.path.replace(backup)
            except OSError:
                pass
            log.critical("Файли ҳолат вайрон аст (%r) — нусха: %s. Бот бо ҳолати нав оғоз мешавад.", exc, backup.name)

    def save(self) -> None:
        data = {
            "positions": {s: p.to_dict() for s, p in self.positions.items()},
            "trades": [asdict(t) for t in self.trades[-self.MAX_TRADES:]],
            "paused": self.paused,
            "cooldowns": self.cooldowns,
            "paper_balance": round(self.paper_balance, 8),
            "day": self.day,
            "day_start_equity": self.day_start_equity,
            "last_batch_hour": self.last_batch_hour,
            "saved_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        }
        tmp = self.path.with_suffix(".tmp")
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            with open(tmp, "w", encoding="utf-8") as fh:
                json.dump(data, fh, ensure_ascii=False, indent=1)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp, self.path)
        except OSError as exc:
            log.critical("Ҳолат сабт нашуд (%s): %r", self.path, exc)

    # ─────────────────────────────── Омор ───────────────────────────────
    def record_trade(self, trade: ClosedTrade) -> None:
        self.trades.append(trade)
        if len(self.trades) > self.MAX_TRADES:
            self.trades = self.trades[-self.MAX_TRADES:]
        self.save()

    def pnl_since(self, ts: float) -> tuple[float, int]:
        sel = [t for t in self.trades if t.closed_at >= ts]
        return sum(t.pnl_usdt for t in sel), len(sel)

    def stats(self, since: float = 0.0) -> dict[str, float]:
        sel = [t for t in self.trades if t.closed_at >= since]
        wins = [t for t in sel if t.pnl_usdt > 0]
        n = len(sel)
        gross_win = sum(t.pnl_usdt for t in wins)
        gross_loss = -sum(t.pnl_usdt for t in sel if t.pnl_usdt <= 0)
        return {
            "trades": n,
            "wins": len(wins),
            "win_rate": len(wins) / n * 100.0 if n else 0.0,
            "pnl": gross_win - gross_loss,
            "fees": sum(t.fees_usdt for t in sel),
            "avg_pct": sum(t.pnl_pct for t in sel) / n if n else 0.0,
            "avg_hold_h": sum(t.hold_hours for t in sel) / n if n else 0.0,
            "best": max((t.pnl_usdt for t in sel), default=0.0),
            "worst": min((t.pnl_usdt for t in sel), default=0.0),
        }

    def active_cooldowns(self) -> set[str]:
        now = time.time()
        return {s for s, until in self.cooldowns.items() if until > now}

    def prune_cooldowns(self) -> None:
        now = time.time()
        expired = [s for s, until in self.cooldowns.items() if until <= now]
        for s in expired:
            del self.cooldowns[s]
        if expired:
            self.save()


ExitHandler = Callable[[ClosedTrade], Awaitable[None]]
Notifier = Callable[[str], None]


# ════════════════════════════════ Назоратчӣ ═══════════════════════════════════
class OrderWatcher:
    def __init__(
        self,
        executor: "TradeExecutor",
        market: "MarketData",
        store: StateStore,
        on_exit: ExitHandler,
        notify: Notifier,
    ):
        self.executor = executor
        self.market = market
        self.store = store
        self.on_exit = on_exit
        self.notify = notify
        self._lock = asyncio.Lock()
        self._close_requests: dict[str, ExitReason] = {}
        self._last_sync: dict[str, float] = {}
        self._stale_warned: set[str] = set()

    @property
    def positions(self) -> dict[str, Position]:
        return self.store.positions

    async def open(self, position: Position) -> None:
        async with self._lock:
            existing = self.store.positions.get(position.symbol)
            if existing is not None and existing is not position:
                log.critical("Мантиқ: мавқеи дуюм барои %s — мавқеи нав сабт нашуд", position.symbol)
                return
            position.min_price = position.min_price or position.entry_price
            self.store.positions[position.symbol] = position
            self.store.save()
            self._last_sync[position.symbol] = 0.0

    def request_close(self, symbol: str | None, reason: ExitReason = ExitReason.MANUAL) -> int:
        """Дархости бастан (symbol=None — ҳамаи мавқеъҳо). Бармегардонад шумораи мавқеъҳо."""
        targets = list(self.store.positions) if symbol is None else [s for s in (symbol,) if s in self.store.positions]
        for s in targets:
            self._close_requests[s] = reason
        return len(targets)

    # ───────────────────────────── Цикл ─────────────────────────────
    async def watch_forever(self) -> None:
        while True:
            for symbol in list(self.store.positions):
                try:
                    await self._tick(symbol)
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.error("Хатои техникӣ дар назорати %s: %r", symbol, exc, exc_info=True)
                    await asyncio.sleep(2.0)
            await asyncio.sleep(TIMING.watch_loop_sec)

    async def _tick(self, symbol: str) -> None:
        async with self._lock:
            pos = self.store.positions.get(symbol)
            if pos is None:
                return
            ticker = self.market.ticker(symbol)
            self._check_staleness(pos, ticker)
            if ticker is not None and (pos.min_price <= 0 or ticker.price < pos.min_price):
                pos.min_price = ticker.price

            reason = self._close_requests.pop(symbol, None)
            if reason is not None:
                fill = await self.executor.close_position(pos, reason, ticker)
                if fill is None:
                    self._close_requests[symbol] = reason
                    log.error("Бастани %s (%s) ноком шуд — кӯшиши навбатӣ", symbol, reason.value)
                    self.notify(f"⚠️ Бастани <b>{symbol}</b> ноком шуд ({reason.value}). Кӯшиши такрорӣ…")
                    await asyncio.sleep(3.0)
                    return
                await self._finalize(pos, fill)
                return

            if self._sync_due(pos, ticker):
                self._last_sync[symbol] = time.monotonic()
                fill = await self.executor.sync_position(pos, ticker)
                if fill is not None:
                    await self._finalize(pos, fill)

    def _sync_due(self, pos: Position, ticker: "Ticker | None") -> bool:
        if not self.executor.live:
            return True  # симулятсия арзон аст — ҳар тик
        if pos.sell_order_id is None:
            return time.monotonic() - self._last_sync.get(pos.symbol, 0.0) >= (
                1.0 if pos.place_failures == 0 else TIMING.sell_retry_sec
            )
        elapsed = time.monotonic() - self._last_sync.get(pos.symbol, 0.0)
        if elapsed >= TIMING.exchange_sync_sec:
            return True
        return ticker is not None and elapsed >= 1.0 and ticker.price >= pos.target_price * 0.999

    def _check_staleness(self, pos: Position, ticker: "Ticker | None") -> None:
        if ticker is None and pos.symbol not in self._stale_warned:
            self._stale_warned.add(pos.symbol)
            where = "ордери фурӯш дар биржа фаъол аст" if pos.sell_order_id else "ордери фурӯш ҳанӯз нест"
            log.warning("Нархи зиндаи %s дастнорас — %s", pos.symbol, where)
        elif ticker is not None:
            self._stale_warned.discard(pos.symbol)

    async def _finalize(self, pos: Position, fill: ExitFill) -> None:
        pnl = fill.proceeds_usdt - pos.cost_usdt
        trade = ClosedTrade(
            symbol=pos.symbol,
            mode=pos.mode,
            qty=fill.qty,
            entry_price=pos.entry_price,
            exit_price=fill.price,
            cost_usdt=pos.cost_usdt,
            proceeds_usdt=fill.proceeds_usdt,
            pnl_usdt=pnl,
            pnl_pct=pnl / pos.cost_usdt * 100.0 if pos.cost_usdt > 0 else 0.0,
            fees_usdt=pos.entry_fee_usdt + fill.fee_usdt,
            reason=fill.reason.value,
            opened_at=pos.opened_at,
            closed_at=time.time(),
            max_drawdown_pct=(pos.min_price / pos.entry_price - 1.0) * 100.0 if pos.min_price > 0 else 0.0,
        )
        self.store.positions.pop(pos.symbol, None)
        self._close_requests.pop(pos.symbol, None)
        self._last_sync.pop(pos.symbol, None)
        self.store.record_trade(trade)
        log.info("Мавқеъ баста шуд: %s %s PnL %+.2f USDT (%+.2f%%)", pos.symbol, trade.reason, pnl, trade.pnl_pct)
        try:
            await self.on_exit(trade)
        except Exception as exc:
            log.error("Хатои коркарди хуруҷ: %r", exc, exc_info=True)
