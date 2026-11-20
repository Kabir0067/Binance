"""
Идоракунии пурраи бот тавассути Telegram (танҳо барои ADMIN_ID).

Фармонҳо ва тугмаҳо:
    /status    — ҳолати умумӣ: режим, сармоя, бозор, PnL-и имрӯз, мавқеъҳо
    /positions — ҳамаи мавқеъҳои кушода бо PnL-и зинда ва масофа то ҳадаф
    /scan      — таҳлили зиндаи 7 ашё (чаро харид мешавад ё намешавад)
    /stats     — омор: шумора, % бо фоида, PnL, фоидаи миёна, вақти миёна
    /balance   — баланс
    /errors    — хатоҳои охирин аз логҳо
    /pause     — қатъи вуруди нав     /resume — идома
    /close     — бастани мавқеъ(ҳо) бо нархи бозор (бо тасдиқ)

Огоҳиҳо тавассути навбат (queue) фиристода мешаванд: retry, риояи лимити Telegram (429),
ва бот ҳеҷ гоҳ аз сабаби Telegram суст ё қатъ намешавад.
"""

from __future__ import annotations

import asyncio
import html
import logging
import re
import time
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from telebot import types
from telebot.async_telebot import AsyncTeleBot
from telebot.asyncio_helper import ApiTelegramException

from config import LOG_DIR, QUOTE_ASSET, RISK, STRATEGY, TIMING, get_logger, settings
from order_watcher import ClosedTrade, Position

if TYPE_CHECKING:
    from analyzer import TradeSignal
    from engine import TradingEngine

log = get_logger("telegram_bot")

# Логҳои дохилии китобхонаи telebot ба файли шахсии ҳамин модул равона мешаванд.
_telebot_logger = logging.getLogger("TeleBot")
_telebot_logger.handlers = list(log.handlers)
_telebot_logger.setLevel(logging.ERROR)
_telebot_logger.propagate = False

BTN_STATUS = "📊 Ҳолат"
BTN_POSITIONS = "💼 Мавқеъҳо"
BTN_SCAN = "🔍 Таҳлил"
BTN_STATS = "📈 Омор"
BTN_BALANCE = "💰 Баланс"
BTN_ERRORS = "⚠️ Хатоҳо"
BTN_PAUSE = "⏸ Таваққуф"
BTN_RESUME = "▶️ Идома"
BTN_CLOSE = "🛑 Бастан"

REASON_LABELS = {
    "TAKE_PROFIT": "🎯 Ҳадаф иҷро шуд",
    "TREND_EXIT": "📉 Баромади оморӣ (тренд шикаст)",
    "MANUAL": "✋ Дастӣ",
    "EXTERNAL": "↗️ Берун аз бот",
}

_TAG_RE = re.compile(r"<[^>]+>")


# ════════════════════════════════ Формат ════════════════════════════════
def fp(price: float) -> str:
    if price >= 1000:
        return f"{price:,.2f}"
    if price >= 1:
        return f"{price:,.4f}"
    return f"{price:.6f}"


def fu(value: float) -> str:
    return f"{value:,.2f}"


def fdur(seconds: float) -> str:
    seconds = max(0, int(seconds))
    d, rem = divmod(seconds, 86400)
    h, rem = divmod(rem, 3600)
    m = rem // 60
    if d:
        return f"{d}р {h}с"
    if h:
        return f"{h}с {m}д"
    return f"{m}д"


def _utc_midnight() -> float:
    now = datetime.now(timezone.utc)
    return now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()


def _menu() -> types.ReplyKeyboardMarkup:
    kb = types.ReplyKeyboardMarkup(resize_keyboard=True)
    kb.row(BTN_STATUS, BTN_POSITIONS)
    kb.row(BTN_SCAN, BTN_STATS)
    kb.row(BTN_BALANCE, BTN_ERRORS)
    kb.row(BTN_PAUSE, BTN_RESUME, BTN_CLOSE)
    return kb


# ════════════════════════════════ Бот ═══════════════════════════════════
class TelegramBot:
    def __init__(self, engine: "TradingEngine"):
        self.engine = engine
        self.admin_id = settings.admin_id
        self.enabled = settings.has_telegram
        self.bot: AsyncTeleBot | None = AsyncTeleBot(settings.tele_bot_api, parse_mode="HTML") if self.enabled else None
        self._queue: asyncio.Queue[str] = asyncio.Queue(maxsize=500)
        if self.bot is not None:
            self._register_handlers()

    # ─────────────────────────── Огоҳиҳо ───────────────────────────
    def notify(self, text: str) -> None:
        """Ғайриблокӣ: паём ба навбат гузошта мешавад."""
        try:
            self._queue.put_nowait(text)
        except asyncio.QueueFull:
            try:
                self._queue.get_nowait()
                self._queue.put_nowait(text)
            except (asyncio.QueueEmpty, asyncio.QueueFull):
                pass
            log.warning("Навбати Telegram пур шуд — паёми кӯҳнатарин партофта шуд")

    def notify_buy(self, pos: Position, signal: "TradeSignal") -> None:
        move = (pos.target_price / pos.entry_price - 1) * 100
        n = len(self.engine.store.positions)
        self.notify(
            f"🟢 <b>ХАРИД · {pos.symbol}</b> [{pos.mode}] · мавқеъ {n}/{RISK.max_positions}\n"
            f"Нарх: <b>{fp(pos.entry_price)}</b> · Маблағ: <b>{fu(pos.cost_usdt)} {QUOTE_ASSET}</b>\n"
            f"🎯 Фурӯш: <b>{fp(pos.target_price)}</b> (+{move:.2f}% нарх → <b>+{pos.net_target_pct:g}% соф</b>)\n"
            f"📐 P(ҳадаф дар {STRATEGY.hit_horizon_hours}с) = <b>{signal.hit_prob:.0%}</b>\n"
            f"🔬 {html.escape(signal.notes)}"
        )

    def notify_exit(self, t: ClosedTrade) -> None:
        head = "✅ <b>ФОИДА</b>" if t.pnl_usdt > 0 else "🔻 <b>ЗАРАР</b>"
        self.notify(
            f"{head} · <b>{t.symbol}</b> [{t.mode}]\n"
            f"{REASON_LABELS.get(t.reason, t.reason)}\n"
            f"Вуруд → Хуруҷ: {fp(t.entry_price)} → <b>{fp(t.exit_price)}</b>\n"
            f"PnL софи: <b>{t.pnl_usdt:+.2f} {QUOTE_ASSET}</b> ({t.pnl_pct:+.2f}%)\n"
            f"Комиссия: {t.fees_usdt:.2f} · Вақт: {fdur(t.closed_at - t.opened_at)} · пасттарин {t.max_drawdown_pct:+.1f}%"
        )

    async def send_now(self, text: str) -> None:
        if self.enabled:
            await self._deliver(text)

    # ─────────────────────────── Ҳаёти бот ───────────────────────────
    async def run(self) -> None:
        if self.bot is None:
            log.warning("Telegram ғайрифаъол: TELE_BOT_API ё ADMIN_ID дар .env нест — огоҳиҳо танҳо дар консол")
            while True:
                text = await self._queue.get()
                log.info("[TG] %s", _TAG_RE.sub("", text).replace("\n", " | "))

        await self._setup_commands()
        tasks = {asyncio.create_task(self._sender_loop()), asyncio.create_task(self._poll())}
        try:
            done, _ = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
            for task in done:
                task.result()
            raise RuntimeError("вазифаи Telegram бе хато қатъ шуд")
        finally:
            for task in tasks:
                task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

    async def close(self) -> None:
        if self.bot is None:
            return
        try:
            await self.bot.close_session()
        except Exception:
            pass

    async def _setup_commands(self) -> None:
        assert self.bot is not None
        commands = [
            ("status", "Ҳолати бот"), ("positions", "Мавқеъҳои кушода"), ("scan", "Таҳлили бозор"),
            ("stats", "Омор"), ("balance", "Баланс"), ("errors", "Хатоҳои охирин"),
            ("pause", "Қатъи вуруди нав"), ("resume", "Идомаи савдо"), ("close", "Бастани мавқеъ"),
        ]
        try:
            await self.bot.set_my_commands([types.BotCommand(c, d) for c, d in commands])
        except Exception as exc:
            log.warning("Менюи фармонҳо насб нашуд: %r", exc)

    async def _poll(self) -> None:
        assert self.bot is not None
        await self.bot.infinity_polling(
            timeout=30,
            request_timeout=60,
            skip_pending=True,
            allowed_updates=["message", "callback_query"],
            logger_level=logging.ERROR,
        )

    async def _sender_loop(self) -> None:
        while True:
            text = await self._queue.get()
            await self._deliver(text)
            await asyncio.sleep(0.05)

    async def _deliver(self, text: str, chat_id: int | None = None, markup: object | None = None) -> None:
        assert self.bot is not None
        target = chat_id or self.admin_id
        text = text[:4000]
        for attempt in range(4):
            try:
                await asyncio.wait_for(
                    self.bot.send_message(target, text, disable_web_page_preview=True, reply_markup=markup),
                    timeout=TIMING.telegram_send_timeout_sec,
                )
                return
            except ApiTelegramException as exc:
                if exc.error_code == 429:
                    retry = int((exc.result_json or {}).get("parameters", {}).get("retry_after", 5))
                    await asyncio.sleep(min(retry, 60))
                    continue
                if exc.error_code == 400 and "parse" in str(exc.description).lower():
                    text = _TAG_RE.sub("", text)  # HTML вайрон — матни оддӣ
                    continue
                log.error("Telegram паёмро рад кард: %s", exc)
                return
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("Фиристодан ба Telegram ноком (кӯшиши %d): %r", attempt + 1, exc)
                await asyncio.sleep(2 ** attempt)
        log.error("Паём ба Telegram баъди 4 кӯшиш нарасид")

    # ─────────────────────────── Фармонҳо ───────────────────────────
    def _authorized(self, user_id: int | None) -> bool:
        if user_id is not None and int(user_id) == int(self.admin_id):
            return True
        log.warning("Кӯшиши дастрасии беиҷозат: user_id=%s", user_id)
        return False

    def _register_handlers(self) -> None:
        bot = self.bot
        assert bot is not None
        routes = {
            "/start": self._cmd_help, "/help": self._cmd_help,
            "/status": self._cmd_status, BTN_STATUS: self._cmd_status,
            "/positions": self._cmd_positions, "/position": self._cmd_positions, "/pos": self._cmd_positions,
            BTN_POSITIONS: self._cmd_positions,
            "/scan": self._cmd_scan, BTN_SCAN: self._cmd_scan,
            "/stats": self._cmd_stats, BTN_STATS: self._cmd_stats,
            "/balance": self._cmd_balance, BTN_BALANCE: self._cmd_balance,
            "/errors": self._cmd_errors, BTN_ERRORS: self._cmd_errors,
            "/pause": self._cmd_pause, BTN_PAUSE: self._cmd_pause,
            "/resume": self._cmd_resume, BTN_RESUME: self._cmd_resume,
            "/close": self._cmd_close, BTN_CLOSE: self._cmd_close,
        }

        @bot.message_handler(content_types=["text"])
        async def on_text(message: types.Message) -> None:
            if not self._authorized(getattr(message.from_user, "id", None)):
                return
            key = (message.text or "").strip()
            if key.startswith("/"):
                key = key.split()[0].split("@")[0].lower()
            handler = routes.get(key, self._cmd_help)
            try:
                text, markup = await handler()
            except Exception as exc:
                log.error("Хатои фармони '%s': %r", key, exc, exc_info=True)
                text, markup = f"❌ Хатогӣ: <code>{html.escape(repr(exc))[:300]}</code>", None
            await self._deliver(text, message.chat.id, markup or _menu())

        @bot.callback_query_handler(func=lambda call: True)
        async def on_callback(call: types.CallbackQuery) -> None:
            if not self._authorized(getattr(call.from_user, "id", None)):
                return
            try:
                await bot.answer_callback_query(call.id)
            except Exception:
                pass
            data = str(call.data or "")
            if data.startswith("close:") and data != "close:no":
                target = data.split(":", 1)[1]
                n = self.engine.request_close(None if target == "ALL" else target)
                text = f"⏳ Фармони бастан қабул шуд ({n}) — натиҷа фавран меояд." if n else "ℹ️ Мавқеи кушода нест."
            else:
                text = "👌 Бекор шуд."
            await self._deliver(text, call.message.chat.id if call.message else None)

    # ─────────────────────────── Ҷавобҳо ───────────────────────────
    async def _cmd_help(self) -> tuple[str, object | None]:
        mode = "🟢 LIVE — пули воқеӣ" if self.engine.executor.live else "🧪 PAPER — савдои виртуалӣ"
        nets = " / ".join(f"{x:g}%" for x in sorted(STRATEGY.net_targets_pct))
        return (
            "🤖 <b>Боти савдои Binance Spot</b>\n"
            f"Режим: <b>{mode}</b>\n"
            f"Ҳадафи соф: <b>{nets}</b> · то {RISK.max_positions} ашё ҳамзамон\n\n"
            "📊 /status — ҳолати умумӣ\n"
            "💼 /positions — мавқеъҳои кушода\n"
            "🔍 /scan — таҳлили 7 ашё\n"
            "📈 /stats — омор\n"
            "💰 /balance — баланс\n"
            "⚠️ /errors — хатоҳои охирин\n"
            "⏸ /pause · ▶️ /resume — идораи вуруд\n"
            "🛑 /close — бастани мавқеъ бо нархи бозор",
            _menu(),
        )

    def _pos_pnl(self, pos: Position) -> tuple[float, float, float]:
        price = self.engine.market.price(pos.symbol) or pos.entry_price
        value = pos.value(price) - pos.remaining_qty * price * self.engine.executor.taker_fee
        pnl = value - pos.cost_usdt
        return price, pnl, pnl / pos.cost_usdt * 100 if pos.cost_usdt else 0.0

    async def _cmd_status(self) -> tuple[str, object | None]:
        e = self.engine
        ok, why = e.trading_gate()
        try:
            equity = await e.executor.equity()
            free = await e.executor.free_quote()
            money = f"💰 Сармоя: <b>{fu(equity)} {QUOTE_ASSET}</b> (озод {fu(free)})"
        except Exception as exc:
            money = f"💰 Баланс дастнорас: {html.escape(str(exc))[:120]}"
        day_pnl, day_n = e.store.pnl_since(_utc_midnight())
        lines = [
            "🤖 <b>Ҳолати бот</b>",
            f"Режим: <b>{'🟢 LIVE' if e.executor.live else '🧪 PAPER'}</b> · Кор: {fdur(time.time() - e.started_at)}",
            f"Бозор: {'✅ онлайн' if e.market.is_healthy else '❌ қатъ'} · Пайвастшавиҳо: {e.market.reconnects}",
            f"Савдо: {'✅ фаъол' if ok else '⏸ ' + html.escape(why)}",
            money,
            f"📅 Имрӯз: <b>{day_pnl:+.2f} {QUOTE_ASSET}</b> ({day_n} савдо)",
            f"💼 Мавқеъҳо: {len(e.store.positions)}/{RISK.max_positions}",
        ]
        for pos in e.store.positions.values():
            _, pnl, pct = self._pos_pnl(pos)
            lines.append(f"   • {pos.symbol} {pct:+.2f}% ({pnl:+.2f}) · {fdur(pos.held_sec)}")
        if e.last_batch_at:
            lines.append(f"🕐 Таҳлили охирин: {fdur(time.time() - e.last_batch_at)} пеш (ҳар соат)")
        return "\n".join(lines), None

    async def _cmd_positions(self) -> tuple[str, object | None]:
        e = self.engine
        if not e.store.positions:
            return "ℹ️ Мавқеи кушода нест. Бот ҳар соат бозорро таҳлил мекунад.", None
        blocks = []
        for pos in e.store.positions.values():
            price, pnl, pct = self._pos_pnl(pos)
            to_target = (pos.target_price / price - 1) * 100
            order = f"ордер #{pos.sell_order_id}" if pos.sell_order_id else ("симулятсия" if pos.mode == "PAPER" else "⚠️ ордер нест")
            part = f" · фурӯхта {pos.sold_qty:g}" if pos.sold_qty else ""
            blocks.append(
                f"💼 <b>{pos.symbol}</b> [{pos.mode}]\n"
                f"Вуруд {fp(pos.entry_price)} → ҳозир <b>{fp(price)}</b>\n"
                f"PnL: <b>{pnl:+.2f} {QUOTE_ASSET}</b> ({pct:+.2f}%) · пасттарин {(pos.min_price / pos.entry_price - 1) * 100:+.1f}%\n"
                f"🎯 Фурӯш: {fp(pos.target_price)} ({to_target:+.2f}% то ҳадаф, +{pos.net_target_pct:g}% соф) · {order}{part}\n"
                f"⏱ {fdur(pos.held_sec)} · маблағ {fu(pos.cost_usdt)}"
            )
        return "\n\n".join(blocks), None

    async def _cmd_scan(self) -> tuple[str, object | None]:
        rows = self.engine.analyzer.scan()
        lines = ["🔍 <b>Таҳлили зинда</b> (1d → 4h → 1h · эҳтимолият)"]
        held = self.engine.store.positions
        for a in rows:
            mark = "💼" if a.symbol in held else ("✅" if a.signal else "▫️")
            lines.append(f"{mark} <b>{a.symbol}</b> {fp(a.price)} — {html.escape(a.reason)}")
        ok, why = self.engine.trading_gate()
        if not ok:
            lines.append(f"\n⏸ Вуруд манъ: {html.escape(why)}")
        lines.append("\nℹ️ Вуруд танҳо дар оғози ҳар соат (баъди бастани шамъи 1h).")
        return "\n".join(lines), None

    async def _cmd_stats(self) -> tuple[str, object | None]:
        store = self.engine.store

        def block(title: str, s: dict[str, float]) -> str:
            return (
                f"<b>{title}</b>: {s['trades']:.0f} савдо · {s['win_rate']:.0f}% бо фоида\n"
                f"   PnL <b>{s['pnl']:+.2f}</b> · миёна {s['avg_pct']:+.2f}% · вақт ~{s['avg_hold_h']:.1f}с"
            )

        total = store.stats()
        lines = [
            "📈 <b>Омор</b>",
            block("Имрӯз", store.stats(_utc_midnight())),
            block("7 рӯз", store.stats(time.time() - 7 * 86400)),
            block("30 рӯз", store.stats(time.time() - 30 * 86400)),
            block("Ҳамагӣ", total),
        ]
        if total["trades"]:
            lines.append(f"Беҳтарин {total['best']:+.2f} · Бадтарин {total['worst']:+.2f} · Комиссия {total['fees']:.2f}")
        recent = store.trades[-7:]
        if recent:
            lines.append("\n<b>Савдоҳои охир:</b>")
            for t in reversed(recent):
                icon = "✅" if t.pnl_usdt > 0 else "🔻"
                when = datetime.fromtimestamp(t.closed_at, timezone.utc).strftime("%d.%m %H:%M")
                lines.append(f"{icon} {when} {t.symbol} {t.pnl_usdt:+.2f} ({t.pnl_pct:+.2f}%) · {fdur(t.closed_at - t.opened_at)}")
        return "\n".join(lines), None

    async def _cmd_balance(self) -> tuple[str, object | None]:
        e = self.engine
        balances = await e.executor.balances(force=True)
        lines = [f"💰 <b>Баланс</b> [{e.executor.mode}]"]
        total = 0.0
        for asset, (free, locked) in sorted(balances.items(), key=lambda kv: kv[0] != QUOTE_ASSET):
            px = 1.0 if asset == QUOTE_ASSET else e.market.price(f"{asset}{QUOTE_ASSET}")
            value = (free + locked) * px
            if asset != QUOTE_ASSET and value < 1.0:
                continue
            total += value
            lock = f" (дар ордер {locked:g})" if locked else ""
            lines.append(f"• <b>{asset}</b>: {free + locked:g}{lock} ≈ {fu(value)} {QUOTE_ASSET}")
        lines.append(f"Ҳамагӣ ≈ <b>{fu(total)} {QUOTE_ASSET}</b>")
        return "\n".join(lines), None

    async def _cmd_errors(self) -> tuple[str, object | None]:
        entries: list[str] = []
        for path in LOG_DIR.glob("*.log"):
            try:
                with open(path, "rb") as fh:
                    fh.seek(0, 2)
                    size = fh.tell()
                    fh.seek(max(0, size - 8192))
                    tail = fh.read().decode("utf-8", errors="replace").splitlines()
                entries.extend(line for line in tail[-15:] if line[:4].isdigit())
            except OSError:
                continue
        if not entries:
            return "✅ Дар логҳо хатое нест.", None
        entries.sort()
        body = "\n".join(html.escape(line[:220]) for line in entries[-12:])
        return f"⚠️ <b>Хатоҳои охирин</b>\n<pre>{body}</pre>", None

    async def _cmd_pause(self) -> tuple[str, object | None]:
        self.engine.pause()
        return "⏸ Вуруди нав қатъ шуд. Мавқеъҳои кушода назорат мешаванд. /resume — идома.", None

    async def _cmd_resume(self) -> tuple[str, object | None]:
        self.engine.resume()
        return "▶️ Савдо идома дорад.", None

    async def _cmd_close(self) -> tuple[str, object | None]:
        positions = self.engine.store.positions
        if not positions:
            return "ℹ️ Мавқеи кушода нест.", None
        kb = types.InlineKeyboardMarkup()
        lines = ["🛑 <b>Кадом мавқеъро ҳозир бо нархи бозор мебандед?</b>"]
        for pos in positions.values():
            _, pnl, pct = self._pos_pnl(pos)
            lines.append(f"• {pos.symbol}: {pct:+.2f}% ({pnl:+.2f} {QUOTE_ASSET})")
            kb.row(types.InlineKeyboardButton(f"{pos.symbol} ({pct:+.2f}%)", callback_data=f"close:{pos.symbol}"))
        if len(positions) > 1:
            kb.row(types.InlineKeyboardButton("Ҳамааш", callback_data="close:ALL"))
        kb.row(types.InlineKeyboardButton("❌ Не", callback_data="close:no"))
        lines.append("\nДиққат: агар PnL манфӣ бошад, бастан зарарро воқеӣ мекунад.")
        return "\n".join(lines), kb
