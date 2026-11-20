"""
Иҷрокунандаи савдо дар Binance Spot.

• REST-клиенти имзошуда (HMAC-SHA256): синхронизатсияи вақт бо биржа, ротатсияи endpoint-ҳо,
  риояи rate-limit (429/418 + Retry-After), такрор танҳо дар ҷое ки бехатар аст.
• Қоидаҳои ҷуфт (tickSize / stepSize / minNotional / PERCENT_PRICE) бо Decimal — ордер аз филтр рад намешавад.
• Вуруд: MARKET BUY (quoteOrderQty) → фавран LIMIT SELL (GTC) дар нархи ҳадаф дар худи биржа.
  Ордери фурӯш ҳатто вақте ки бот, сервер ё интернет хомӯш аст, дар биржа кор мекунад.
• Ҳадафи нарх комиссияи воқеии харид (taker) ва фурӯш (maker) аз ҳисобро дақиқ дар бар мегирад —
  фоидаи соф маҳз ҳамон 1.0–1.5% мешавад.
• Иҷрои қисмӣ, бекоркунии беруна, бастани дастӣ ва баромади оморӣ дуруст ҷамъбаст мешаванд.
• Идемпотентӣ: ҳар ордер clientOrderId-и худро дорад; ҳангоми timeout / 5xx ҳолати ордер
  аз биржа санҷида мешавад — харид ё фурӯш ҳеҷ гоҳ ду бор иҷро намешавад.
• PAPER: ҳамон мантиқ бо нархҳои зинда, қоидаҳои воқеии ҷуфт ва комиссия — бе пули воқеӣ.
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import secrets
import time
from dataclasses import dataclass
from decimal import ROUND_DOWN, ROUND_UP, Decimal
from typing import TYPE_CHECKING, Any
from urllib.parse import urlencode

import aiohttp

from analyzer import gross_move_pct
from config import BINANCE, CORE_SYMBOLS, PAPER_FEE_RATE, QUOTE_ASSET, RISK, TIMING, get_logger, settings
from order_watcher import ExitFill, ExitReason, Position, StateStore

if TYPE_CHECKING:
    from analyzer import TradeSignal
    from market_stream import MarketData, Ticker

log = get_logger("trade_executor")

_NOT_FOUND_CODES = {-2011, -2013}


# ═══════════════════════════════ Хатоҳо ═══════════════════════════════
class BinanceError(Exception):
    pass


class BinanceAPIError(BinanceError):
    def __init__(self, status: int, code: int, msg: str, path: str):
        super().__init__(f"HTTP {status} code={code} {msg} [{path}]")
        self.status = status
        self.code = code
        self.msg = msg


class BinanceNetworkError(BinanceError):
    """Натиҷаи дархост номаълум (timeout / узилиш / 5xx) — бояд ҳолат аз биржа санҷида шавад."""


def _parse_error(text: str) -> tuple[int, str]:
    try:
        data = json.loads(text)
        return int(data.get("code", 0)), str(data.get("msg", ""))[:300]
    except (ValueError, TypeError, AttributeError):
        return 0, text[:300]


# ═══════════════════════════════ REST-клиент ══════════════════════════
class BinanceClient:
    def __init__(self, api_key: str, api_secret: str, base_urls: tuple[str, ...]):
        self._api_key = api_key
        self._secret = api_secret.encode()
        self._bases = base_urls
        self._base_idx = 0
        self._session: aiohttp.ClientSession | None = None
        self._offset_ms = 0
        self._blocked_until = 0.0
        self._weight_warned_at = 0.0
        self.used_weight = 0

    async def start(self) -> None:
        if self._session is None or self._session.closed:
            self._session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=BINANCE.http_timeout_sec),
                connector=aiohttp.TCPConnector(limit=20, ttl_dns_cache=300, keepalive_timeout=60),
                headers={"X-MBX-APIKEY": self._api_key} if self._api_key else None,
            )
        await self.sync_time()

    async def close(self) -> None:
        if self._session is not None and not self._session.closed:
            await self._session.close()

    async def sync_time(self) -> None:
        t0 = time.time() * 1000.0
        data = await self.request("GET", "/api/v3/time")
        t1 = time.time() * 1000.0
        self._offset_ms = int(int(data["serverTime"]) - (t0 + t1) / 2.0)
        if abs(self._offset_ms) > 1000:
            log.warning("Соати сервер аз Binance %+d ms фарқ дорад (худкор ислоҳ шуд). NTP-ро санҷед.", self._offset_ms)

    def _rotate(self) -> None:
        self._base_idx = (self._base_idx + 1) % len(self._bases)

    def _track_weight(self, resp: aiohttp.ClientResponse) -> None:
        raw = resp.headers.get("X-MBX-USED-WEIGHT-1M") or resp.headers.get("x-mbx-used-weight-1m")
        if not raw:
            return
        try:
            self.used_weight = int(raw)
        except ValueError:
            return
        if self.used_weight >= BINANCE.weight_warn_level and time.monotonic() - self._weight_warned_at > 60:
            self._weight_warned_at = time.monotonic()
            log.warning("Вазни дархостҳо баланд: %d / 6000 дар дақиқа", self.used_weight)

    async def request(self, method: str, path: str, params: dict[str, Any] | None = None, *, signed: bool = False) -> Any:
        if self._session is None:
            raise RuntimeError("BinanceClient.start() даъват нашудааст")
        attempts = 0
        resynced = False
        while True:
            wait = self._blocked_until - time.monotonic()
            if wait > 0:
                if wait > 30:
                    raise BinanceAPIError(429, -1003, f"лимити Binance — {wait:.0f} сония интизорӣ", path)
                await asyncio.sleep(wait)

            query = {k: v for k, v in (params or {}).items() if v is not None}
            if signed:
                query["recvWindow"] = BINANCE.recv_window_ms
                query["timestamp"] = int(time.time() * 1000) + self._offset_ms
            qs = urlencode(query)
            if signed:
                qs += "&signature=" + hmac.new(self._secret, qs.encode(), hashlib.sha256).hexdigest()
            url = f"{self._bases[self._base_idx]}{path}" + (f"?{qs}" if qs else "")

            try:
                async with self._session.request(method, url) as resp:
                    self._track_weight(resp)
                    status = resp.status
                    retry_after = resp.headers.get("Retry-After")
                    text = await resp.text()
            except aiohttp.ClientConnectorError as exc:
                # Пайваст барқарор нашуд — дархост ба биржа нарасид, такрор бехатар аст.
                attempts += 1
                self._rotate()
                if attempts >= 3:
                    raise BinanceNetworkError(f"{method} {path}: {exc!r}") from exc
                await asyncio.sleep(0.3 * attempts)
                continue
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                attempts += 1
                self._rotate()
                if method == "POST" or attempts >= 3:
                    raise BinanceNetworkError(f"{method} {path}: {exc!r}") from exc
                await asyncio.sleep(0.3 * attempts)
                continue

            if status == 200:
                return json.loads(text) if text else {}

            code, msg = _parse_error(text)
            if status in (418, 429):
                delay = float(retry_after or 10)
                self._blocked_until = time.monotonic() + delay
                (log.critical if status == 418 else log.error)(
                    "Binance лимит: HTTP %d (%s) — интизорӣ %.0f сония", status, msg, delay
                )
                if status == 418 or delay > 30:
                    raise BinanceAPIError(status, code, msg, path)
                continue
            if code == -1021 and not resynced:
                resynced = True
                await self.sync_time()
                continue
            if status >= 500 or code == -1007:
                attempts += 1
                if method == "POST":
                    raise BinanceNetworkError(f"{method} {path}: HTTP {status} {code} {msg}")
                if attempts >= 3:
                    raise BinanceAPIError(status, code, msg, path)
                await asyncio.sleep(0.5 * attempts)
                continue
            raise BinanceAPIError(status, code, msg, path)


# ═══════════════════════════════ Қоидаҳои ҷуфт ════════════════════════
def _floor(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_DOWN) * step


def _ceil(value: Decimal, step: Decimal) -> Decimal:
    if step <= 0:
        return value
    return (value / step).to_integral_value(rounding=ROUND_UP) * step


def fmt(value: Decimal) -> str:
    text = format(value.normalize(), "f")
    return text if text not in ("", "-0") else "0"


@dataclass(frozen=True)
class SymbolRules:
    symbol: str
    base_asset: str
    quote_asset: str
    status: str
    tick_size: Decimal
    step_size: Decimal
    min_qty: Decimal
    max_qty: Decimal
    min_notional: Decimal
    order_types: frozenset[str]
    sell_price_up: float = 2.0      # PERCENT_PRICE_BY_SIDE: нархи SELL ≤ avgPrice × askMultiplierUp
    sell_price_down: float = 0.2    # нархи SELL ≥ avgPrice × askMultiplierDown

    @classmethod
    def parse(cls, s: dict[str, Any]) -> "SymbolRules":
        f = {x["filterType"]: x for x in s.get("filters", [])}
        price_f = f.get("PRICE_FILTER", {})
        lot = f.get("LOT_SIZE", {})
        notional = f.get("NOTIONAL") or f.get("MIN_NOTIONAL") or {}
        pct = f.get("PERCENT_PRICE_BY_SIDE") or f.get("PERCENT_PRICE") or {}
        return cls(
            symbol=s["symbol"],
            base_asset=s["baseAsset"],
            quote_asset=s["quoteAsset"],
            status=s.get("status", ""),
            tick_size=Decimal(price_f.get("tickSize", "0.00000001")),
            step_size=Decimal(lot.get("stepSize", "0.00000001")),
            min_qty=Decimal(lot.get("minQty", "0")),
            max_qty=Decimal(lot.get("maxQty", "9000000000")),
            min_notional=Decimal(notional.get("minNotional", "5")),
            order_types=frozenset(s.get("orderTypes", [])),
            sell_price_up=float(pct.get("askMultiplierUp") or pct.get("multiplierUp") or 2.0),
            sell_price_down=float(pct.get("askMultiplierDown") or pct.get("multiplierDown") or 0.2),
        )

    @property
    def tradable(self) -> bool:
        return self.status == "TRADING"

    def qty(self, value: float | Decimal) -> Decimal:
        return min(_floor(Decimal(str(value)), self.step_size), self.max_qty)

    def price_down(self, value: float | Decimal) -> Decimal:
        return _floor(Decimal(str(value)), self.tick_size)

    def price_up(self, value: float | Decimal) -> Decimal:
        return _ceil(Decimal(str(value)), self.tick_size)


def _client_id(tag: str) -> str:
    return f"{BINANCE.client_order_prefix}{tag}{int(time.time() * 1000)}{secrets.token_hex(2)}"


# ═══════════════════════════════ Иҷрокунанда ══════════════════════════
class TradeExecutor:
    def __init__(self, store: StateStore, market: "MarketData", live: bool):
        self.store = store
        self.market = market
        self.live = bool(live and settings.has_binance_keys)
        self.mode = "LIVE" if self.live else "PAPER"
        self.client = BinanceClient(
            settings.binance_api_key if self.live else "",
            settings.binance_api_secret if self.live else "",
            BINANCE.rest_urls if self.live else BINANCE.public_rest_urls,
        )
        self.rules: dict[str, SymbolRules] = {}
        self.taker_fee = PAPER_FEE_RATE
        self.maker_fee = PAPER_FEE_RATE
        self._balances: dict[str, tuple[float, float]] = {}
        self._balances_at = 0.0

    # ─────────────────────────── Оғоз / маълумот ───────────────────────────
    async def start(self) -> None:
        await self.client.start()
        await self.load_rules()
        if self.live:
            account = await self._account()
            if not account.get("canTrade", False):
                raise RuntimeError("API-калид иҷозати Spot Trading надорад (canTrade=false)")
            rates = account.get("commissionRates") or {}
            self.taker_fee = float(rates.get("taker") or PAPER_FEE_RATE)
            self.maker_fee = float(rates.get("maker") or PAPER_FEE_RATE)
        log.info("Executor омода: режим=%s, комиссия taker=%.4f%% maker=%.4f%%, ҷуфтҳо=%d",
                 self.mode, self.taker_fee * 100, self.maker_fee * 100, len(self.rules))

    async def close(self) -> None:
        await self.client.close()

    async def load_rules(self) -> None:
        data = await self.client.request(
            "GET", "/api/v3/exchangeInfo", {"symbols": json.dumps(list(CORE_SYMBOLS), separators=(",", ":"))}
        )
        rules: dict[str, SymbolRules] = {}
        for s in data.get("symbols", []):
            r = SymbolRules.parse(s)
            rules[r.symbol] = r
            if not r.tradable:
                log.error("%s ҳолати '%s' дорад — савдо бо он манъ аст", r.symbol, r.status)
        missing = set(CORE_SYMBOLS) - set(rules)
        if missing:
            log.error("Қоидаҳои ҷуфт аз биржа наомад: %s", ", ".join(sorted(missing)))
        if rules:
            self.rules = rules

    def can_trade(self, symbol: str) -> bool:
        r = self.rules.get(symbol)
        return r is not None and r.tradable

    def target_price(self, entry: float, net_pct: float) -> float:
        """Нархи фурӯш, ки баъди комиссияи харид (taker) ва фурӯш (maker) маҳз net_pct фоидаи соф медиҳад."""
        return entry * (1.0 + gross_move_pct(net_pct, self.taker_fee, self.maker_fee) / 100.0)

    async def _account(self) -> dict[str, Any]:
        acct = await self.client.request("GET", "/api/v3/account", {"omitZeroBalances": "true"}, signed=True)
        self._balances = {b["asset"]: (float(b["free"]), float(b["locked"])) for b in acct.get("balances", [])}
        self._balances_at = time.monotonic()
        return acct

    async def balances(self, force: bool = False) -> dict[str, tuple[float, float]]:
        """{asset: (free, locked)}"""
        if not self.live:
            out = {QUOTE_ASSET: (self.store.paper_balance, 0.0)}
            for pos in self.store.positions.values():
                out[self._base_of(pos.symbol)] = (0.0, pos.remaining_qty)
            return out
        if force or time.monotonic() - self._balances_at > TIMING.balance_cache_sec:
            await self._account()
        return dict(self._balances)

    async def free_quote(self, force: bool = False) -> float:
        return (await self.balances(force)).get(QUOTE_ASSET, (0.0, 0.0))[0]

    async def equity(self) -> float:
        """USDT (озод + бандшуда) + арзиши ҷории ҳамаи мавқеъҳои бот."""
        free, locked = (await self.balances()).get(QUOTE_ASSET, (0.0, 0.0))
        total = free + locked
        for pos in self.store.positions.values():
            total += pos.value(self.market.price(pos.symbol) or pos.entry_price)
        return total

    def _base_of(self, symbol: str) -> str:
        r = self.rules.get(symbol)
        return r.base_asset if r else symbol[: -len(QUOTE_ASSET)]

    def _invalidate_balances(self) -> None:
        self._balances_at = 0.0

    async def _free_base(self, rules: SymbolRules) -> float:
        return (await self.balances(force=True)).get(rules.base_asset, (0.0, 0.0))[0]

    # ─────────────────────────── Ёрирасонҳо ───────────────────────────
    def _fee_to_usdt(self, commission: float, asset: str, price: float, notional: float, rules: SymbolRules) -> float:
        if commission <= 0:
            return 0.0
        if asset == rules.quote_asset:
            return commission
        if asset == rules.base_asset:
            return commission * price
        px = self.market.price(f"{asset}{QUOTE_ASSET}")
        return commission * px if px > 0 else notional * self.taker_fee

    def _parse_fills(self, order: dict[str, Any], rules: SymbolRules) -> tuple[Decimal, float, float, Decimal]:
        """→ (executed_qty, quote_qty, fee_usdt, fee_in_base)"""
        executed = Decimal(str(order.get("executedQty", "0")))
        quote = float(order.get("cummulativeQuoteQty", "0") or 0)
        fee_usdt = 0.0
        fee_base = Decimal(0)
        for f in order.get("fills") or []:
            commission = Decimal(str(f.get("commission", "0")))
            asset = f.get("commissionAsset", "")
            price = float(f.get("price", 0) or 0)
            qty = float(f.get("qty", 0) or 0)
            if asset == rules.base_asset:
                fee_base += commission
            fee_usdt += self._fee_to_usdt(float(commission), asset, price, price * qty, rules)
        return executed, quote, fee_usdt, fee_base

    async def _order_trades(self, symbol: str, order_id: int) -> list[dict[str, Any]]:
        try:
            rows = await self.client.request("GET", "/api/v3/myTrades", {"symbol": symbol, "orderId": order_id}, signed=True)
            return list(rows or [])
        except BinanceError as exc:
            log.error("myTrades барои %s #%s гирифта нашуд: %s", symbol, order_id, exc)
            return []

    async def _with_trade_fills(self, order: dict[str, Any], rules: SymbolRules) -> dict[str, Any]:
        """Ба ордер fills-ро аз myTrades илова мекунад (барои комиссияи дақиқ)."""
        if not order.get("fills") and Decimal(str(order.get("executedQty", "0"))) > 0:
            trades = await self._order_trades(rules.symbol, int(order["orderId"]))
            order = dict(order)
            order["fills"] = [
                {"price": t["price"], "qty": t["qty"], "commission": t["commission"], "commissionAsset": t["commissionAsset"]}
                for t in trades
            ]
        return order

    async def _find_order(self, symbol: str, client_id: str) -> dict[str, Any] | None:
        for attempt in range(4):
            try:
                return await self.client.request(
                    "GET", "/api/v3/order", {"symbol": symbol, "origClientOrderId": client_id}, signed=True
                )
            except BinanceAPIError as exc:
                if exc.code not in _NOT_FOUND_CODES:
                    log.error("Санҷиши ордери %s ноком: %s", client_id, exc)
            except BinanceNetworkError as exc:
                log.error("Санҷиши ордери %s: шабака %s", client_id, exc)
            await asyncio.sleep(1.0 + attempt)
        return None

    # ═════════════════════════════ Кушодан ═════════════════════════════
    async def open_position(self, signal: "TradeSignal", quote_amount: float) -> Position | None:
        rules = self.rules.get(signal.symbol)
        if rules is None or not rules.tradable:
            log.error("%s: қоидаҳои савдо нест ё ҷуфт фаъол нест — вуруд бекор шуд", signal.symbol)
            return None
        if signal.symbol in self.store.positions:
            log.error("Мантиқ: %s аллакай мавқеъ дорад — харид бекор шуд", signal.symbol)
            return None
        spend = Decimal(str(quote_amount)).quantize(Decimal("0.01"), rounding=ROUND_DOWN)
        minimum = max(float(rules.min_notional) * 1.1, RISK.min_trade_usdt)
        if float(spend) < minimum:
            log.info("%s: маблағ %.2f аз ҳадди ақал %.2f кам — вуруд нест", signal.symbol, spend, minimum)
            return None
        if self.live:
            return await self._open_live(signal, rules, spend)
        return self._open_paper(signal, rules, float(spend))

    def _new_position(self, signal: "TradeSignal", qty: float, avg: float, cost: float, fee: float) -> Position:
        # Ҳадаф аз хароҷоти ВОҚЕӢ ва миқдори ВОҚЕИИ фурӯш (баъди комиссия ва яклухткунии stepSize) —
        # фоидаи соф маҳз net_target_pct мешавад.
        exact = cost * (1.0 + signal.net_target_pct / 100.0) / (qty * (1.0 - self.maker_fee)) if qty > 0 else 0.0
        return Position(
            symbol=signal.symbol,
            mode=self.mode,
            qty=qty,
            entry_price=avg,
            cost_usdt=cost,
            entry_fee_usdt=fee,
            target_price=max(exact, self.target_price(avg, signal.net_target_pct)),
            net_target_pct=signal.net_target_pct,
            opened_at=time.time(),
            hit_prob=signal.hit_prob,
            min_price=avg,
            score=signal.score,
            notes=signal.notes,
        )

    async def _open_live(self, signal: "TradeSignal", rules: SymbolRules, spend: Decimal) -> Position | None:
        cid = _client_id("B")
        params = {
            "symbol": rules.symbol, "side": "BUY", "type": "MARKET",
            "quoteOrderQty": fmt(spend), "newClientOrderId": cid, "newOrderRespType": "FULL",
        }
        try:
            order = await self.client.request("POST", "/api/v3/order", params, signed=True)
        except BinanceNetworkError as exc:
            log.error("BUY %s: натиҷа номаълум (%s) — санҷиш бо clientOrderId", rules.symbol, exc)
            found = await self._find_order(rules.symbol, cid)
            if found is None:
                log.error("BUY %s дар биржа ёфт нашуд — ҳисоб мешавад, ки иҷро нашуд", rules.symbol)
                return None
            order = await self._with_trade_fills(found, rules)
        except BinanceAPIError as exc:
            log.error("BUY %s рад шуд: %s", rules.symbol, exc)
            return None
        finally:
            self._invalidate_balances()

        executed, quote, fee_usdt, fee_base = self._parse_fills(order, rules)
        if executed <= 0 or quote <= 0:
            log.error("BUY %s иҷро нашуд (status=%s)", rules.symbol, order.get("status"))
            return None
        avg = quote / float(executed)
        qty_net = rules.qty(executed - fee_base)
        cost = quote + fee_usdt - float(fee_base) * avg
        pos = self._new_position(signal, float(qty_net), avg, cost, fee_usdt)

        # Ҳолат фавран сабт мешавад — ҳатто агар ҳозир бот афтад, мавқеъ гум намешавад.
        self.store.positions[pos.symbol] = pos
        self.store.save()
        log.info("BUY %s: %s @ %.8g (%.2f USDT, комиссия %.4f)", rules.symbol, fmt(qty_net), avg, quote, fee_usdt)

        try:
            await self._place_sell(pos, rules)
        except Exception as exc:
            pos.place_failures += 1
            self.store.save()
            log.error("Ордери фурӯши %s фавран гузошта нашуд (%r) — назоратчӣ такрор мекунад", rules.symbol, exc)
        return pos

    def _open_paper(self, signal: "TradeSignal", rules: SymbolRules, spend: float) -> Position | None:
        price = signal.entry_price
        qty_gross = rules.qty(spend / price)
        if qty_gross < rules.min_qty or qty_gross <= 0:
            log.warning("PAPER %s: миқдор аз minQty кам", signal.symbol)
            return None
        cost = float(qty_gross) * price
        if cost > self.store.paper_balance + 1e-9:
            log.error("PAPER: баланс кофӣ нест (%.2f > %.2f)", cost, self.store.paper_balance)
            return None
        fee = cost * self.taker_fee
        qty_net = rules.qty(float(qty_gross) * (1.0 - self.taker_fee))
        self.store.paper_balance -= cost
        pos = self._new_position(signal, float(qty_net), price, cost, fee)
        self.store.positions[pos.symbol] = pos
        self.store.save()
        log.info("PAPER BUY %s: %s @ %.8g (%.2f USDT), ҳадаф %.8g", signal.symbol, fmt(qty_net), price, cost, pos.target_price)
        return pos

    # ═════════════════════════════ Ордери фурӯш (ҳадаф) ═══════════════
    async def _place_sell(self, pos: Position, rules: SymbolRules) -> int:
        free = await self._free_base(rules)
        qty = rules.qty(min(pos.remaining_qty, free))
        price = rules.price_up(min(pos.target_price, (self.market.price(pos.symbol) or pos.entry_price) * rules.sell_price_up * 0.98))
        if qty <= 0 or qty < rules.min_qty or float(qty) * float(price) < float(rules.min_notional):
            raise _DustError(f"миқдор барои фурӯш кам: {fmt(qty)} (озод {free})")
        cid = _client_id("T")
        params = {
            "symbol": rules.symbol, "side": "SELL", "type": "LIMIT", "timeInForce": "GTC",
            "quantity": fmt(qty), "price": fmt(price), "newClientOrderId": cid, "newOrderRespType": "RESULT",
        }
        try:
            resp = await self.client.request("POST", "/api/v3/order", params, signed=True)
        except BinanceNetworkError:
            resp = await self._find_order(rules.symbol, cid)
            if resp is None:
                raise
        pos.sell_order_id = int(resp["orderId"])
        pos.place_failures = 0
        self._invalidate_balances()
        self.store.save()
        log.info("SELL LIMIT %s #%s: %s @ %s (ҳадафи соф +%.2f%%)", rules.symbol, pos.sell_order_id, fmt(qty), fmt(price),
                 pos.net_target_pct)
        return pos.sell_order_id

    async def _absorb(self, pos: Position, rules: SymbolRules, order: dict[str, Any]) -> None:
        """Иҷрои (қисмии) ордери фурӯшро ба мавқеъ ҷамъ мекунад."""
        executed = float(order.get("executedQty", 0) or 0)
        if executed <= 0:
            return
        order = await self._with_trade_fills(order, rules)
        _, quote, fee, _ = self._parse_fills(order, rules)
        if not order.get("fills"):
            fee = quote * self.maker_fee
        pos.sold_qty += executed
        pos.sold_quote += quote
        pos.sold_fee_usdt += fee
        self.store.save()

    def _combined_fill(self, pos: Position, reason: ExitReason) -> ExitFill:
        qty = pos.sold_qty
        price = pos.sold_quote / qty if qty > 0 else (self.market.price(pos.symbol) or pos.entry_price)
        return ExitFill(price=price, qty=qty, proceeds_usdt=pos.sold_quote - pos.sold_fee_usdt,
                        fee_usdt=pos.sold_fee_usdt, reason=reason)

    def _is_dust(self, rules: SymbolRules, qty: float, price: float) -> bool:
        q = rules.qty(qty)
        return q <= 0 or q < rules.min_qty or float(q) * price < float(rules.min_notional)

    async def sync_position(self, pos: Position, ticker: "Ticker | None") -> ExitFill | None:
        """Ҳолати ордери фурӯшро месанҷад. ExitFill — агар мавқеъ пурра баста шуда бошад."""
        if not self.live:
            return self._paper_check(pos, ticker)
        rules = self.rules.get(pos.symbol)
        if rules is None:
            log.error("%s: қоидаҳои ҷуфт нест — санҷиши ордер ғайриимкон", pos.symbol)
            return None
        try:
            if pos.sell_order_id is None:
                return await self._ensure_sell(pos, rules, ticker)
            try:
                order = await self.client.request(
                    "GET", "/api/v3/order", {"symbol": pos.symbol, "orderId": pos.sell_order_id}, signed=True
                )
            except BinanceAPIError as exc:
                if exc.code in _NOT_FOUND_CODES:
                    log.warning("Ордери фурӯши %s #%s дар биржа ёфт нашуд — аз нав гузошта мешавад", pos.symbol, pos.sell_order_id)
                    pos.sell_order_id = None
                    self.store.save()
                    return None
                raise
            status = str(order.get("status", ""))
            if status == "FILLED":
                pos.sell_order_id = None
                await self._absorb(pos, rules, order)
                return self._combined_fill(pos, ExitReason.TAKE_PROFIT)
            if status in ("NEW", "PARTIALLY_FILLED", "PENDING_NEW"):
                return None
            # CANCELED / EXPIRED / REJECTED — берун аз бот
            pos.sell_order_id = None
            await self._absorb(pos, rules, order)
            log.warning("Ордери фурӯши %s берун аз бот '%s' шуд — ҳолат санҷида мешавад", pos.symbol, status)
            return await self._ensure_sell(pos, rules, ticker)
        except BinanceError as exc:
            log.warning("Санҷиши ордери %s муваққатан ноком: %s", pos.symbol, exc)
            return None

    async def _ensure_sell(self, pos: Position, rules: SymbolRules, ticker: "Ticker | None") -> ExitFill | None:
        price = (ticker.price if ticker else 0.0) or self.market.price(pos.symbol) or pos.entry_price
        if self._is_dust(rules, pos.remaining_qty, price):
            if pos.sold_qty > 0:
                return self._combined_fill(pos, ExitReason.TAKE_PROFIT)
            return self._external_fill(pos, ticker)
        free = await self._free_base(rules)
        if free < pos.remaining_qty * 0.5:
            log.warning("%s: ашё дар ҳисоб нест (озод %s) — берун аз бот фурӯхта шудааст", pos.symbol, free)
            return self._external_fill(pos, ticker)
        try:
            await self._place_sell(pos, rules)
        except Exception as exc:
            pos.place_failures += 1
            self.store.save()
            level = log.critical if pos.place_failures >= 5 else log.error
            level("Ордери фурӯши %s гузошта нашуд (кӯшиши %d): %r", pos.symbol, pos.place_failures, exc)
        return None

    # ═════════════════════════════ Бастан (дастӣ / оморӣ) ══════════════
    async def close_position(self, pos: Position, reason: ExitReason, ticker: "Ticker | None") -> ExitFill | None:
        if not self.live:
            return self._paper_exit(pos, reason, ticker)
        rules = self.rules.get(pos.symbol)
        if rules is None:
            log.error("%s: қоидаҳои ҷуфт нест — бастан ғайриимкон", pos.symbol)
            return None
        try:
            if pos.sell_order_id is not None:
                try:
                    resp = await self.client.request(
                        "DELETE", "/api/v3/order", {"symbol": pos.symbol, "orderId": pos.sell_order_id}, signed=True
                    )
                except BinanceAPIError as exc:
                    if exc.code not in _NOT_FOUND_CODES:
                        raise
                    order = await self.client.request(
                        "GET", "/api/v3/order", {"symbol": pos.symbol, "orderId": pos.sell_order_id}, signed=True
                    )
                    if order.get("status") == "FILLED":
                        pos.sell_order_id = None
                        await self._absorb(pos, rules, order)
                        return self._combined_fill(pos, ExitReason.TAKE_PROFIT)
                    resp = order
                pos.sell_order_id = None
                self._invalidate_balances()
                await self._absorb(pos, rules, resp)

            price = self.market.price(pos.symbol) or pos.entry_price
            free = await self._free_base(rules)
            qty = rules.qty(min(pos.remaining_qty, free))
            if self._is_dust(rules, float(qty), price):
                if pos.sold_qty > 0:
                    return self._combined_fill(pos, reason)
                log.warning("%s: барои фурӯш миқдор нест/кам (озод %s)", pos.symbol, free)
                return self._external_fill(pos, ticker)
            sold = await self._market_sell(rules, qty, reason)
            if sold is None:
                return None
            pos.sold_qty += sold.qty
            pos.sold_quote += sold.proceeds_usdt + sold.fee_usdt
            pos.sold_fee_usdt += sold.fee_usdt
            self.store.save()
            return self._combined_fill(pos, reason)
        except BinanceError as exc:
            log.error("Бастани %s ноком: %s", pos.symbol, exc)
            return None

    async def _market_sell(self, rules: SymbolRules, qty: Decimal, reason: ExitReason) -> ExitFill | None:
        cid = _client_id("S")
        params = {
            "symbol": rules.symbol, "side": "SELL", "type": "MARKET",
            "quantity": fmt(qty), "newClientOrderId": cid, "newOrderRespType": "FULL",
        }
        try:
            order = await self.client.request("POST", "/api/v3/order", params, signed=True)
        except BinanceNetworkError as exc:
            log.error("SELL %s: натиҷа номаълум (%s) — санҷиш бо clientOrderId", rules.symbol, exc)
            found = await self._find_order(rules.symbol, cid)
            if found is None:
                return None
            order = await self._with_trade_fills(found, rules)
        except BinanceAPIError as exc:
            log.error("SELL %s рад шуд: %s", rules.symbol, exc)
            return None
        finally:
            self._invalidate_balances()

        executed, quote, fee_usdt, _ = self._parse_fills(order, rules)
        if executed <= 0:
            log.error("SELL %s иҷро нашуд (status=%s)", rules.symbol, order.get("status"))
            return None
        if not order.get("fills"):
            fee_usdt = quote * self.taker_fee
        log.info("SELL MARKET %s: %s @ %.8g (%s)", rules.symbol, fmt(executed), quote / float(executed), reason.value)
        return ExitFill(price=quote / float(executed), qty=float(executed),
                        proceeds_usdt=quote - fee_usdt, fee_usdt=fee_usdt, reason=reason)

    def _external_fill(self, pos: Position, ticker: "Ticker | None") -> ExitFill:
        price = (ticker.bid if ticker is not None else 0.0) or self.market.price(pos.symbol) or pos.entry_price
        gross = pos.remaining_qty * price
        fee = gross * self.taker_fee
        pos.sell_order_id = None
        qty = pos.sold_qty + pos.remaining_qty
        quote = pos.sold_quote + gross
        fees = pos.sold_fee_usdt + fee
        return ExitFill(price=quote / qty if qty else price, qty=qty, proceeds_usdt=quote - fees,
                        fee_usdt=fees, reason=ExitReason.EXTERNAL)

    # ═════════════════════════════ PAPER ═══════════════════════════════
    def _paper_fill(self, pos: Position, price: float, fee_rate: float, reason: ExitReason) -> ExitFill:
        gross = pos.remaining_qty * price
        fee = gross * fee_rate
        self.store.paper_balance += gross - fee
        pos.sold_qty += pos.remaining_qty
        pos.sold_quote += gross
        pos.sold_fee_usdt += fee
        return self._combined_fill(pos, reason)

    def _paper_check(self, pos: Position, ticker: "Ticker | None") -> ExitFill | None:
        if ticker is not None and ticker.price >= pos.target_price:
            return self._paper_fill(pos, pos.target_price, self.maker_fee, ExitReason.TAKE_PROFIT)
        return None

    def _paper_exit(self, pos: Position, reason: ExitReason, ticker: "Ticker | None") -> ExitFill | None:
        if ticker is None or ticker.bid <= 0:
            log.warning("PAPER: нархи зиндаи %s нест — бастан баъдтар", pos.symbol)
            return None
        return self._paper_fill(pos, ticker.bid, self.taker_fee, reason)

    # ═════════════════════════════ Барқароркунӣ ════════════════════════
    async def recover_orphans(self) -> list[Position]:
        """LIVE: ордерҳои кушодаи фурӯши боти худро ёфта, мавқеъҳои гумшударо барқарор мекунад."""
        if not self.live:
            return []
        found: list[Position] = []
        for symbol in self.rules:
            if symbol in self.store.positions:
                continue
            orders = await self.client.request("GET", "/api/v3/openOrders", {"symbol": symbol}, signed=True)
            for o in orders or []:
                if (o.get("side") != "SELL" or o.get("type") != "LIMIT"
                        or not str(o.get("clientOrderId", "")).startswith(BINANCE.client_order_prefix)):
                    continue
                qty = float(o["origQty"])
                target = float(o["price"])
                entry, cost = await self._last_buy(symbol)
                if entry <= 0:
                    entry = target / (1.0 + gross_move_pct(1.0, self.taker_fee, self.maker_fee) / 100.0)
                    cost = qty * entry
                net = ((target * (1.0 - self.maker_fee)) * qty / cost - 1.0) * 100.0 if cost > 0 else 1.0
                pos = Position(
                    symbol=symbol, mode="LIVE", qty=qty, entry_price=entry, cost_usdt=cost,
                    entry_fee_usdt=cost * self.taker_fee, target_price=target, net_target_pct=round(net, 3),
                    opened_at=float(o.get("time", time.time() * 1000)) / 1000.0, sell_order_id=int(o["orderId"]),
                    min_price=entry, notes="барқароршуда аз биржа",
                )
                log.warning("Ордери фурӯши бесоҳиб ёфт шуд (%s #%s) — мавқеъ барқарор шуд", symbol, pos.sell_order_id)
                found.append(pos)
                break
        return found

    async def _last_buy(self, symbol: str) -> tuple[float, float]:
        try:
            trades = await self.client.request("GET", "/api/v3/myTrades", {"symbol": symbol, "limit": 50}, signed=True)
        except BinanceError as exc:
            log.error("Таърихи савдои %s гирифта нашуд: %s", symbol, exc)
            return 0.0, 0.0
        buys = [t for t in trades or [] if t.get("isBuyer")]
        if not buys:
            return 0.0, 0.0
        last_id = buys[-1]["orderId"]
        sel = [t for t in buys if t["orderId"] == last_id]
        qty = sum(float(t["qty"]) for t in sel)
        quote = sum(float(t["quoteQty"]) for t in sel)
        return (quote / qty if qty > 0 else 0.0), quote


class _DustError(RuntimeError):
    """Миқдор барои ордер аз ҳадди биржа кам аст."""
