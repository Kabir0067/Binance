"""
Ҷараёни маълумоти бозор дар вақти воқеӣ.

• Як пайвасти combined WebSocket: @ticker (нарх, bid/ask, 24h) ва @kline_<tf> барои ҳамаи ашёҳо.
• Таърихи шамъҳо (1h / 4h / 1d, 999 шамъ) ҳангоми ҳар пайвастшавӣ аз REST бор мешавад — холигӣ намемонад.
• Пайвастшавии худкор бо backoff, ротатсияи endpoint-ҳо ва муайянкунии ҷараёни «мурда» (stale).
"""

from __future__ import annotations

import asyncio
import json
import time
from collections import deque
from dataclasses import dataclass

import aiohttp

from config import BINANCE, CORE_SYMBOLS, MARKET, get_logger

log = get_logger("market_stream")


@dataclass(slots=True)
class Candle:
    open_time: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    close_time: int


@dataclass(slots=True)
class Ticker:
    symbol: str
    price: float
    bid: float
    ask: float
    high_24h: float
    low_24h: float
    change_pct: float
    quote_volume: float
    updated_at: float  # time.monotonic()

    @property
    def age(self) -> float:
        return time.monotonic() - self.updated_at

    @property
    def spread_pct(self) -> float:
        if self.bid <= 0 or self.ask < self.bid:
            return float("inf")
        mid = (self.bid + self.ask) / 2.0
        return (self.ask - self.bid) / mid * 100.0


class CandleSeries:
    """Шамъҳои баста (deque) + шамъи ҷорӣ. `version` ҳар дафъа ки шамъи нав баста мешавад, меафзояд."""

    __slots__ = ("closed", "live", "version")

    def __init__(self, maxlen: int):
        self.closed: deque[Candle] = deque(maxlen=maxlen)
        self.live: Candle | None = None
        self.version = 0

    def reset(self, closed: list[Candle], live: Candle | None) -> None:
        self.closed.clear()
        self.closed.extend(closed)
        self.live = live
        self.version += 1

    def update(self, candle: Candle, is_closed: bool) -> None:
        last = self.closed[-1] if self.closed else None
        if last is not None and candle.open_time < last.open_time:
            return  # паёми кӯҳна

        if is_closed:
            if last is not None and candle.open_time == last.open_time:
                self.closed[-1] = candle
            else:
                self.closed.append(candle)
            if self.live is not None and self.live.open_time <= candle.open_time:
                self.live = None
            self.version += 1
            return

        if last is not None and candle.open_time == last.open_time:
            return  # ин шамъ аллакай баста шудааст
        if self.live is not None and candle.open_time > self.live.open_time:
            # Паёми «x=true»-и шамъи қаблӣ гум шуд — онро баста ҳисоб мекунем.
            self.closed.append(self.live)
            self.version += 1
        self.live = candle


class MarketData:
    """Ҳолати муштараки бозор барои ҳамаи модулҳо."""

    def __init__(
        self,
        symbols: tuple[str, ...] = CORE_SYMBOLS,
        timeframes: tuple[str, ...] = MARKET.timeframes,
        history: int = MARKET.history_limit,
    ):
        self.symbols = tuple(symbols)
        self.timeframes = tuple(timeframes)
        self.tickers: dict[str, Ticker] = {}
        self.series: dict[tuple[str, str], CandleSeries] = {
            (s, tf): CandleSeries(history) for s in self.symbols for tf in self.timeframes
        }
        self.ready = asyncio.Event()
        self.connected = False
        self.last_message_at = 0.0
        self.reconnects = 0

    def ticker(self, symbol: str, max_age: float = MARKET.ticker_max_age_sec) -> Ticker | None:
        t = self.tickers.get(symbol)
        return t if t is not None and t.age <= max_age else None

    def price(self, symbol: str) -> float:
        t = self.tickers.get(symbol)
        return t.price if t is not None else 0.0

    def candles(self, symbol: str, timeframe: str) -> CandleSeries:
        return self.series[(symbol, timeframe)]

    @property
    def is_healthy(self) -> bool:
        return self.connected and (time.monotonic() - self.last_message_at) < MARKET.stale_after_sec


class MarketStream:
    def __init__(self, market: MarketData):
        self.market = market
        streams = [f"{s.lower()}@ticker" for s in market.symbols]
        streams += [f"{s.lower()}@kline_{tf}" for s in market.symbols for tf in market.timeframes]
        self._query = "streams=" + "/".join(streams)
        self._ws_idx = 0

    # ───────────────────────────── Цикли асосӣ ─────────────────────────────
    async def run_forever(self) -> None:
        backoff = 1.0
        timeout = aiohttp.ClientTimeout(total=None, sock_connect=BINANCE.http_timeout_sec)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while True:
                url = f"{BINANCE.ws_urls[self._ws_idx]}?{self._query}"
                try:
                    async with session.ws_connect(url, autoping=True, max_msg_size=0) as ws:
                        await self._bootstrap(session)
                        self.market.connected = True
                        self.market.last_message_at = time.monotonic()
                        self.market.ready.set()
                        backoff = 1.0
                        log.info("WebSocket пайваст шуд (%s), %d ашё", BINANCE.ws_urls[self._ws_idx], len(self.market.symbols))
                        await self._consume(ws)
                except asyncio.CancelledError:
                    raise
                except _ExpectedDisconnect as exc:
                    log.info("WebSocket: %s — пайвасти нав", exc)
                except Exception as exc:
                    log.error("WebSocket узилиш: %r — такрор баъд аз %.0f сония", exc, backoff)
                    self._ws_idx = (self._ws_idx + 1) % len(BINANCE.ws_urls)
                finally:
                    self.market.connected = False

                self.market.reconnects += 1
                await asyncio.sleep(backoff)
                backoff = min(backoff * 2.0, MARKET.reconnect_max_backoff_sec)

    async def _consume(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        while True:
            try:
                msg = await ws.receive(timeout=MARKET.stale_after_sec)
            except asyncio.TimeoutError as exc:
                raise ConnectionError(f"{MARKET.stale_after_sec:.0f} сония паём наомад (stale)") from exc

            if msg.type is aiohttp.WSMsgType.TEXT:
                self._handle(msg.data)
            elif msg.type in (aiohttp.WSMsgType.CLOSE, aiohttp.WSMsgType.CLOSING, aiohttp.WSMsgType.CLOSED):
                raise _ExpectedDisconnect(f"сервер пайвастро баст (code={ws.close_code})")
            elif msg.type is aiohttp.WSMsgType.ERROR:
                raise ConnectionError(f"хатои WebSocket: {ws.exception()!r}")

    # ───────────────────────────── Коркарди паём ───────────────────────────
    def _handle(self, raw: str) -> None:
        try:
            data = json.loads(raw).get("data")
            if not data:
                return
            event = data.get("e")
            if event == "24hrTicker":
                symbol = data["s"]
                price = float(data["c"])
                self.market.tickers[symbol] = Ticker(
                    symbol=symbol,
                    price=price,
                    bid=float(data.get("b") or price),
                    ask=float(data.get("a") or price),
                    high_24h=float(data["h"]),
                    low_24h=float(data["l"]),
                    change_pct=float(data["P"]),
                    quote_volume=float(data["q"]),
                    updated_at=time.monotonic(),
                )
            elif event == "kline":
                k = data["k"]
                series = self.market.series.get((data["s"], k["i"]))
                if series is not None:
                    series.update(
                        Candle(
                            open_time=int(k["t"]),
                            open=float(k["o"]),
                            high=float(k["h"]),
                            low=float(k["l"]),
                            close=float(k["c"]),
                            volume=float(k["v"]),
                            close_time=int(k["T"]),
                        ),
                        is_closed=bool(k["x"]),
                    )
        except (ValueError, KeyError, TypeError, AttributeError) as exc:
            log.warning("Паёми нодуруст аз WebSocket: %r", exc)
        finally:
            self.market.last_message_at = time.monotonic()

    # ───────────────────────────── REST: таърих ────────────────────────────
    async def _bootstrap(self, session: aiohttp.ClientSession) -> None:
        jobs = [self._load_klines(session, s, tf) for (s, tf) in self.market.series]
        jobs.append(self._load_tickers(session))
        await asyncio.gather(*jobs)

    async def _load_klines(self, session: aiohttp.ClientSession, symbol: str, timeframe: str) -> None:
        rows = await self._get(
            session,
            "/api/v3/klines",
            {"symbol": symbol, "interval": timeframe, "limit": str(min(MARKET.history_limit + 1, 1000))},
        )
        candles = [
            Candle(
                open_time=int(r[0]),
                open=float(r[1]),
                high=float(r[2]),
                low=float(r[3]),
                close=float(r[4]),
                volume=float(r[5]),
                close_time=int(r[6]),
            )
            for r in rows
        ]
        if not candles:
            raise ConnectionError(f"{symbol} {timeframe}: шамъ наомад")
        # Шамъи охирин ҳамеша ҷорӣ (кушода) аст.
        self.market.series[(symbol, timeframe)].reset(candles[:-1], candles[-1])

    async def _load_tickers(self, session: aiohttp.ClientSession) -> None:
        rows = await self._get(
            session,
            "/api/v3/ticker/24hr",
            {"symbols": json.dumps(list(self.market.symbols), separators=(",", ":"))},
        )
        now = time.monotonic()
        for r in rows:
            price = float(r["lastPrice"])
            self.market.tickers[r["symbol"]] = Ticker(
                symbol=r["symbol"],
                price=price,
                bid=float(r.get("bidPrice") or price),
                ask=float(r.get("askPrice") or price),
                high_24h=float(r["highPrice"]),
                low_24h=float(r["lowPrice"]),
                change_pct=float(r["priceChangePercent"]),
                quote_volume=float(r["quoteVolume"]),
                updated_at=now,
            )

    async def _get(self, session: aiohttp.ClientSession, path: str, params: dict[str, str]):
        last_error: Exception | None = None
        req_timeout = aiohttp.ClientTimeout(total=BINANCE.http_timeout_sec)
        for base in BINANCE.public_rest_urls:
            try:
                async with session.get(base + path, params=params, timeout=req_timeout) as resp:
                    if resp.status == 200:
                        return await resp.json(content_type=None)
                    body = (await resp.text())[:200]
                    last_error = ConnectionError(f"HTTP {resp.status} {base}{path}: {body}")
                    if resp.status in (418, 429):
                        retry_after = float(resp.headers.get("Retry-After") or 10)
                        log.error("Лимити Binance (HTTP %d) — интизорӣ %.0f сония", resp.status, retry_after)
                        await asyncio.sleep(min(retry_after, 120.0))
            except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
                last_error = exc
        raise ConnectionError(f"REST {path} дастнорас: {last_error!r}")


class _ExpectedDisconnect(Exception):
    """Узилиши муқаррарӣ (Binance ҳар 24 соат пайвастро мебандад)."""
