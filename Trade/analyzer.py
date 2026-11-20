"""
Таҳлилгари илмӣ-омории бозор — 7 ашёро бисёрсатҳа (1d → 4h → 1h) таҳлил карда, ашёҳоеро интихоб мекунад,
ки эҳтимоли баланди расидан ба ҳадафи фоидаи софи 1.0–1.5% дар 24 соат доранд.

Стратегия — «Statistical Trend Take-Profit» (танҳо Spot, танҳо харид):

  1. Бозор (BTC):     1d — EMA50 > EMA200 ва нарх > EMA50;  4h — нарх > EMA50↑ ва тренд t ≥ 0.
  2. Макро (ашё 1d):   нарх > EMA50, EMA50 баланд мешавад, EMA50 > EMA200 (golden cross).
  3. Тренд (ашё 4h):   нарх > EMA50↑ ва тренди АЗ ҶИҲАТИ ОМОРӢ МУҲИМ: t-statistic-и регрессияи OLS-и
                       log(нарх) дар 30 шамъ ≥ 3 (p < 0.01) — ба ашёе ки поён меравад, ҳеҷ гоҳ ворид намешавем.
  4. Вуруд (ашё 1h):   t(48h) ≥ 0, нарх > EMA20 ва аз шамъи қаблӣ баланд (импулс ҳозир боло),
                       z-score ≤ 1.5 (аз ҳад «тасфида» нест), RSI 45–70.
  5. Эҳтимолият:       P(нарх дар 24 соат ба ҳадаф мерасад) аз назарияи first-passage-и ҳаракати Броун
                       бо drift (принсипи инъикос): drift — аз регрессия, волатилият — Garman–Klass.
                       Аз ҳадафҳои софи 1.5% → 1.25% → 1.0% калонтаринаш бо P ≥ 75% интихоб мешавад.
                       Ҳадафи нарх комиссияи воқеии харид ва фурӯшро дақиқ дар бар мегирад (≈ 1.2–1.7%).
  6. Баромад:          LIMIT-фурӯш дар ҳадаф. Дар поёнравии муваққатӣ НАМЕФУРӮШЕМ.
                       Танҳо агар тренди 4h аз ҷиҳати оморӣ поён гардад (t ≤ −3) ВА зарар > 5% — сармоя
                       аз «дармондан»-и дарозмуддат ҳифз мешавад (дар backtest бадтарин мавқеъ ≈ −34%, на −90%).

Backtest 2020-01 → 2026-11 (7 ашё, комиссия 0.1%, slippage, то 4 мавқеъ, ≤ 50% сармоя ба як ашё):
  ≈ +227%, ~0.6 савдо дар рӯз, 89.5% савдоҳо бо фоида, ҳар сол бо фоида, max drawdown ≈ 33%.
  Натиҷаи гузашта кафолати оянда нест.

Ҳамаи ҳисобҳо бо Python-и тоза ва танҳо дар шамъҳои басташуда (бе «назар ба оянда»), натиҷа кэш мешавад.
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass
from typing import Sequence

from config import (
    MARKET, MARKET_LEADER, PAPER_FEE_RATE, STRATEGY, SYMBOL_PROFILES, StrategyConfig, SymbolProfile, get_logger,
)
from market_stream import Candle, MarketData

log = get_logger("analyzer")


# ════════════════════════════════ Математика ════════════════════════════════
def ema(values: Sequence[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [0.0] * len(values)
    acc = out[0] = float(values[0])
    for i in range(1, len(values)):
        acc = values[i] * k + acc * (1.0 - k)
        out[i] = acc
    return out


def sma(values: Sequence[float], period: int) -> list[float]:
    out = [0.0] * len(values)
    total = 0.0
    for i, v in enumerate(values):
        total += v
        if i >= period:
            total -= values[i - period]
        out[i] = total / min(i + 1, period)
    return out


def rolling_std(values: Sequence[float], period: int) -> list[float]:
    mean = sma(values, period)
    mean_sq = sma([v * v for v in values], period)
    return [math.sqrt(max(q - m * m, 0.0)) for q, m in zip(mean_sq, mean)]


def rsi(closes: Sequence[float], period: int) -> list[float]:
    n = len(closes)
    out = [50.0] * n
    if n <= period:
        return out
    gain = loss = 0.0
    for i in range(1, period + 1):
        d = closes[i] - closes[i - 1]
        gain += max(d, 0.0)
        loss += max(-d, 0.0)
    gain /= period
    loss /= period
    out[period] = 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)
    for i in range(period + 1, n):
        d = closes[i] - closes[i - 1]
        gain = (gain * (period - 1) + max(d, 0.0)) / period
        loss = (loss * (period - 1) + max(-d, 0.0)) / period
        out[i] = 100.0 if loss == 0 else 100.0 - 100.0 / (1.0 + gain / loss)
    return out


def log_regression(values: Sequence[float], window: int) -> tuple[list[float], list[float]]:
    """Регрессияи ғалтони OLS-и log(нарх) аз рӯи вақт: (slope дар як шамъ, t-statistic-и slope)."""
    n = len(values)
    slope, tstat = [0.0] * n, [0.0] * n
    if n < window or window < 3:
        return slope, tstat
    y = [math.log(v) for v in values]
    sx = window * (window - 1) / 2.0
    sxx = (window - 1) * window * (2 * window - 1) / 6.0
    den = window * sxx - sx * sx
    var_x = sxx - sx * sx / window
    for i in range(window - 1, n):
        w = y[i - window + 1 : i + 1]
        sy = sum(w)
        sxy = sum(k * v for k, v in enumerate(w))
        b = (window * sxy - sx * sy) / den
        a = (sy - b * sx) / window
        sse = sum((v - a - b * k) ** 2 for k, v in enumerate(w))
        se = math.sqrt(sse / (window - 2) / var_x) if sse > 0 else 1e-12
        slope[i], tstat[i] = b, b / se
    return slope, tstat


def garman_klass_vol(o: Sequence[float], h: Sequence[float], l: Sequence[float], c: Sequence[float], window: int) -> list[float]:
    """Волатилияти Garman–Klass (log, дар як шамъ) — аз OHLC, дақиқтар аз close-to-close."""
    k = 2.0 * math.log(2.0) - 1.0
    var = [
        max(0.5 * math.log(hh / ll) ** 2 - k * math.log(cc / oo) ** 2, 0.0)
        for oo, hh, ll, cc in zip(o, h, l, c)
    ]
    return [math.sqrt(v) for v in sma(var, window)]


def _phi(x: float) -> float:
    return 0.5 * (1.0 + math.erf(x / math.sqrt(2.0)))


def hit_probability(barrier: float, drift: float, sigma: float, horizon: float) -> float:
    """P(max_{0≤t≤T} X_t ≥ b) барои X_t = μt + σW_t (first-passage, принсипи инъикос). Ҳама дар log-воҳид."""
    if sigma <= 0 or horizon <= 0:
        return 1.0 if drift * horizon >= barrier else 0.0
    st = sigma * math.sqrt(horizon)
    expo = min(2.0 * drift * barrier / (sigma * sigma), 50.0)
    return min(1.0, _phi((drift * horizon - barrier) / st) + math.exp(expo) * _phi((-barrier - drift * horizon) / st))


def gross_move_pct(net_pct: float, fee_buy: float, fee_sell: float) -> float:
    """Ҳаракати лозимии нарх (%) барои фоидаи софи net_pct баъди комиссияи харид ва фурӯш."""
    return ((1.0 + net_pct / 100.0) / ((1.0 - fee_buy) * (1.0 - fee_sell)) - 1.0) * 100.0


# ════════════════════════════════ Фреймҳо ═══════════════════════════════════
class Frame1h:
    __slots__ = ("open", "high", "low", "close", "close_time", "ema", "zmean", "zstd", "rsi", "slope", "tstat", "vol")

    def __init__(self, candles: Sequence[Candle], cfg: StrategyConfig = STRATEGY):
        self.open = [c.open for c in candles]
        self.high = [c.high for c in candles]
        self.low = [c.low for c in candles]
        self.close = [c.close for c in candles]
        self.close_time = [c.close_time for c in candles]
        self.ema = ema(self.close, cfg.ema_trigger)
        self.zmean = sma(self.close, cfg.zscore_period)
        self.zstd = rolling_std(self.close, cfg.zscore_period)
        self.rsi = rsi(self.close, cfg.rsi_period)
        self.slope, self.tstat = log_regression(self.close, cfg.reg_window_1h)
        self.vol = garman_klass_vol(self.open, self.high, self.low, self.close, cfg.vol_window)

    def __len__(self) -> int:
        return len(self.close)


class Frame4h:
    __slots__ = ("close", "close_time", "ema", "slope", "tstat")

    def __init__(self, candles: Sequence[Candle], cfg: StrategyConfig = STRATEGY):
        self.close = [c.close for c in candles]
        self.close_time = [c.close_time for c in candles]
        self.ema = ema(self.close, cfg.ema_regime)
        self.slope, self.tstat = log_regression(self.close, cfg.reg_window_4h)

    def __len__(self) -> int:
        return len(self.close)


class FrameDaily:
    __slots__ = ("close", "close_time", "ema_mid", "ema_slow")

    def __init__(self, candles: Sequence[Candle], cfg: StrategyConfig = STRATEGY):
        self.close = [c.close for c in candles]
        self.close_time = [c.close_time for c in candles]
        self.ema_mid = ema(self.close, cfg.ema_regime)
        self.ema_slow = ema(self.close, cfg.ema_macro)

    def __len__(self) -> int:
        return len(self.close)


# ════════════════════════════════ Натиҷаҳо ══════════════════════════════════
@dataclass(frozen=True)
class Setup:
    """Шароити техникӣ дар шамъи басташудаи 1h иҷро шуд (ҳанӯз бе нархи зинда)."""

    symbol: str
    signal_close: float
    signal_close_time: int   # ms
    drift: float             # μ дар як соат (log)
    sigma: float             # σ дар як соат (log)
    tstat_4h: float
    notes: str


@dataclass(frozen=True)
class TradeSignal:
    symbol: str
    score: float
    entry_price: float
    target_price: float
    net_target_pct: float    # фоидаи соф баъди ҳамаи комиссияҳо
    gross_target_pct: float  # ҳаракати лозимии нарх
    hit_prob: float          # P(расидан ба ҳадаф дар horizon)
    valid_until_ms: int
    notes: str


@dataclass(frozen=True)
class Assessment:
    symbol: str
    price: float
    signal: TradeSignal | None
    reason: str


# ═════════════════════════════ Мантиқи стратегия ═════════════════════════════
def _daily_golden(fd: FrameDaily, d: int) -> bool:
    return fd.ema_mid[d] > fd.ema_slow[d] and fd.close[d] > fd.ema_mid[d]


def _regime_4h(f4: Frame4h, k: int, cfg: StrategyConfig) -> bool:
    return f4.close[k] > f4.ema[k] and f4.ema[k] > f4.ema[k - cfg.regime_slope_bars_4h]


def market_block_reason(btc4: Frame4h, kb: int, btcd: FrameDaily, db: int, cfg: StrategyConfig = STRATEGY) -> str | None:
    """None — бозори умумӣ (BTC) барои харид мусоид аст; вагарна сабаби рад."""
    if db < cfg.ema_macro or kb < cfg.ema_regime:
        return "бозор: маълумоти BTC кам"
    if not _daily_golden(btcd, db):
        return "бозор: BTC 1d дар тренди болоравӣ нест"
    if not _regime_4h(btc4, kb, cfg) or btc4.tstat[kb] < cfg.btc_trend_t_min_4h:
        return "бозор: BTC 4h поён меравад"
    return None


def evaluate_setup(
    symbol: str,
    f1: Frame1h, i: int,
    f4: Frame4h, k: int,
    fd: FrameDaily, d: int,
    cfg: StrategyConfig = STRATEGY,
) -> Setup | str:
    """Филтрҳои техникӣ дар шамъҳои баста (i, k, d). Setup ё сабаби рад."""
    if i < cfg.warmup_1h or k < cfg.ema_regime or d < cfg.ema_macro:
        return "маълумот кам"

    # 1) Макро — 1d
    if not (fd.close[d] > fd.ema_mid[d] and fd.ema_mid[d] > fd.ema_mid[d - cfg.regime_slope_bars_1d]):
        return "1d: тренд болоравӣ нест"
    if fd.ema_mid[d] <= fd.ema_slow[d]:
        return "1d: golden-cross нест"

    # 2) Тренди оморӣ — 4h
    if not _regime_4h(f4, k, cfg):
        return "4h: нарх зери EMA50 / EMA50 поён"
    t4 = f4.tstat[k]
    if t4 < cfg.trend_t_min_4h:
        return f"4h: тренд аз ҷиҳати оморӣ муҳим нест (t={t4:.1f})"

    # 3) Вуруд — 1h
    close = f1.close[i]
    if f1.tstat[i] < cfg.trend_t_min_1h:
        return f"1h: тренди кӯтоҳ поён (t={f1.tstat[i]:.1f})"
    if close <= f1.ema[i] or close <= f1.close[i - 1]:
        return "1h: нарх ҳоло поён меравад"
    z = (close - f1.zmean[i]) / f1.zstd[i] if f1.zstd[i] > 0 else 0.0
    if z > cfg.zscore_max:
        return f"1h: нарх аз ҳад баланд (z={z:.1f})"
    r = f1.rsi[i]
    if not (cfg.rsi_min <= r <= cfg.rsi_max):
        return f"1h: RSI {r:.0f} берун аз {cfg.rsi_min:.0f}–{cfg.rsi_max:.0f}"

    drift = cfg.drift_shrink * max(f1.slope[i], f4.slope[k] / cfg.hours_per_regime_bar)
    return Setup(
        symbol=symbol,
        signal_close=close,
        signal_close_time=f1.close_time[i],
        drift=drift,
        sigma=f1.vol[i],
        tstat_4h=t4,
        notes=f"t4h {t4:.1f} · z {z:+.1f} · RSI {r:.0f} · σ {f1.vol[i] * 100:.2f}%/с",
    )


def build_signal(
    setup: Setup,
    entry: float,
    profile: SymbolProfile,
    fee_buy: float,
    fee_sell: float,
    cost_buffer: float,
    cfg: StrategyConfig = STRATEGY,
) -> TradeSignal | str:
    """Аз Setup ва нархи вуруд (ask) сигнал месозад: калонтарин ҳадафи соф бо P(hit) ≥ ҳадд."""
    if entry <= 0:
        return "нарх нест"
    if entry > setup.signal_close * (1.0 + cfg.max_entry_drift_pct / 100.0):
        return "нарх аз сигнал дур рафт"
    for net in cfg.net_targets_pct:
        gross = gross_move_pct(net, fee_buy, fee_sell)
        barrier = math.log(1.0 + gross / 100.0 + cost_buffer)
        prob = hit_probability(barrier, setup.drift, setup.sigma, cfg.hit_horizon_hours)
        if prob >= cfg.hit_prob_min:
            return TradeSignal(
                symbol=setup.symbol,
                score=round(prob * net * profile.weight, 4),
                entry_price=entry,
                target_price=entry * (1.0 + gross / 100.0),
                net_target_pct=net,
                gross_target_pct=round(gross, 3),
                hit_prob=round(prob, 3),
                valid_until_ms=setup.signal_close_time + tf_ms(MARKET.tf_trigger),
                notes=setup.notes,
            )
    best = hit_probability(
        math.log(1.0 + gross_move_pct(min(cfg.net_targets_pct), fee_buy, fee_sell) / 100.0 + cost_buffer),
        setup.drift, setup.sigma, cfg.hit_horizon_hours,
    )
    return f"эҳтимоли ҳадаф кам (P={best:.0%} < {cfg.hit_prob_min:.0%})"


def trend_exit_reason(f1: Frame1h, i: int, f4: Frame4h, k: int, entry_price: float, cfg: StrategyConfig = STRATEGY) -> str | None:
    """Баромади оморӣ: тренди 4h муҳим поён (t ≤ ҳадд) ВА зарар аз ҳадд зиёд. Вагарна None (интизорӣ)."""
    if i < 0 or k < cfg.reg_window_4h:
        return None
    loss_pct = (f1.close[i] / entry_price - 1.0) * 100.0
    if f4.tstat[k] < cfg.trend_exit_t_max_4h and loss_pct < -cfg.trend_exit_loss_pct:
        return f"тренди 4h поён (t={f4.tstat[k]:.1f}), нарх {loss_pct:+.1f}%"
    return None


def last_closed_index(close_times: Sequence[int], ts_ms: int) -> int:
    """Индекси охирин шамъе, ки то ts_ms баста шудааст (ҷустуҷӯи дуӣ). −1 агар нест."""
    lo, hi = 0, len(close_times) - 1
    ans = -1
    while lo <= hi:
        mid = (lo + hi) // 2
        if close_times[mid] <= ts_ms:
            ans = mid
            lo = mid + 1
        else:
            hi = mid - 1
    return ans


def tf_ms(tf: str) -> int:
    return int(tf[:-1]) * {"m": 60_000, "h": 3_600_000, "d": 86_400_000, "w": 604_800_000}[tf[-1]]


# ════════════════════════════════ Таҳлилгар ═════════════════════════════════
class MarketAnalyzer:
    def __init__(self, market: MarketData, fee_buy: float = PAPER_FEE_RATE, fee_sell: float = PAPER_FEE_RATE):
        self.market = market
        self.fee_buy = fee_buy
        self.fee_sell = fee_sell
        self._frames: dict[tuple[str, str], tuple[int, object]] = {}
        self._setups: dict[str, tuple[tuple[int, ...], Setup | str]] = {}

    def _frame(self, symbol: str, tf: str):
        series = self.market.candles(symbol, tf)
        cached = self._frames.get((symbol, tf))
        if cached is not None and cached[0] == series.version:
            return cached[1]
        cls = {MARKET.tf_trigger: Frame1h, MARKET.tf_regime: Frame4h, MARKET.tf_macro: FrameDaily}[tf]
        frame = cls(list(series.closed))
        self._frames[(symbol, tf)] = (series.version, frame)
        return frame

    def frames(self, symbol: str) -> tuple[Frame1h, Frame4h, FrameDaily]:
        return (self._frame(symbol, MARKET.tf_trigger), self._frame(symbol, MARKET.tf_regime),
                self._frame(symbol, MARKET.tf_macro))

    def last_closed_hour(self, symbol: str) -> int:
        """open_time-и охирин шамъи басташудаи 1h (ms), ё 0."""
        closed = self.market.candles(symbol, MARKET.tf_trigger).closed
        return closed[-1].open_time if closed else 0

    def _setup(self, symbol: str) -> Setup | str:
        key = tuple(self.market.candles(symbol, tf).version for tf in MARKET.timeframes)
        cached = self._setups.get(symbol)
        if cached is not None and cached[0] == key:
            return cached[1]
        try:
            f1, f4, fd = self.frames(symbol)
            result: Setup | str = evaluate_setup(symbol, f1, len(f1) - 1, f4, len(f4) - 1, fd, len(fd) - 1)
        except Exception as exc:  # хатои техникӣ дар ҳисоб — ин ашё муваққатан рад мешавад
            log.error("Хатои ҳисоби индикаторҳо барои %s: %r", symbol, exc, exc_info=True)
            result = "хатои техникӣ дар таҳлил"
        self._setups[symbol] = (key, result)
        return result

    def market_block_reason(self) -> str | None:
        try:
            _, btc4, btcd = self.frames(MARKET_LEADER)
            return market_block_reason(btc4, len(btc4) - 1, btcd, len(btcd) - 1)
        except Exception as exc:
            log.error("Хатои филтри бозор: %r", exc, exc_info=True)
            return "филтри бозор кор накард"

    def assess(self, symbol: str, block_reason: str | None = None) -> Assessment:
        ticker = self.market.ticker(symbol)
        if ticker is None:
            return Assessment(symbol, self.market.price(symbol), None, "нархи зинда нест")
        setup = self._setup(symbol)
        if isinstance(setup, str):
            return Assessment(symbol, ticker.price, None, setup)
        if block_reason:
            return Assessment(symbol, ticker.price, None, block_reason)
        profile = SYMBOL_PROFILES[symbol]
        if ticker.spread_pct > profile.max_spread_pct:
            return Assessment(symbol, ticker.price, None, f"спред васеъ ({ticker.spread_pct:.3f}%)")
        if time.time() * 1000 > setup.signal_close_time + tf_ms(MARKET.tf_trigger):
            return Assessment(symbol, ticker.price, None, "сигнал кӯҳна шуд")
        buffer = max(ticker.spread_pct / 100.0, STRATEGY.min_cost_buffer)
        signal = build_signal(setup, ticker.ask, profile, self.fee_buy, self.fee_sell, buffer)
        if isinstance(signal, str):
            return Assessment(symbol, ticker.price, None, signal)
        return Assessment(
            symbol, ticker.price, signal,
            f"СИГНАЛ · ҳадаф +{signal.net_target_pct:g}% соф · P={signal.hit_prob:.0%} · {signal.notes}",
        )

    def scan(self) -> list[Assessment]:
        block = self.market_block_reason()
        return [self.assess(symbol, block) for symbol in self.market.symbols]

    def signals(self, exclude: set[str] | frozenset[str] = frozenset()) -> list[TradeSignal]:
        """Ҳамаи сигналҳои қавӣ (аз беҳтарин ба поён). Холӣ, агар бозор мусоид набошад."""
        if self.market_block_reason():
            return []
        found = [a.signal for s in self.market.symbols if s not in exclude for a in (self.assess(s),) if a.signal]
        return sorted(found, key=lambda sg: sg.score, reverse=True)

    def trend_exit(self, symbol: str, entry_price: float) -> str | None:
        try:
            f1, f4, _ = self.frames(symbol)
            return trend_exit_reason(f1, len(f1) - 1, f4, len(f4) - 1, entry_price)
        except Exception as exc:
            log.error("Хатои санҷиши баромади %s: %r", symbol, exc, exc_info=True)
            return None
