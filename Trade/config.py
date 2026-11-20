"""
Конфигуратсияи ягонаи система — ҳамаи модулҳо параметрҳоро танҳо аз ҳамин ҷо мегиранд.

Аз .env танҳо 4 калиди махфӣ хонда мешавад:
    BINANCE_API_KEY, BINANCE_API_SECRET, TELE_BOT_API, ADMIN_ID
Ҳамаи параметрҳои дигар (режим, стратегия, риск, вақт, роҳҳо, логҳо) дар ҳамин файл.

Логҳо: ҳар модул файли шахсии худро дорад (Logs/<модул>.log) ва дар он ФАҚАТ хатоҳо
сабт мешаванд: WARNING — мантиқӣ, ERROR — техникӣ, CRITICAL — критикӣ.
Ҷараёни муқаррарии кор (INFO) танҳо ба консол / journald меравад.
"""

from __future__ import annotations

import logging
import os
import sys
from dataclasses import dataclass
from logging.handlers import RotatingFileHandler
from pathlib import Path

from dotenv import load_dotenv

# ════════════════════════════════ Роҳҳо ════════════════════════════════
PROJECT_ROOT = Path(__file__).resolve().parent.parent
ENV_FILE = PROJECT_ROOT / ".env"
LOG_DIR = PROJECT_ROOT / "Logs"
DATA_DIR = PROJECT_ROOT / "Data"
STATE_FILE = DATA_DIR / "state.json"

load_dotenv(ENV_FILE)

# ════════════════════════════════ Режим ════════════════════════════════
# False → PAPER: савдои виртуалӣ бо нархҳои воқеии бозор (пули воқеӣ истифода намешавад).
# True  → LIVE:  савдои воқеӣ дар Binance Spot бо пули ҳисоб.
LIVE_TRADING: bool = False

PAPER_START_BALANCE: float = 1000.0   # баланси ибтидоии виртуалӣ, USDT
PAPER_FEE_RATE: float = 0.001         # 0.1% — комиссияи стандартии Binance Spot (дар LIVE аз ҳисоб гирифта мешавад)
QUOTE_ASSET = "USDT"


# ════════════════════════════ Калидҳо (.env) ═══════════════════════════
def _env_str(name: str) -> str:
    return (os.getenv(name) or "").strip().strip('"').strip("'")


def _env_int(name: str, default: int = 0) -> int:
    raw = _env_str(name)
    try:
        return int(raw) if raw else default
    except ValueError:
        return default


@dataclass(frozen=True)
class Settings:
    binance_api_key: str
    binance_api_secret: str
    tele_bot_api: str
    admin_id: int

    @property
    def has_binance_keys(self) -> bool:
        return bool(self.binance_api_key and self.binance_api_secret)

    @property
    def has_telegram(self) -> bool:
        return bool(self.tele_bot_api and self.admin_id)


settings = Settings(
    binance_api_key=_env_str("BINANCE_API_KEY"),
    binance_api_secret=_env_str("BINANCE_API_SECRET"),
    tele_bot_api=_env_str("TELE_BOT_API"),
    admin_id=_env_int("ADMIN_ID"),
)


# ═══════════════════════════════ Binance ═══════════════════════════════
@dataclass(frozen=True)
class BinanceConfig:
    rest_urls: tuple[str, ...] = (
        "https://api.binance.com",
        "https://api1.binance.com",
        "https://api2.binance.com",
        "https://api3.binance.com",
        "https://api4.binance.com",
    )
    # Барои маълумоти оммавии бозор (захиравӣ, агар api.binance.com дастнорас бошад)
    public_rest_urls: tuple[str, ...] = (
        "https://api.binance.com",
        "https://data-api.binance.vision",
        "https://api1.binance.com",
    )
    ws_urls: tuple[str, ...] = (
        "wss://stream.binance.com:9443/stream",
        "wss://stream.binance.com:443/stream",
        "wss://data-stream.binance.vision/stream",
    )
    recv_window_ms: int = 5000
    http_timeout_sec: float = 10.0
    weight_warn_level: int = 4800       # аз 6000 / дақиқа
    client_order_prefix: str = "kbx"    # префикси clientOrderId — ордерҳои бот аз дигарон фарқ мешаванд


BINANCE = BinanceConfig()


# ═══════════════════════════════ Ашёҳо ═════════════════════════════════
@dataclass(frozen=True)
class SymbolProfile:
    symbol: str
    weight: float = 1.0           # зарби афзалият дар рейтинг (1.0 — муқаррарӣ)
    max_spread_pct: float = 0.10  # спреди ҳадди аксари иҷозатдодашуда, %


SYMBOL_PROFILES: dict[str, SymbolProfile] = {
    p.symbol: p
    for p in (
        SymbolProfile("BTCUSDT", max_spread_pct=0.05),
        SymbolProfile("ETHUSDT", max_spread_pct=0.05),
        SymbolProfile("BNBUSDT", max_spread_pct=0.08),
        SymbolProfile("SOLUSDT", max_spread_pct=0.08),
        SymbolProfile("XRPUSDT", max_spread_pct=0.08),
        SymbolProfile("LINKUSDT", max_spread_pct=0.10),
        SymbolProfile("AVAXUSDT", max_spread_pct=0.10),
    )
}
CORE_SYMBOLS: tuple[str, ...] = tuple(SYMBOL_PROFILES)
MARKET_LEADER = "BTCUSDT"   # барои филтри режими умумии бозор


# ═══════════════════════════ Маълумоти бозор ═══════════════════════════
@dataclass(frozen=True)
class MarketConfig:
    tf_trigger: str = "1h"      # сигнали вуруд
    tf_regime: str = "4h"       # тренди оморӣ
    tf_macro: str = "1d"        # режими калон (golden cross)
    history_limit: int = 999    # шамъҳои баста (+1 ҷорӣ = 1000, ҳадди Binance) — EMA200 пурра ҷамъ мешавад
    stale_after_sec: float = 30.0        # ин қадар паём наояд → reconnect
    ticker_max_age_sec: float = 15.0     # нархи кӯҳнатар аз ин истифода намешавад
    reconnect_max_backoff_sec: float = 30.0

    @property
    def timeframes(self) -> tuple[str, str, str]:
        return self.tf_trigger, self.tf_regime, self.tf_macro


MARKET = MarketConfig()


# ═══════════════════════════════ Стратегия ═════════════════════════════
@dataclass(frozen=True)
class StrategyConfig:
    """«Statistical Trend Take-Profit» — параметрҳо бо backtest-и 2020–2026 (комиссия + slippage) интихоб
    ва устуворияшон бо санҷиши ҳамсояҳо тасдиқ шудааст. Тағйир додан бе санҷиши такрорӣ тавсия дода намешавад."""

    # Индикаторҳо
    ema_trigger: int = 20         # EMA(1h)
    zscore_period: int = 20       # z-score(1h)
    rsi_period: int = 14
    reg_window_1h: int = 48       # регрессияи OLS: 48 соат
    reg_window_4h: int = 30       # регрессияи OLS: 30 × 4h = 5 рӯз
    vol_window: int = 48          # волатилияти Garman–Klass: 48 соат
    ema_regime: int = 50          # EMA50 (4h ва 1d)
    ema_macro: int = 200          # EMA200 (1d)
    regime_slope_bars_4h: int = 3
    regime_slope_bars_1d: int = 5
    hours_per_regime_bar: int = 4
    warmup_1h: int = 250

    # Филтрҳо («ба ашёе ки поён меравад, ворид намешавем»)
    trend_t_min_4h: float = 3.0       # тренди 4h аз ҷиҳати оморӣ муҳим: t ≥ 3 (p < 0.01)
    trend_t_min_1h: float = 0.0       # тренди 48 соат манфӣ нест
    btc_trend_t_min_4h: float = 0.0   # BTC 4h поён намеравад
    zscore_max: float = 1.5           # нарх аз ҳад «тасфида» нест
    rsi_min: float = 45.0
    rsi_max: float = 70.0

    # Ҳадаф ва эҳтимолият
    net_targets_pct: tuple[float, ...] = (1.5, 1.25, 1.0)   # фоидаи СОФ баъди комиссия (калонтаринаш бо P ≥ ҳадд)
    hit_horizon_hours: int = 24       # P(расидан ба ҳадаф дар 24 соат)
    hit_prob_min: float = 0.75        # ҳадди ақали эҳтимолият
    drift_shrink: float = 0.5         # drift-и регрессия эҳтиёткорона 2 баробар кам карда мешавад
    min_cost_buffer: float = 0.0003   # спред/slippage дар ҳисоби эҳтимолият
    max_entry_drift_pct: float = 0.3  # ask аз close-и сигнал на зиёда аз 0.3% баланд

    # Баромади оморӣ (ягона ҳолати фурӯш бо зарар — сармоя «дарозмуддат дармонда» намешавад)
    trend_exit_t_max_4h: float = -3.0  # тренди 4h аз ҷиҳати оморӣ муҳим ПОЁН (t ≤ −3) …
    trend_exit_loss_pct: float = 5.0   # … ВА нарх аз вуруд зиёда аз 5% поён


STRATEGY = StrategyConfig()


# ═══════════════════════════════ Сармоя ════════════════════════════════
@dataclass(frozen=True)
class RiskConfig:
    max_positions: int = 4             # то 4 ашё ҳамзамон (як мавқеъ ба як ашё)
    capital_usage_pct: float = 98.0    # USDT-и озод, ки ба савдо меравад (2% — захира барои комиссия)
    max_position_pct: float = 50.0     # як ашё ≤ 50% аз сармоя (diversification — ҳифз аз «дармондан»)
    min_trade_usdt: float = 12.0       # савдои хурдтар аз ин кушода намешавад
    entry_fail_cooldown_min: float = 5.0


RISK = RiskConfig()


# ═══════════════════════════════ Вақт ══════════════════════════════════
@dataclass(frozen=True)
class TimingConfig:
    entry_loop_sec: float = 1.0
    batch_grace_sec: float = 30.0           # баъди бастани соат то ин қадар интизори шамъҳои ҳамаи ашёҳо
    batch_max_age_sec: float = 180.0        # сигнал танҳо дар 3 дақиқаи аввали соат (мисли backtest — вуруд дар оғози соат)
    watch_loop_sec: float = 1.0
    exchange_sync_sec: float = 3.0          # санҷиши ордерҳои фурӯш дар биржа (LIVE)
    sell_retry_sec: float = 10.0            # такрори гузоштани ордери фурӯш, агар ноком шавад
    balance_cache_sec: float = 5.0
    time_sync_sec: float = 1800.0
    rules_refresh_sec: float = 6 * 3600.0
    housekeeping_sec: float = 30.0
    market_down_alert_sec: float = 120.0
    telegram_send_timeout_sec: float = 15.0
    shutdown_timeout_sec: float = 15.0


TIMING = TimingConfig()


# ═══════════════════════════════ Логҳо ═════════════════════════════════
LOG_FORMAT = "%(asctime)s | %(levelname)-8s | %(name)-14s | %(message)s"
LOG_DATE_FORMAT = "%Y-%m-%d %H:%M:%S"
LOG_FILE_LEVEL = logging.WARNING     # файли шахсӣ: фақат хатоҳо
LOG_CONSOLE_LEVEL = logging.INFO     # консол / journald: ҷараёни кор
LOG_MAX_BYTES = 5 * 1024 * 1024
LOG_BACKUP_COUNT = 5

for _stream in (sys.stdout, sys.stderr):
    try:
        _stream.reconfigure(encoding="utf-8", errors="replace")  # type: ignore[attr-defined]
    except (AttributeError, ValueError):
        pass

_console_handler: logging.Handler | None = None


def _shared_console_handler() -> logging.Handler:
    global _console_handler
    if _console_handler is None:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(LOG_CONSOLE_LEVEL)
        handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        _console_handler = handler
    return _console_handler


def get_logger(name: str) -> logging.Logger:
    """Логгери модул: консол (INFO+) ва файли шахсии Logs/<name>.log (WARNING+)."""
    logger = logging.getLogger(name)
    if getattr(logger, "_kbx_configured", False):
        return logger

    logger.setLevel(logging.INFO)
    logger.propagate = False
    logger.addHandler(_shared_console_handler())

    try:
        LOG_DIR.mkdir(parents=True, exist_ok=True)
        file_handler = RotatingFileHandler(
            LOG_DIR / f"{name}.log",
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
        )
        file_handler.setLevel(LOG_FILE_LEVEL)
        file_handler.setFormatter(logging.Formatter(LOG_FORMAT, LOG_DATE_FORMAT))
        logger.addHandler(file_handler)
    except OSError as exc:  # диск пур / иҷозат нест — бот бояд бе файли лог ҳам кор кунад
        logger.error("Файли лог кушода нашуд (%s): %s", name, exc)

    logger._kbx_configured = True  # type: ignore[attr-defined]
    return logger
