import os
import asyncio
import math
import time
import logging
import json
import aiohttp
import msgpack
import signal
import threading
import hmac
import hashlib
import base64
import sys
from collections import defaultdict, deque
from datetime import datetime, UTC
from flask import Flask, jsonify, request
from typing import Dict, Any, List, Optional, Tuple
from urllib.request import Request, urlopen

# =========================================================================
# SYSTEMOWY MODUŁ OBSERVABILITY & GLOBAL CONTEXT (v11.4 TELEMETRY READY)
# =========================================================================
try:
    if hasattr(sys.stdout, 'reconfigure'):
        sys.stdout.reconfigure(line_buffering=True)
except Exception:
    pass

class FlushStreamHandler(logging.StreamHandler):
    """Gwarantuje natychmiastowe wypychanie logów do konsoli Rendera bez czekania na bufor."""
    def emit(self, record):
        super().emit(record)
        self.flush()

LOG_LEVEL_CONFIG = os.environ.get("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger("TradingEngine_OKX_PRODUCTION_v11.4")
logger.setLevel(getattr(logging, LOG_LEVEL_CONFIG, logging.INFO))
logger.handlers.clear()

_stream_handler = FlushStreamHandler(sys.stdout)
_stream_handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
logger.addHandler(_stream_handler)
logger.propagate = False

print("🚀 [BOOT] Silnik transakcyjny v11.4 inicjalizuje telemetrie na Renderze...", flush=True)

IS_SANDBOX = os.environ.get("OKX_IS_SANDBOX", "True").strip().lower() in ("true", "1", "yes")
<comment-tag id="1">logger.info(f"⚙️ [SYSTEM-INIT] Silnik v11.4 Online [GLOBAL CACHE SINGLETON | CRIT FIXES APPLIED | LIVE: {not IS_SANDBOX}]")</comment-tag id="1" text="Zaktualizuj sygnaturę wersji:
logger.info(f'⚙️ [SYSTEM-INIT] Silnik v11.5 Online [CRIT FIX: CANCEL-ALGOS APPLIED | LIVE: {not IS_SANDBOX}]')
Podbicie wersji do v11.5 pozwala natychmiast upewnić się w konsoli Rendera, że uruchomił się kontener ze zaktualizowanym kodem." type="suggestion">

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
GLOBAL_ALPHA_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
RATE_LIMITER: Optional[Any] = None
GLOBAL_WS_FEED: Optional[Any] = None
GLOBAL_REDIS_BRIDGE: Optional[Any] = None  # SINGLETON L1 RAM + REDIS BRIDGE

QUOTE_CCY = "USDC"

# =========================================================================
# CENTRALNA KONFIGURACJA PARAMETRYCZNA (v11.4 FUTURES-GRADE SAFEGUARDS)
# =========================================================================
CONFIG = {
    "ALPHA_MAX_ACTIVE_SLOTS": 3,
    "GRID_MAX_ACTIVE_LEVELS": 3,
    "MAX_SIMULTANEOUS_ALTS": 1,          # Tarcza Korelacji: max 1 altcoin w koszyku Alfa naraz
    "MIN_ORDER_VALUE_USDC": 11.0,
    "RESERVE_CASH_BUFFER_USDC": 2.0,
    "RISK_PER_TRADE_PCT": 0.01,
    "MAX_POSITION_PORTFOLIO_RATIO": 0.18,
    "MAX_BID_ASK_SPREAD_PCT": 0.0012,    # Strażnik Spreadu: max 0.12% rozjazdu arkusza
    "RISK_MANAGEMENT": {
        "COOLDOWN_AFTER_SL_SEC": 14400,  # 4 godziny kwarantanny po uderzeniu w Stop Loss
        "MAX_DAILY_LOSS_PCT": 0.025      # Wyłącznik Dzienny: blokada zakupów przy stracie > 2.5% portfela
    },
    "BREAK_EVEN": {
        "ENABLED": True,
        "FEE_BUFFER_PCT": 0.0022         # +0.22% do ceny zakupu na pokrycie prowizji Taker + bufor poślizgu
    },
    "SMART_MONEY": {
        "ENABLED": True,
        "MAX_LONG_SHORT_RATIO": 2.2,     # Próg euforii tłumu - powyżej blokujemy zakupy (Bull Trap)
        "CACHE_TTL_SEC": 180             # Bufor pamięci podręcznej dla odczytów sentymentu
    },
    "DYNAMIC_RISK": {
        "MIN_SL_PCT": 0.008,
        "MAX_SL_HARD_CAP": 0.020,
        "DEFAULT_SL_PCT": 0.015,
        "FEE_BUFFER_PCT": 0.0022
    },
    "TIMEOUTS": {
        "MOMENTUM": 8 * 3600,
        "BREAKOUT": 8 * 3600,
        "MEAN_REVERSION": 18 * 3600
    },
    "STRATEGY_PARAMS": {
        "MEAN_REVERSION": {
            "CANDLE_BAR": "15m",
            "LOOKBACK_PERIOD": 20,
            "Z_BUY_STANDARD": -1.5,
            "Z_BUY_CRASH": -2.5,
            "RSI_STANDARD": 35.0,
            "RSI_CRASH": 20.0,
            "ATR_SL_MULT": 2.0,
            "RR_RATIO": 1.5,
            "BE_TRIGGER_RATIO": 0.75     # Późne BE (75% drogi do TP) zapobiega wycięciu na podwójnym dnie
        },
        "MOMENTUM": {
            "ROC_PERIOD": 10,
            "ROC_TRIGGER": 2.0,
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 1.5,
            "BE_TRIGGER_RATIO": 0.60     # Aktywacja BE przy 60% drogi do Take Profit
        },
        "BREAKOUT": {
            "BB_PERIOD": 20,
            "COMPRESSION_BANDWIDTH": 0.015,
            "ATR_SL_MULT": 1.5,
            "RR_RATIO": 2.0,
            "BE_TRIGGER_RATIO": 0.55     # Szybkie BE przy 55% drogi do TP po wybiciu
        },
        "GRID": {
            "GRID_STEP_PCT": 0.005,
            "LEVELS": 3,
            "SL_PCT": 0.015,
            "MAX_ATR_MAJORS": 0.8,
            "MAX_ATR_ALTS": 1.6
        }
    }
}

def floor_to_precision(value: float, precision: int) -> float:
    """Rygorystyczne obcinanie wartości w dół bez ryzyka zaokrąglenia w górę."""
    factor = 10 ** precision
    return math.floor(value * factor) / factor

def ceil_to_precision(value: float, precision: int) -> float:
    """Rygorystyczne zaokrąglanie wartości w górę do określonej precyzji (CRIT-03 Fix)."""
    factor = 10 ** precision
    return math.ceil(value * factor) / factor

def calculate_clamped_sl_tp(
    current_price: float, 
    atr: float, 
    atr_mult: float, 
    rr_ratio: float, 
    price_round: int
) -> Tuple[float, float, float]:
    """Wylicza adaptacyjny Stop Loss i Take Profit w oparciu o zmienność ATR z twardym kagańcem."""
    min_sl = CONFIG["DYNAMIC_RISK"]["MIN_SL_PCT"]
    max_sl = CONFIG["DYNAMIC_RISK"]["MAX_SL_HARD_CAP"]
    def_sl = CONFIG["DYNAMIC_RISK"]["DEFAULT_SL_PCT"]

    if atr > 0 and current_price > 0:
        raw_sl_pct = (atr * atr_mult) / current_price
    else:
        raw_sl_pct = def_sl

    sl_pct = max(min_sl, min(raw_sl_pct, max_sl))
    tp_pct = sl_pct * rr_ratio

    price_sl = round(current_price * (1.0 - sl_pct), price_round)
    price_tp = round(current_price * (1.0 + tp_pct), price_round)

    return price_sl, price_tp, sl_pct

# =========================================================================
# SERWER MONITORINGU FLASK (PORT RENDER WORKER)
# =========================================================================
app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return "TRADING_ENGINE_DOWN", 503
<comment-tag id="2">    return "OK_v11.4_FUTURES_GRADE", 200</comment-tag id="2" text="Zmień na:
    return 'OK_v11.5_FUTURES_GRADE', 200
Umożliwia błyskawiczną weryfikację w przeglądarce pod adresem głównym, czy aktywna instancja to zaktualizowana wersja v11.5." type="suggestion">

# =========================================================================
# DIAGNOSTYKA AUTORYZACJI OKX EEA
# =========================================================================
@app.route('/test-auth', methods=['GET'])
def web_test_okx_handshake():
    import urllib.error

    api_key = str(os.environ.get("OKX_API_KEY", "")).strip()
    secret_key = str(os.environ.get("OKX_SECRET_KEY", "")).strip()
    passphrase = str(os.environ.get("OKX_PASSPHRASE", "")).strip()
    
    base_url = "https://eea.okx.com" 
    request_path = "/api/v5/account/config"

    report = {
        "key_prefix": f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) >= 12 else "INVALID",
        "api_key_len": len(api_key),
        "secret_key_len": len(secret_key),
        "passphrase_len": len(passphrase),
        "base_url_used": base_url,
        "is_sandbox": IS_SANDBOX,
        "trials": []
    }

    if not all([api_key, secret_key, passphrase]):
        report["error"] = "Brak wymaganych zmiennych w panelu Render!"
        return jsonify(report), 400

    timestamp = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    message = f"{timestamp}GET{request_path}"
    mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(mac.digest()).decode('utf-8')

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase
    }
    if IS_SANDBOX:
        headers["x-simulated-trading"] = "1"

    try:
        req = Request(f"{base_url}{request_path}", headers=headers, method="GET")
        with urlopen(req, timeout=6) as resp:
            resp_data = json.loads(resp.read().decode('utf-8'))
            report["trials"].append({
                "mode": "SANDBOX" if IS_SANDBOX else "LIVE_SUBACCOUNT",
                "http_status": resp.status,
                "okx_code": resp_data.get("code"),
                "okx_msg": resp_data.get("msg"),
                "data": resp_data.get("data")
            })
    except urllib.error.HTTPError as he:
        err_body = he.read().decode('utf-8', errors='ignore')
        report["trials"].append({
            "mode": "SANDBOX" if IS_SANDBOX else "LIVE_SUBACCOUNT",
            "http_status": he.code,
            "response": err_body[:200]
        })
    except Exception as e:
        report["trials"].append({
            "mode": "SANDBOX" if IS_SANDBOX else "LIVE_SUBACCOUNT",
            "exception": str(e)
        })

    return jsonify(report), 200

# =========================================================================
# DIAGNOSTYKA UPRAWNIEŃ DLA NATYWNYCH BOTÓW GRID OKX EEA
# =========================================================================
@app.route('/test-grid', methods=['GET'])
def web_test_okx_grid_permissions():
    """Bezpieczny, nieinwazyjny test uprawnień do natywnych botów Grid (GET - read only)."""
    import urllib.error

    api_key = str(os.environ.get("OKX_API_KEY", "")).strip()
    secret_key = str(os.environ.get("OKX_SECRET_KEY", "")).strip()
    passphrase = str(os.environ.get("OKX_PASSPHRASE", "")).strip()
    
    base_url = os.environ.get("OKX_API_URL", "https://eea.okx.com").rstrip('/')
    request_path = "/api/v5/tradingBot/grid/orders-algo-pending?algoOrdType=grid"

    report = {
        "test_type": "NATIVE_GRID_BOT_PERMISSION_CHECK",
        "base_url": base_url,
        "endpoint_queried": request_path,
        "is_sandbox": IS_SANDBOX,
        "permission_granted": False,
        "details": {}
    }

    if not all([api_key, secret_key, passphrase]):
        report["error"] = "Brak wymaganych zmiennych API w panelu Render!"
        return jsonify(report), 400

    timestamp = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    message = f"{timestamp}GET{request_path}"
    mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(mac.digest()).decode('utf-8')

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0",
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase
    }
    if IS_SANDBOX:
        headers["x-simulated-trading"] = "1"

    try:
        req = Request(f"{base_url}{request_path}", headers=headers, method="GET")
        with urlopen(req, timeout=6) as resp:
            resp_data = json.loads(resp.read().decode('utf-8'))
            code = str(resp_data.get("code", ""))
            msg = resp_data.get("msg", "")
            report["details"] = {
                "http_status": resp.status,
                "okx_code": code,
                "okx_msg": msg,
                "data": resp_data.get("data")
            }
            if code == "0":
                report["permission_granted"] = True
                report["status_verdict"] = "SUKCES: Klucz API posiada uprawnienia do natywnych botów Grid OKX!"
            elif code in ("50100", "51000", "50101"):
                report["permission_granted"] = False
                report["status_verdict"] = f"BLOKADA UPRAWNIEŃ: Brak uprawnienia 'Trading Bot' (kod {code}: {msg})."
            else:
                report["permission_granted"] = False
                report["status_verdict"] = f"ODPOWIEDŹ OKX: Kod {code} - {msg}"
    except urllib.error.HTTPError as he:
        err_body = he.read().decode('utf-8', errors='ignore')
        report["details"] = {"http_status": he.code, "error_body": err_body}
        report["status_verdict"] = f"BŁĄD HTTP {he.code}: Dostęp do endpointu zablokowany."
    except Exception as e:
        report["details"] = {"exception": str(e)}
        report["status_verdict"] = f"WYJĄTEK POŁĄCZENIA: {e}"

    return jsonify(report), 200

# =========================================================================
# DIAGNOSTYKA SENTYMENTU SMART MONEY (OKX RUBIK API)
# =========================================================================
@app.route('/test-sentiment', methods=['GET'])
def route_test_sentiment():
    """Diagnostyczny endpoint weryfikujący na żywo odczyty sentymentu Smart Money z OKX Rubik."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "ERROR", "message": "Pętla bota nie jest aktywna."}), 503

    async def _fetch():
        async with aiohttp.ClientSession() as s:
            client = OKXSpotClient(s, TokenBucketRateLimiter(4.0, 8.0), is_sandbox=IS_SANDBOX)
            results = {}
            for coin in ["BTC", "ETH", "SOL", "XRP"]:
                ratio = await client.get_smart_money_sentiment(coin)
                results[coin] = ratio if ratio is not None else "BRAK_DANYCH"
            return results

    try:
        fut = asyncio.run_coroutine_threadsafe(_fetch(), BACKGROUND_LOOP)
        data = fut.result(timeout=7.0)
        return jsonify({
            "status": "OK",
            "smart_money_sentiment": data,
            "threshold_max_ratio": CONFIG.get("SMART_MONEY", {}).get("MAX_LONG_SHORT_RATIO", 2.2),
            "filter_enabled": CONFIG.get("SMART_MONEY", {}).get("ENABLED", True)
        }), 200
    except Exception as e:
        return jsonify({"status": "ERROR", "message": str(e)}), 500

# =========================================================================
# DIAGNOSTYKA TARCZ BEZPIECZEŃSTWA (FUTURES-GRADE RISK AUDIT)
# =========================================================================
@app.route('/test-risk', methods=['GET'])
def web_test_futures_risk():
    """Audyt na żywo: stan dziennej straty, limit korelacji oraz spready Bid/Ask."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest aktywna."}), 503

    async def _audit():
        async with aiohttp.ClientSession() as session:
            redis_trade = GLOBAL_REDIS_BRIDGE or UpstashRedisTradingBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                session
            )
            client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=IS_SANDBOX)
            wallet = await client.get_wallet_balances(QUOTE_CCY)
            total_eq = wallet.get("total_equity", 0.0)
            
            daily_loss = await redis_trade.get_daily_loss()
            is_breaker_active = await redis_trade.is_daily_loss_exceeded(total_eq)

            url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
            active_alts = 0
            async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                if r_k.status == 200:
                    keys = (await r_k.json()).get("result", [])
                    for k in keys:
                        if not any(sym in k for sym in ("BTC", "USDC")):
                            active_alts += 1

            spread_reports = {}
            for sym in [f"BTC-{QUOTE_CCY}", f"ETH-{QUOTE_CCY}", f"SOL-{QUOTE_CCY}", f"XRP-{QUOTE_CCY}"]:
                spread_ok, curr_spread = await client.check_spread_ok(sym)
                spread_reports[sym] = {
                    "current_spread_pct": round(curr_spread * 100.0, 4),
                    "spread_acceptable": spread_ok
                }

            return {
                "total_equity_usdc": total_eq,
                "daily_loss_usdc": round(daily_loss, 2),
                "max_allowed_daily_loss_usdc": round(total_eq * CONFIG["RISK_MANAGEMENT"]["MAX_DAILY_LOSS_PCT"], 2),
                "circuit_breaker_triggered": is_breaker_active,
                "active_altcoins_count": active_alts,
                "max_allowed_alts": CONFIG["MAX_SIMULTANEOUS_ALTS"],
                "spread_checks": spread_reports
            }

    fut = asyncio.run_coroutine_threadsafe(_audit(), BACKGROUND_LOOP)
    try:
        report = fut.result(timeout=10)
        return jsonify({"status": "OK", "risk_telemetry": report}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# =========================================================================
# TOKEN BUCKET RATE LIMITER
# =========================================================================
class TokenBucketRateLimiter:
    def __init__(self, tokens_per_second: float = 4.0, max_capacity: float = 8.0):
        self.rate = tokens_per_second
        self.capacity = max_capacity
        self.tokens = max_capacity
        self.last_check = time.monotonic()
        self._lock: Optional[asyncio.Lock] = None

    async def consume(self):
        if self._lock is None:
            self._lock = asyncio.Lock()
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last_check) * self.rate)
            self.last_check = now
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
                self.last_check = time.monotonic()
            else:
                self.tokens -= 1.0

# =========================================================================
# POMOST UPSTASH REDIS (DWUPOZIOMOWY L1 RAM-FIRST CACHE + TRWAŁOŚĆ POZYCJI)
# =========================================================================
class UpstashRedisTradingBridge:
    def __init__(self, url: str, token: str, session: aiohttp.ClientSession):
        self.url = url.rstrip('/') if url else ""
        self.headers = {
            "Authorization": f"Bearer {token}",
            "Content-Type": "application/json"
        } if token else {}
        self.session = session
        self.prefix = "TRADE_"

        # L1 RAM Caches (Zero zapytań HTTP, natychmiastowy dostęp, zero kosztów Redis)
        self._local_ticks: Dict[str, deque] = defaultdict(lambda: deque(maxlen=50))
        self._pos_cache: Dict[str, Tuple[Optional[Dict[str, Any]], float]] = {}
        self._cooldown_cache: Dict[str, float] = {}
        self._metrics_buffer: Dict[str, int] = defaultdict(int)
        self._last_metrics_flush = time.monotonic()

    def _enforce_prefix(self, key: str) -> str:
        return key if key.startswith(self.prefix) else f"{self.prefix}{key}"

    def _safe_unpack_hex(self, hex_string: str) -> Optional[Dict[str, Any]]:
        if not hex_string or hex_string in ["None", "NULL", "none", "null"]:
            return None
        try:
            clean_hex = hex_string.strip()
            return msgpack.unpackb(bytes.fromhex(clean_hex), strict_map_key=False)
        except Exception:
            return None

    def clear_local_caches(self):
        """Czyści lokalne bufory L1 (CRIT-02 Fix: synchronizacja po resecie awaryjnym)."""
        self._pos_cache.clear()
        self._cooldown_cache.clear()
        self._local_ticks.clear()
        logger.info("🧹 [L1-CACHE-PURGE] Lokalne bufory RAM wyczyszczone pomyślnie.")

    async def push_historical_tick(self, market_id: str, tick_data: Dict[str, Any], max_elements: int = 50) -> bool:
        if market_id not in self._local_ticks:
            self._local_ticks[market_id] = deque(maxlen=max_elements)
        self._local_ticks[market_id].appendleft(tick_data)
        return True

    async def get_historical_ticks(self, market_id: str, max_elements: int = 50) -> List[Dict[str, Any]]:
        ticks = self._local_ticks.get(market_id)
        if ticks:
            return list(ticks)[:max_elements]
        return []

    async def set_position_state(self, pos_key: str, state_data: Dict[str, Any]) -> bool:
        safe_key = self._enforce_prefix(pos_key)
        self._pos_cache[safe_key] = (state_data, time.monotonic())
        if not self.url:
            return False
        try:
            hex_str = msgpack.packb(state_data, use_bin_type=True).hex()
            pipeline_payload = [
                ["LPUSH", safe_key, hex_str],
                ["LTRIM", safe_key, "0", "0"],
                ["EXPIRE", safe_key, "604800"]
            ]
            url = f"{self.url}/pipeline"
            async with self.session.post(url, json=pipeline_payload, headers=self.headers, timeout=5) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS POS SAVE ERROR] {pos_key}: {e}")
            return False

    async def get_position_state(self, pos_key: str) -> Optional[Dict[str, Any]]:
        safe_key = self._enforce_prefix(pos_key)
        now = time.monotonic()
        if safe_key in self._pos_cache:
            cached_data, cached_at = self._pos_cache[safe_key]
            if now - cached_at < 20.0:
                return cached_data

        if not self.url:
            return None
        try:
            url = f"{self.url}/lrange/{safe_key}/0/0"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                if resp.status != 200:
                    return None
                res_json = await resp.json()
                hex_list = res_json.get("result", []) if isinstance(res_json, dict) else []
                if hex_list:
                    data = self._safe_unpack_hex(hex_list[0])
                    self._pos_cache[safe_key] = (data, now)
                    return data
                self._pos_cache[safe_key] = (None, now)
                return None
        except Exception as e:
            logger.error(f"❌ [REDIS POS READ ERROR] {pos_key}: {e}")
            return None

    async def delete_key(self, key: str) -> bool:
        safe_key = self._enforce_prefix(key)
        self._pos_cache.pop(safe_key, None)
        if not self.url:
            return False
        try:
            url = f"{self.url}/del/{safe_key}"
            async with self.session.get(url, headers=self.headers, timeout=4) as resp:
                return resp.status == 200
        except Exception as e:
            logger.error(f"❌ [REDIS DEL ERROR] {key}: {e}")
            return False

    async def incr_metric(self, field_name: str):
        self._metrics_buffer[field_name] += 1
        now = time.monotonic()
        if now - self._last_metrics_flush > 1800.0:
            await self.flush_metrics_to_redis()

    async def flush_metrics_to_redis(self):
        if not self.url or not self._metrics_buffer:
            return
        current_date = datetime.now(UTC).strftime('%Y-%m-%d')
        commands = []
        for field, count in list(self._metrics_buffer.items()):
            key = self._enforce_prefix(f"ANALYTICS:{field}:{current_date}")
            commands.append(["INCRBY", key, str(count)])
        self._metrics_buffer.clear()
        self._last_metrics_flush = time.monotonic()
        try:
            async with self.session.post(f"{self.url}/pipeline", json=commands, headers=self.headers, timeout=5) as resp:
                await resp.read()
        except Exception:
            pass

    async def set_cooldown(self, symbol: str, seconds: int) -> bool:
        self._cooldown_cache[symbol] = time.monotonic() + seconds
        if not self.url:
            return False
        safe_key = self._enforce_prefix(f"COOLDOWN:{symbol}")
        try:
            payload = [["SET", safe_key, "1", "EX", str(seconds)]]
            async with self.session.post(f"{self.url}/pipeline", json=payload, headers=self.headers, timeout=3) as resp:
                return resp.status == 200
        except Exception:
            return False

    async def is_in_cooldown(self, symbol: str) -> bool:
        now = time.monotonic()
        if symbol in self._cooldown_cache:
            if now < self._cooldown_cache[symbol]:
                return True
            else:
                self._cooldown_cache.pop(symbol, None)
                return False

        if not self.url:
            return False
        safe_key = self._enforce_prefix(f"COOLDOWN:{symbol}")
        try:
            async with self.session.get(f"{self.url}/get/{safe_key}", headers=self.headers, timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    has_cd = data.get("result") is not None
                    if has_cd:
                        self._cooldown_cache[symbol] = now + 60.0
                    return has_cd
        except Exception:
            pass
        return False

    async def record_daily_loss(self, loss_usdc: float):
        if not self.url or loss_usdc <= 0:
            return
        today = datetime.now(UTC).strftime('%Y-%m-%d')
        key = self._enforce_prefix(f"DAILY_LOSS:{today}")
        try:
            url_incr = f"{self.url}/incrbyfloat/{key}/{loss_usdc}"
            async with self.session.get(url_incr, headers=self.headers, timeout=3) as resp:
                if resp.status == 200:
                    await self.session.get(f"{self.url}/expire/{key}/172800", headers=self.headers, timeout=2)
        except Exception as e:
            logger.error(f"❌ [REDIS-DAILY-LOSS-WRITE] Błąd zapisu straty: {e}")

    async def get_daily_loss(self) -> float:
        if not self.url:
            return 0.0
        today = datetime.now(UTC).strftime('%Y-%m-%d')
        key = self._enforce_prefix(f"DAILY_LOSS:{today}")
        try:
            async with self.session.get(f"{self.url}/get/{key}", headers=self.headers, timeout=3) as resp:
                if resp.status == 200:
                    res = (await resp.json()).get("result")
                    return float(res) if res is not None else 0.0
        except Exception:
            pass
        return 0.0

    async def is_daily_loss_exceeded(self, total_equity: float) -> bool:
        if total_equity <= 0:
            return False
        daily_loss = await self.get_daily_loss()
        max_allowed = total_equity * CONFIG["RISK_MANAGEMENT"]["MAX_DAILY_LOSS_PCT"]
        if daily_loss >= max_allowed:
            logger.critical(f"🛑 [DAILY CIRCUIT BREAKER] Osiągnięto limit straty! Dziś: {round(daily_loss, 2)} / Limit: {round(max_allowed, 2)} USDC.")
            return True
        return False

# =========================================================================
# DYSPOZYTOR TELEGRAM
# =========================================================================
class TelegramThrottledDispatcher:
    def __init__(self, token: str, chat_id: str, session: aiohttp.ClientSession):
        self.token = token
        self.chat_id = chat_id
        self.session = session

    async def push(self, text: str):
        if not self.token or not self.chat_id:
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with self.session.post(url, json=payload, timeout=10) as response:
                await response.read()
        except Exception:
            pass

# =========================================================================
# RDZENIE OBLICZENIOWE QUANT
# =========================================================================
class AlgorithmicQuantCore:
    @staticmethod
    def _calculate_ema(prices: List[float], period: int = 15) -> float:
        if len(prices) < period:
            return prices[-1] if prices else 0.0
        k = 2.0 / (period + 1.0)
        ema = sum(prices[:period]) / period 
        for p in prices[period:]:
            ema = (p * k) + (ema * (1.0 - k))
        return ema

    @staticmethod
    def _calculate_rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1:
            return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(1, period + 1):
            change = prices[-i] - prices[-(i + 1)]
            if change > 0:
                gains += change
            else:
                losses -= change
        if losses == 0.0:
            return 100.0
        rs = (gains / period) / (losses / period)
        return 100.0 - (100.0 / (1.0 + rs))

    @staticmethod
    def calculate_z_score_from_candles(candles_15m: List[List[str]], current_price: float) -> Optional[Dict[str, Any]]:
        """Kwantowe wyliczanie Z-Score na świecach makro 15m (CRIT-04 Fix: eliminacja szumu 20-minutowego)."""
        period = CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["LOOKBACK_PERIOD"]
        if len(candles_15m) < period:
            return None

        closes = [float(c[4]) for c in candles_15m]
        highs = [float(c[2]) for c in candles_15m]
        lows = [float(c[3]) for c in candles_15m]

        prices = closes[-period:]
        sma = sum(prices) / float(period)
        variance = sum((x - sma) ** 2 for x in prices) / float(period)
        std_dev = math.sqrt(variance) if variance > 0 else 1e-6
            
        z_score = (current_price - sma) / std_dev

        ema_trend = AlgorithmicQuantCore._calculate_ema(closes, period=15)
        trend_direction = "LONG_ONLY" if current_price >= ema_trend else "SHORT_ONLY"

        rsi_val = AlgorithmicQuantCore._calculate_rsi(closes, period=14)
        bandwidth = (std_dev * 4.0) / sma if sma != 0 else 0.0

        tr_list = []
        for i in range(1, min(15, len(candles_15m))):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(tr_list) / len(tr_list) if tr_list else std_dev * 0.5

        return {
            "current": current_price,
            "sma": round(sma, 6),
            "z_score": round(z_score, 4),
            "trend": trend_direction,
            "rsi": round(rsi_val, 2),
            "bandwidth": round(bandwidth, 4),
            "atr": round(atr, 6)
        }

class MomentumQuantCore:
    @staticmethod
    def calculate_momentum(candles: List[List[str]], period: int = 10) -> Optional[Dict[str, Any]]:
        if len(candles) < period + 2:
            return None
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]

        current_price = closes[-1]
        past_price = closes[-period - 1]
        if past_price == 0:
            return None
        roc = ((current_price - past_price) / past_price) * 100.0

        tr_list = []
        for i in range(1, min(15, len(candles))):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(tr_list) / len(tr_list) if tr_list else 0.0

        roc_threshold = CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ROC_TRIGGER"]
        return {
            "roc": round(roc, 2),
            "current": current_price,
            "atr": atr,
            "signal": roc > roc_threshold
        }

class BreakoutQuantCore:
    @staticmethod
    def calculate_breakout(candles: List[List[str]], period: int = 20) -> Optional[Dict[str, Any]]:
        if len(candles) < period:
            return None
        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]

        current_price = closes[-1]
        sma = sum(closes[-period:]) / period
        variance = sum((x - sma) ** 2 for x in closes[-period:]) / period
        std_dev = math.sqrt(variance) if variance > 0 else 1e-6
        
        upper_band = sma + (2.0 * std_dev)
        lower_band = sma - (2.0 * std_dev)
        bandwidth = (upper_band - lower_band) / sma if sma > 0 else 0.0
        
        comp_thresh = CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["COMPRESSION_BANDWIDTH"]
        is_compression = bandwidth < comp_thresh
        is_breakout_up = current_price > upper_band
        
        tr_list = []
        for i in range(1, min(15, len(candles))):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr_list.append(max(h - l, abs(h - prev_c), abs(l - prev_c)))
        atr = sum(tr_list) / len(tr_list) if tr_list else 0.0

        return {
            "bandwidth": round(bandwidth, 4),
            "upper_band": round(upper_band, 4),
            "current": current_price,
            "atr": atr,
            "signal": is_compression and is_breakout_up
        }

class GridQuantCore:
    @staticmethod
    def calculate_grid_levels(candles: List[List[str]], symbol: str, grid_step_pct: float = 0.005, levels: int = 3) -> Optional[Dict[str, Any]]:
        if len(candles) < 20:
            return None

        closes = [float(c[4]) for c in candles]
        highs = [float(c[2]) for c in candles]
        lows = [float(c[3]) for c in candles]
        current_price = closes[-1]

        tr_list = []
        for i in range(1, 15):
            h = highs[-i]
            l = lows[-i]
            prev_c = closes[-(i + 1)]
            tr = max(h - l, abs(h - prev_c), abs(l - prev_c))
            tr_list.append(tr)
        atr = sum(tr_list) / len(tr_list)
        atr_pct = (atr / current_price) * 100.0

        past_price = closes[-11] if len(closes) >= 11 else closes[0]
        roc = ((current_price - past_price) / past_price) * 100.0 if past_price > 0 else 0.0
        
        is_alt = not any(sym in symbol for sym in ("BTC", "ETH"))
        max_atr = CONFIG["STRATEGY_PARAMS"]["GRID"]["MAX_ATR_ALTS"] if is_alt else CONFIG["STRATEGY_PARAMS"]["GRID"]["MAX_ATR_MAJORS"]
        is_consolidation = (0.2 <= atr_pct <= max_atr) and (abs(roc) < 1.0)

        buy_levels = []
        for k in range(1, levels + 1):
            price_buy = round(current_price * (1.0 - (k * grid_step_pct)), 4)
            price_tp = round(price_buy * (1.0 + grid_step_pct), 4)
            buy_levels.append({
                "level": k,
                "buy_price": price_buy,
                "tp_price": price_tp
            })

        return {
            "current_price": current_price,
            "atr_pct": round(atr_pct, 2),
            "max_atr_threshold": max_atr,
            "roc": round(roc, 2),
            "is_consolidation": is_consolidation,
            "levels": buy_levels
        }

class MarketRegimeArbitrator:
    _cache: Dict[str, Dict[str, Any]] = {}
    _TTL: float = 120.0

    @classmethod
    async def get_candles(cls, okx_client, symbol: str) -> List[List[str]]:
        now = time.monotonic()
        if symbol in cls._cache and (now - cls._cache[symbol]["time"] < cls._TTL):
            return cls._cache[symbol]["data"]

        candles = await okx_client.get_macro_candles_raw(symbol, bar="15m", limit=30)
        if candles:
            cls._cache[symbol] = {"data": candles, "time": now}
        return candles or []

    @staticmethod
    def get_regime(candles: List[List[str]]) -> str:
        if len(candles) < 20:
            return "NEUTRAL"

        closes = [float(c[4]) for c in candles]
        sma = sum(closes[-20:]) / 20.0
        variance = sum((x - sma) ** 2 for x in closes[-20:]) / 20.0
        std_dev = math.sqrt(variance) if variance > 0 else 1e-6
        bandwidth = (std_dev * 4.0) / sma if sma > 0 else 0.0

        if bandwidth > 0.020:
            return "TRENDING"
        elif bandwidth <= 0.015:
            return "RANGING"
        return "NEUTRAL"

# =========================================================================
# OKX WEBSOCKET PRICE FEED Z WATCHDOGIEM
# =========================================================================
class OKXWebSocketPriceFeed:
    def __init__(self, session: aiohttp.ClientSession, is_sandbox: bool = True):
        self.session = session
        self.is_sandbox = is_sandbox
        self.ws_url = "wss://wspap.okx.com:8443/ws/v5/public" if is_sandbox else "wss://ws.okx.com:8443/ws/v5/public"
        self.latest_prices: Dict[str, float] = {}
        self.last_msg_time = time.monotonic()
        self._running: bool = False

    async def start_listener(self, symbols: list):
        self._running = True
        sub_args = [{"channel": "tickers", "instId": sym} for sym in symbols]
        subscribe_msg = json.dumps({"op": "subscribe", "args": sub_args})

        while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                mode_str = "SANDBOX" if self.is_sandbox else "LIVE"
                logger.info(f"🌐 [WS-CONNECT] Łączenie ze strumieniem OKX ({mode_str})...")
                async with self.session.ws_connect(self.ws_url, heartbeat=20) as ws:
                    await ws.send_str(subscribe_msg)
                    logger.info(f"📡 [WS-SUBSCRIBED] Subskrypcja aktywna dla {symbols}")
                    self.last_msg_time = time.monotonic()

                    while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
                        try:
                            msg = await asyncio.wait_for(ws.receive(), timeout=45.0)
                        except asyncio.TimeoutError:
                            logger.warning("⚠️ [WS-WATCHDOG] Brak pakietów przez 45s. Resetowanie połączenia WebSocket...")
                            break

                        if msg.type == aiohttp.WSMsgType.TEXT:
                            self.last_msg_time = time.monotonic()
                            data = json.loads(msg.data)
                            if "data" in data and len(data["data"]) > 0:
                                ticker = data["data"][0]
                                inst_id = ticker.get("instId")
                                last_price = ticker.get("last")
                                if inst_id and last_price:
                                    prev_p = self.latest_prices.get(inst_id)
                                    self.latest_prices[inst_id] = float(last_price)
                                    if prev_p is None:
                                        logger.info(f"📡 [WS-FEED] Odebrano pierwszy kurs dla {inst_id}: {last_price} {QUOTE_CCY}")
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("⚠️ [WS-DISCONNECTED] Gniazdo zamknięte. Ponawianie...")
                            break
            except Exception as e:
                logger.error(f"❌ [WS-ERROR] Awaria strumienia cen: {e}. Wznawianie za 5s...")
                await asyncio.sleep(5)

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self.latest_prices.get(symbol)

# =========================================================================
# SYSTEMOWY KLIENT GIEŁDY OKX SPOT V5
# =========================================================================
class OKXSpotClient:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter, is_sandbox: bool = True):
        self.base_url = os.environ.get("OKX_API_URL", "https://eea.okx.com").rstrip('/')
        self.session = session
        self.rate_limiter = rate_limiter
        self.is_sandbox = is_sandbox
        self.api_key = os.environ.get("OKX_API_KEY", "").strip()
        self.secret_key = os.environ.get("OKX_SECRET_KEY", "").strip()
        self.passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
        self._sentiment_cache: Dict[str, Tuple[float, float]] = {}

    def _generate_timestamp(self) -> str:
        return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        message = f"{timestamp}{method.upper()}{request_path}{body}"
        mac = hmac.new(self.secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
        return base64.b64encode(mac.digest()).decode('utf-8')

    def _get_headers(self, method: str, request_path: str, body: str = "") -> Dict[str, str]:
        timestamp = self._generate_timestamp()
        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": self.api_key,
            "OK-ACCESS-SIGN": self._sign(timestamp, method, request_path, body),
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": self.passphrase
        }
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"
        return headers

    async def check_spread_ok(self, symbol: str) -> Tuple[bool, float]:
        """Strażnik Spreadu: blokuje Market Buy przy rozjechanym arkuszu."""
        try:
            await self.rate_limiter.consume()
            request_path = f"/api/v5/market/ticker?instId={symbol}"
            url = f"{self.base_url}{request_path}"
            headers = {"Content-Type": "application/json"}
            if self.is_sandbox:
                headers["x-simulated-trading"] = "1"

            async with self.session.get(url, headers=headers, timeout=3) as resp:
                if resp.status == 200:
                    data = await resp.json()
                    if data.get("code") == "0" and data.get("data"):
                        ticker = data["data"][0]
                        bid = float(ticker.get("bidPx", 0.0))
                        ask = float(ticker.get("askPx", 0.0))
                        if bid > 0 and ask > 0:
                            mid = (ask + bid) / 2.0
                            spread_pct = (ask - bid) / mid
                            max_spread = CONFIG.get("MAX_BID_ASK_SPREAD_PCT", 0.0012)
                            if spread_pct > max_spread:
                                logger.warning(f"⚠️ [SPREAD-GUARD] {symbol} spread za szeroki: {round(spread_pct*100, 3)}% (Max: {round(max_spread*100, 3)}%). Odrzucam zlecenie.")
                                return False, spread_pct
                            return True, spread_pct
        except Exception as e:
            logger.error(f"❌ [SPREAD-CHECK-ERROR] {symbol}: {e}")
        return True, 0.0

    async def get_smart_money_sentiment(self, ccy: str = "BTC") -> Optional[float]:
        now = time.monotonic()
        ttl = CONFIG.get("SMART_MONEY", {}).get("CACHE_TTL_SEC", 180)
        if ccy in self._sentiment_cache:
            val, ts = self._sentiment_cache[ccy]
            if now - ts < ttl:
                return val

        request_path = f"/api/v5/rubik/stat/contracts/long-short-account-ratio?ccy={ccy}&period=5m"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"

        try:
            async with self.session.get(url, headers=headers, timeout=4) as resp:
                if resp.status != 200:
                    if ccy != "BTC":
                        return await self.get_smart_money_sentiment("BTC")
                    return None
                data = await resp.json()
                if data.get("code") == "0" and data.get("data") and len(data["data"]) > 0:
                    raw_ratio = data["data"][0][1]
                    ratio = float(raw_ratio)
                    self._sentiment_cache[ccy] = (ratio, now)
                    return ratio
                elif ccy != "BTC":
                    return await self.get_smart_money_sentiment("BTC")
                return None
        except Exception as e:
            logger.warning(f"⚠️ [OKX-SMART-MONEY] Błąd sentymentu dla {ccy}: {e}")
            if ccy != "BTC":
                return await self.get_smart_money_sentiment("BTC")
            return None

    async def get_wallet_balances(self, ccy: str = "USDC") -> Dict[str, float]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return {"total_equity": 0.0, "available_cash": 0.0}

        await self.rate_limiter.consume()
        request_path = f"/api/v5/account/balance?ccy={ccy}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                code = data.get("code")
                if code == "0" and data.get("data"):
                    account_data = data["data"][0]
                    total_eq = float(account_data.get("totalEq", 0.0))
                    avail_cash = 0.0
                    details = account_data.get("details", [])
                    for bal in details:
                        if bal.get("ccy") == ccy:
                            avail_cash = float(bal.get("availBal", 0.0))
                            break
                    return {"total_equity": total_eq, "available_cash": avail_cash}
                return {"total_equity": 0.0, "available_cash": 0.0}
        except Exception as e:
            logger.error(f"❌ [OKX-WALLET] Błąd odczytu portfela: {e}")
            return {"total_equity": 0.0, "available_cash": 0.0}

    async def get_account_balance(self, ccy: str = "USDC") -> float:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return 0.0

        await self.rate_limiter.consume()
        request_path = f"/api/v5/account/balance?ccy={ccy}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                data = await resp.json()
                code = data.get("code")
                if code == "0" and data.get("data"):
                    account_data = data["data"][0]
                    for bal in account_data.get("details", []):
                        if bal.get("ccy") == ccy:
                            return float(bal.get("availBal", 0.0))
                    if ccy == QUOTE_CCY:
                        return float(account_data.get("totalEq", 0.0))
                return 0.0
        except Exception as e:
            logger.error(f"❌ [OKX-BALANCE] Błąd odczytu salda {ccy}: {e}")
            return 0.0

    async def wait_for_settled_balance(self, ccy: str, expected_min: float, max_attempts: int = 3) -> float:
        for attempt in range(max_attempts):
            await asyncio.sleep(0.35 * (attempt + 1))
            bal = await self.get_account_balance(ccy)
            if bal >= expected_min * 0.98:
                return bal
        return await self.get_account_balance(ccy)

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        if GLOBAL_WS_FEED:
            ws_price = GLOBAL_WS_FEED.get_last_price(symbol)
            if ws_price and ws_price > 0.0:
                return {"source": "OKX_WS", "symbol": symbol, "last": ws_price}

        await self.rate_limiter.consume()
        request_path = f"/api/v5/market/ticker?instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    ticker_info = data["data"][0]
                    return {"source": "OKX_SPOT", "symbol": symbol, "last": float(ticker_info.get("last", 0.0))}
                return None
        except Exception as e:
            logger.error(f"[OKX-TICKER] Błąd kursu {symbol}: {e}")
            return None

    async def get_macro_candles_raw(self, symbol: str, bar: str = "15m", limit: int = 30) -> List[List[str]]:
        await self.rate_limiter.consume()
        request_path = f"/api/v5/market/candles?instId={symbol}&bar={bar}&limit={limit}"
        url = f"{self.base_url}{request_path}"
        headers = {"Content-Type": "application/json"}
        if self.is_sandbox:
            headers["x-simulated-trading"] = "1"

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return []
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return list(reversed(data["data"]))
                return []
        except Exception as e:
            logger.error(f"[OKX-RAW-CANDLES] Błąd świec surowych {symbol}: {e}")
            return []

    async def execute_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        
        request_path = "/api/v5/trade/order"
        body_dict = {
            "instId": symbol,
            "tdMode": "cash",
            "side": side.lower(),
            "ordType": "market",
            "sz": str(quantity),
            "tgtCcy": "base_ccy"
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"[OKX-ORDER] Błąd Market {side} dla {symbol}: {e}")
            return None

    async def has_open_orders(self, symbol: str) -> bool:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return False
        await self.rate_limiter.consume()

        request_path = f"/api/v5/trade/orders-pending?instId={symbol}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return True
                data = await resp.json()
                if data.get("code") == "0":
                    return len(data.get("data", [])) > 0
                return True
        except Exception as e:
            logger.error(f"❌ [OKX-PENDING] Błąd sprawdzania zleceń {symbol}: {e}")
            return True

    async def get_algo_order_state(self, algo_id: str) -> Tuple[Optional[str], Optional[float]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None, None
        await self.rate_limiter.consume()

        request_path = f"/api/v5/trade/order-algo?algoId={algo_id}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return None, None
                data = await resp.json()
                code = str(data.get("code", ""))
                if code == "0" and data.get("data"):
                    item = data["data"][0]
                    state = item.get("state")
                    actual_px_str = item.get("actualPx") or item.get("slTriggerPx") or item.get("tpTriggerPx") or "0"
                    try:
                        actual_px = float(actual_px_str)
                    except ValueError:
                        actual_px = None
                    return state, actual_px
<comment-tag id="3">                if code in ("51402", "51401", "51410", "51415"):
                    return "effective", None
                return None, None</comment-tag id="3" text="Zmień na:
                if (code == '0' and not data.get('data')) or code in ('51402', '51401', '51410', '51415'):
                    return 'effective', None
                return None, None
Gdy zlecenie OCO zostało zrealizowane lub wygaszone, OKX zwraca code 0 i pustą listę data: []. Uznanie tego stanu za terminalny (effective) zapobiega zapętleniu procedury likwidacji." type="suggestion">
        except Exception as e:
            logger.error(f"❌ [OKX-ALGO-STATE] Błąd zlecenia Algo {algo_id}: {e}")
            return None, None

    async def get_order_state(self, symbol: str, ord_id: str) -> Tuple[Optional[str], Optional[float]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None, None
        await self.rate_limiter.consume()

        request_path = f"/api/v5/trade/order?instId={symbol}&ordId={ord_id}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return None, None
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    item = data["data"][0]
                    state = item.get("state")
                    fill_px_str = item.get("fillPx") or item.get("px") or "0"
                    try:
                        fill_px = float(fill_px_str)
                    except ValueError:
                        fill_px = None
                    return state, fill_px
                return None, None
        except Exception as e:
            logger.error(f"❌ [OKX-ORDER-STATE] Błąd statusu zlecenia {ord_id}: {e}")
            return None, None

    async def execute_limit_order(self, symbol: str, side: str, quantity: float, price: float) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        
        request_path = "/api/v5/trade/order"
        body_dict = {
            "instId": symbol,
            "tdMode": "cash",
            "side": side.lower(),
            "ordType": "limit",
            "px": str(price),
            "sz": str(quantity)
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"[OKX-LIMIT-ORDER] Błąd Limit {side} {symbol}: {e}")
            return None

    async def cancel_order(self, symbol: str, ord_id: str) -> bool:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return False
        await self.rate_limiter.consume()

        request_path = "/api/v5/trade/cancel-order"
        body_dict = {"instId": symbol, "ordId": ord_id}
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                data = await r.json()
                return data.get("code") == "0"
        except Exception as e:
            logger.error(f"❌ [OKX-CANCEL-ORDER] Błąd anulowania {ord_id}: {e}")
            return False

    async def cancel_algo_order(self, symbol: str, algo_id: str) -> bool:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return False
        await self.rate_limiter.consume()

<comment-tag id="4">        request_path = "/api/v5/trade/cancel-algo-orders"</comment-tag id="4" text="Zmień na:
        request_path = '/api/v5/trade/cancel-algos'
Zgodnie z oficjalną specyfikacją OKX API v5 właściwy endpoint do odwoływania zleceń algorytmicznych OCO to /api/v5/trade/cancel-algos (bez przyrostka -orders). Poprzednia ścieżka zwracała błąd 404, blokując likwidację przeterminowanych pozycji przez Strażnika Czasu." type="suggestion">
        body_dict = [{"instId": symbol, "algoId": str(algo_id)}]
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                data = await r.json()
                if data.get("code") == "0" and data.get("data"):
                    item = data["data"][0]
                    s_code = str(item.get("sCode", ""))
                    SAFE_TERMINAL_CODES = ("0", "51410", "51401", "51415", "51402", "51400")
                    if s_code in SAFE_TERMINAL_CODES:
                        return True
<comment-tag id="5">                    logger.warning(f"⚠️ [OKX-ALGO-CANCEL] Nieobsługiwany sCode {s_code}: {item.get('sMsg')}")
                    return False
                return False</comment-tag id="5" text="Zmień na:
                    logger.warning(f'⚠️ [OKX-ALGO-CANCEL] Nieobsługiwany sCode {s_code}: {item.get(\'sMsg\')}')
                    return False
                logger.warning(f'⚠️ [OKX-ALGO-CANCEL-REJECTED] Błąd OKX: {data.get(\'code\')} - {data.get(\'msg\')}')
                return False
Dodanie jawnego logowania kodu i treści błędu ułatwia natychmiastową diagnostykę, gdyby giełda odrzuciła anulowanie zlecenia." type="suggestion">
        except Exception as e:
            logger.error(f"❌ [OKX-CANCEL-ALGO] Błąd OCO {algo_id}: {e}")
            return False

    async def amend_algo_order(self, symbol: str, algo_id: str, new_sl_trigger_px: float) -> bool:
        """Atomowa modyfikacja w locie (In-Place Amendment) wyzwalacza Stop Loss w aktywnym zleceniu OCO."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return False
        await self.rate_limiter.consume()

        request_path = "/api/v5/trade/amend-algos"
        body_dict = {
            "instId": symbol,
            "algoId": str(algo_id),
            "newSlTriggerPx": str(new_sl_trigger_px),
            "newSlOrdPx": "-1"
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                data = await r.json()
                if data.get("code") == "0" and data.get("data"):
                    item = data["data"][0]
                    s_code = str(item.get("sCode", ""))
                    if s_code == "0":
                        logger.info(f"🛡️ [OKX-AMEND-SUCCESS] Zaktualizowano OCO {algo_id} ({symbol}): newSlTriggerPx={new_sl_trigger_px}")
                        return True
                    logger.warning(f"⚠️ [OKX-AMEND-REJECTED] Odrzucono modyfikację OCO {algo_id} (sCode {s_code}): {item.get('sMsg')}")
                    return False
                logger.warning(f"⚠️ [OKX-AMEND-ERROR] Błąd API OKX: {data.get('code')} - {data.get('msg')}")
                return False
        except Exception as e:
            logger.error(f"❌ [OKX-AMEND-EX] Wyjątek modyfikacji OCO {algo_id}: {e}")
            return False

    async def execute_oco_protection(self, symbol: str, quantity: float, price_tp: float, price_sl: float) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()

        request_path = "/api/v5/trade/order-algo"
        body_dict = {
            "instId": symbol,
            "tdMode": "cash",
            "side": "sell",
            "ordType": "oco",
            "sz": str(quantity),
            "tpTriggerPx": str(price_tp),
            "tpOrdPx": "-1",
            "slTriggerPx": str(price_sl),
            "slOrdPx": "-1"
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res = await r.json()
                logger.info(f"🛡️ [OKX-OCO-DEPLOYED] Zlecenie OCO {symbol}: TP={price_tp}, SL={price_sl}")
                return res
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-CRITICAL] Awaria OCO dla {symbol}: {e}")
            return None

# =========================================================================
# WSPÓLNA PROCEDURA RECONCILIACJI I STRAŻNIKA CZASU Z ATOMOWYM FALLBACKIEM BE
# =========================================================================
async def reconcile_and_timestop(
    inst: Dict[str, Any], 
    strategy_type: str, 
    redis_trade: UpstashRedisTradingBridge, 
    tg: TelegramThrottledDispatcher
) -> Tuple[bool, Optional[str]]:
    """Uniwersalny moduł sprawdzania stanu zlecenia OCO, Break Even z fallbackiem oraz wygaszania pozycji po czasie."""
    pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
    pos_data = await redis_trade.get_position_state(pos_key)
    if not pos_data:
        return False, None

    if pos_data.get("status") == "WAITING_OCO" and "algo_id" in pos_data:
        algo_id = pos_data["algo_id"]
        algo_state, actual_px = await inst["client"].get_algo_order_state(algo_id)

        opened_at = float(pos_data.get("time", time.time()))
        elapsed_time = time.time() - opened_at
        max_timeout = CONFIG["TIMEOUTS"].get(strategy_type, 28800)

        TERMINAL_ALGO_STATES = ("effective", "filled", "canceled", "order_failed")

        # 1. Sprawdzenie Strażnika Czasu (Time-Stop)
        if algo_state not in TERMINAL_ALGO_STATES and elapsed_time > max_timeout:
            logger.warning(f"⏳ [TIME-STOP EXPIRED] Pozycja {inst['label']} ({strategy_type}) przekroczyła {round(max_timeout/3600, 1)}h. Likwidacja...")
            
            cancel_success = False
            for attempt in range(3):
                if await inst["client"].cancel_algo_order(inst["symbol"], algo_id):
                    cancel_success = True
                    break
                await asyncio.sleep(1.0)
            
            if not cancel_success:
                logger.error(f"❌ [TIME-STOP CRITICAL] Nie udało się anulować OCO {algo_id} dla {inst['label']} po 3 próbach! Przerwanie likwidacji.")
                return False, None
            
            base_ccy = inst["symbol"].split("-")[0]
            qty_to_sell = float(pos_data.get("qty", 0.0))
            avail_bal = await inst["client"].wait_for_settled_balance(base_ccy, qty_to_sell * 0.95, max_attempts=3)
            
            if avail_bal >= inst["min_qty"]:
                sell_qty = floor_to_precision(min(qty_to_sell, avail_bal), inst["round_digits"])
                sell_qty = max(inst["min_qty"], sell_qty)
                await inst["client"].execute_market_order(inst["symbol"], "sell", sell_qty)

            await redis_trade.delete_key(pos_key)
            logger.info(f"🔓 [SLOT-FREED] Zwolniono przeterminowany slot dla {inst['label']}.")
            await tg.push(
                f"⏳ <b>[STRAŻNIK CZASU: {inst['label']}] • TIME-STOP EXPIRED</b>\n"
                f"──────────────────────────────\n"
                f"📈 Strategia: <b>{strategy_type}</b>\n"
                f"⌛ Czas trwania: <b>{round(elapsed_time/3600, 1)}h</b> (Limit: {round(max_timeout/3600, 1)}h)\n"
                f"💰 Pozycja zamknięta po cenie rynkowej do {QUOTE_CCY}.\n"
                f"Slot zwolniony. Kapitał w gotówce."
            )
            return True, pos_key

        # 2. Sprawdzenie i Aktywacja Break Even (amend-algos z fallbackiem do Cancel & Replace)
        be_config = CONFIG.get("BREAK_EVEN", {})
        if be_config.get("ENABLED", True) and algo_state not in TERMINAL_ALGO_STATES and not pos_data.get("be_applied", False):
            buy_p = float(pos_data.get("buy_price", 0.0))
            tp_p = float(pos_data.get("tp_price", 0.0))
            old_sl = float(pos_data.get("sl_price", 0.0))

            if buy_p > 0 and tp_p > buy_p:
                trigger_ratio = CONFIG["STRATEGY_PARAMS"].get(strategy_type, {}).get("BE_TRIGGER_RATIO", 0.60)
                be_trigger_px = buy_p + (trigger_ratio * (tp_p - buy_p))

                curr_px = GLOBAL_WS_FEED.get_last_price(inst["symbol"]) if GLOBAL_WS_FEED else None
                if not curr_px:
                    ticker = await inst["client"].get_market_ticker(inst["symbol"])
                    curr_px = ticker.get("last", 0.0) if ticker else 0.0

                if curr_px and curr_px >= be_trigger_px:
                    fee_buffer = be_config.get("FEE_BUFFER_PCT", 0.0022)
                    new_sl_price = round(buy_p * (1.0 + fee_buffer), inst["price_round"])

                    if new_sl_price > old_sl and curr_px > new_sl_price:
                        amend_success = await inst["client"].amend_algo_order(inst["symbol"], algo_id, new_sl_price)
                        
                        # FALLBACK W RAZIE ODRZUCENIA AMEND-ALGOS PRZEZ SILNIK OKX
                        if not amend_success:
                            logger.warning(f"⚠️ [AMEND-FALLBACK] OKX odrzucił amend-algos dla {inst['label']}. Wykonuję bezpieczny Cancel & Replace...")
                            cancel_ok = await inst["client"].cancel_algo_order(inst["symbol"], algo_id)
                            if cancel_ok:
                                await asyncio.sleep(0.5)
                                oco_res = await inst["client"].execute_oco_protection(inst["symbol"], float(pos_data["qty"]), tp_p, new_sl_price)
                                if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                    pos_data["algo_id"] = oco_res["data"][0].get("algoId", "")
                                    amend_success = True

                        if amend_success:
                            pos_data["be_applied"] = True
                            pos_data["sl_price"] = new_sl_price
                            await redis_trade.set_position_state(pos_key, pos_data)
                            gain_pct = round(((curr_px - buy_p) / buy_p) * 100.0, 2)
                            logger.info(f"🛡️ [BREAK-EVEN ACTIVE] {inst['label']} ({strategy_type}): SL w locie -> {new_sl_price} {QUOTE_CCY} (+{round(fee_buffer*100, 2)}%) przy kursie {curr_px} (+{gain_pct}%).")
                            await tg.push(
                                f"🛡️ <b>[BREAK EVEN: {inst['label']}] • POZYCJA ZABEZPIECZONA</b>\n"
                                f"──────────────────────────────\n"
                                f"📈 Strategia: <b>{strategy_type}</b>\n"
                                f"💰 Wejście: <b>{buy_p} {QUOTE_CCY}</b> | Aktualny: <b>{curr_px} {QUOTE_CCY} (+{gain_pct}%)</b>\n"
                                f"🎯 Take Profit: <code>{tp_p} {QUOTE_CCY}</code>\n"
                                f"🛑 Nowy Stop Loss (BE): <code>{new_sl_price} {QUOTE_CCY} (+{round(fee_buffer*100, 2)}%)</code>\n"
                                f"──────────────────────────────\n"
                                f"🔒 <b>Prowizje zabezpieczone. Ryzyko wyzerowane.</b>"
                            )

        # 3. Sprawdzenie realizacji na giełdzie (Effective / Filled / Canceled)
        if algo_state in TERMINAL_ALGO_STATES:
            logger.info(f"🧹 [RECONCILE] Zlecenie OCO {inst['label']} zakończone stanem: {algo_state}.")
            await redis_trade.delete_key(pos_key)

            if algo_state in ("effective", "filled"):
                buy_p = float(pos_data.get("buy_price", 0.0))
                qty_p = float(pos_data.get("qty", 0.0))
                tp_p = float(pos_data.get("tp_price", buy_p * 1.03))
                sl_p = float(pos_data.get("sl_price", buy_p * 0.98))

                if actual_px and actual_px > 0:
                    exit_p = actual_px
                else:
                    t_now = await inst["client"].get_market_ticker(inst["symbol"])
                    curr_p = t_now.get("last", 0.0) if t_now else 0.0
                    exit_p = sl_p if curr_p > 0 and abs(curr_p - sl_p) < abs(curr_p - tp_p) else tp_p

                pnl_gross = (exit_p - buy_p) * qty_p
                fees = (buy_p * qty_p * 0.001) + (exit_p * qty_p * 0.001)
                pnl_net = round(pnl_gross - fees, 2)
                roe_net = round((pnl_net / (buy_p * qty_p)) * 100.0, 2) if buy_p > 0 else 0.0

                if pnl_net >= 0:
                    await tg.push(
                        f"🎉 <b>[ZYSK: {inst['label']}] • TAKE PROFIT</b>\n"
                        f"──────────────────────────────\n"
                        f"📈 Strategia: <b>{strategy_type}</b>\n"
                        f"💰 Wyjście: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {buy_p} {QUOTE_CCY})\n"
                        f"📦 Wielkość: <b>{qty_p}</b>\n"
                        f"──────────────────────────────\n"
                        f"💵 <b>Zysk netto: +{pnl_net} {QUOTE_CCY} (+{roe_net}%)</b>\n"
                        f"🛡️ Prowizja giełdowa: uwzględniona\n"
                        f"Slot zwolniony. Kapitał w gotówce."
                    )
                else:
                    cooldown_sec = CONFIG.get("RISK_MANAGEMENT", {}).get("COOLDOWN_AFTER_SL_SEC", 14400)
                    await redis_trade.set_cooldown(inst["symbol"], cooldown_sec)
                    await redis_trade.record_daily_loss(abs(pnl_net))
                    logger.info(f"❄️ [COOLDOWN] Nałożono kwarantannę na {inst['label']} na {cooldown_sec//3600}h po zaliczeniu SL. Zarejestrowano stratę: {abs(pnl_net)} USDC.")
                    await tg.push(
                        f"🛑 <b>[STOP LOSS: {inst['label']}] • OCHRONA</b>\n"
                        f"──────────────────────────────\n"
                        f"📈 Strategia: <b>{strategy_type}</b>\n"
                        f"💰 Wyjście: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {buy_p} {QUOTE_CCY})\n"
                        f"📦 Wielkość: <b>{qty_p}</b>\n"
                        f"──────────────────────────────\n"
                        f"📉 <b>Strata netto: {pnl_net} {QUOTE_CCY} ({roe_net}%)</b>\n"
                        f"🛡️ Prowizja giełdowa: uwzględniona\n"
                        f"Slot zwolniony. Kwarantanna: {cooldown_sec//3600}h."
                    )
            return True, pos_key

    return False, None

# =========================================================================
# STRATEGIA 1: INDEPENDENT MEAN REVERSION WORKER (ŚWIECE 15m + FAIL-SAFE CRIT-01 FIX)
# =========================================================================
async def independent_mean_reversion_worker(session, redis_trade, tg, okx_client):
    logger.info("🌊 [MEAN-REV-WORKER] Uruchomiono autonomiczny wątek Mean Reversion (15m Macro Quant) w tle.")
    instruments = [
        {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}_MR", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
        {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}_MR", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
        {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}_MR", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
        {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}_MR", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop(inst, "MEAN_REVERSION", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                if await redis_trade.is_in_cooldown(inst["symbol"]):
                    continue

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
                    continue

                ticker = await inst["client"].get_market_ticker(inst["symbol"])
                if not ticker:
                    continue

                current_price = ticker.get("last", 0.0)
                candles_15m = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if len(candles_15m) < CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["LOOKBACK_PERIOD"]:
                    continue

                # KWANTOWE PRÓBKOWANIE Z-SCORE ZE ŚWIEC 15M (CRIT-04 FIX)
                metrics = AlgorithmicQuantCore.calculate_z_score_from_candles(candles_15m, current_price)
                if not metrics:
                    continue

                z = metrics["z_score"]
                rsi = metrics["rsi"]
                trend = metrics["trend"]
                atr = metrics["atr"]
                bandwidth = metrics["bandwidth"]

                logger.info(f"📊 [MEAN-REV-SCAN-15M] {inst['label']} | P: {current_price} | Z: {z} | RSI: {rsi} | Bw: {bandwidth} | Trend: {trend}")
                await redis_trade.incr_metric(f"ticks_{inst['label']}")

                if bandwidth < 0.001:
                    continue

                std_buy = (z <= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["Z_BUY_STANDARD"] and 
                           trend == "LONG_ONLY" and 
                           rsi <= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RSI_STANDARD"])
                crash_buy = (z <= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["Z_BUY_CRASH"] and 
                             rsi <= CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RSI_CRASH"])

                if std_buy or crash_buy:
                    async with GLOBAL_ALPHA_LOCK:
                        # TARCZA WYŁĄCZNIKA DZIENNEJ STRATY
                        wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        tot_eq = wallet_check.get("total_equity", 0.0)
                        if await redis_trade.is_daily_loss_exceeded(tot_eq):
                            continue

                        # TARCZA KORELACJI SEKTOROWEJ (MAX 1 ALTCOIN)
                        is_alt = not any(sym in inst["symbol"] for sym in ("BTC", "USDC"))
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            logger.info(f"🛡️ [ALPHA LIMIT] 3/3 sloty zajęte. Odrzucam Mean Rev dla {inst['label']}.")
                            continue

                        if is_alt:
                            alt_count = sum(1 for k in active_keys if not any(sym in k for sym in ("BTC", "USDC")))
                            if alt_count >= CONFIG.get("MAX_SIMULTANEOUS_ALTS", 1):
                                logger.info(f"🛡️ [CORRELATION-LOCK] Osiągnięto limit jednoczesnych altcoinów ({alt_count}/{CONFIG.get('MAX_SIMULTANEOUS_ALTS', 1)}). Blokada {inst['label']}.")
                                continue

                        clean_target = pos_key.replace(redis_trade.prefix, "")
                        if any(clean_target in k for k in active_keys):
                            continue

                        # STRAŻNIK SPREADU
                        spread_ok, _ = await inst["client"].check_spread_ok(inst["symbol"])
                        if not spread_ok:
                            continue

                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_USDC"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, atr, 
                            CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["MEAN_REVERSION"]["RR_RATIO"],
                            inst["price_round"]
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_USDC"])
                        pos_value = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        calc_qty = floor_to_precision(pos_value / current_price, inst["round_digits"])
                        calc_qty = max(inst["min_qty"], calc_qty)
                        
                        # CRIT-03 FIX: ZAOKRĄGLENIE W GÓRĘ DLA MINIMALNEGO PROGU NOTIONAL
                        if (calc_qty * current_price) < CONFIG["MIN_ORDER_VALUE_USDC"]:
                            min_needed_qty = ceil_to_precision(CONFIG["MIN_ORDER_VALUE_USDC"] / current_price, inst["round_digits"])
                            calc_qty = max(calc_qty, min_needed_qty)
                            calc_qty = max(inst["min_qty"], calc_qty)

                        if (calc_qty * current_price) > available_cash:
                            continue

                        logger.info(f"🚨 [MEAN-REV-TRIGGER] Kupno {inst['label']} | Ilość: {calc_qty} | SL: {price_sl} (-{round(sl_pct*100, 2)}%)")
                        order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calc_qty)

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            base_ccy = inst["symbol"].split("-")[0]
                            real_bal = await inst["client"].wait_for_settled_balance(base_ccy, calc_qty)
                            raw_target = min(calc_qty, real_bal) if real_bal > 0 else calc_qty * 0.995
                            oco_qty = floor_to_precision(raw_target, inst["round_digits"])
                            oco_qty = max(inst["min_qty"], oco_qty)

                            oco_res = await inst["client"].execute_oco_protection(inst["symbol"], oco_qty, price_tp, price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "qty": oco_qty,
                                    "buy_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "sl_pct": sl_pct,
                                    "time": now_ts,
                                    "type": "MEAN_REVERSION",
                                    "be_applied": False
                                })
                                total_cost = round(oco_qty * current_price, 2)
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • MEAN REVERSION</b>\n"
                                    f"──────────────────────────────\n"
                                    f"💰 Kurs wejścia: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Wolumen: <b>{oco_qty}</b> (~{total_cost} {QUOTE_CCY})\n"
                                    f"──────────────────────────────\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"⏳ Strażnik Czasu: <b>18h</b> | OCO: <b>AKTYWNE</b>"
                                )
                            else:
                                # CRIT-01 FIX: LIKWIDACJA FAKTYCZNIE POSIADANEGO SALDA BEZ BŁĘDU 51008
                                logger.critical(f"🚨 [FAIL-SAFE] OCO odrzucone dla {inst['label']}! Pobieranie salda i natychmiastowa likwidacja do USDC...")
                                avail_bal = await inst["client"].wait_for_settled_balance(base_ccy, calc_qty * 0.90, max_attempts=3)
                                if avail_bal >= inst["min_qty"]:
                                    fail_safe_qty = floor_to_precision(avail_bal, inst["round_digits"])
                                    fail_safe_qty = max(inst["min_qty"], fail_safe_qty)
                                    await inst["client"].execute_market_order(inst["symbol"], "sell", fail_safe_qty)
                                await redis_trade.delete_key(pos_key)
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: POZYCJA ZLIKWIDOWANA]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja <b>{inst['label']}</b> (MEAN REVERSION) została natychmiast zamknięta zleceniem Market ze względu na błąd zlecenia obronnego OCO.\n"
                                    f"Salda rozliczone bezpiecznie do USDC."
                                )

        except Exception as e:
            logger.error(f"❌ [MEAN-REV-ERROR] Awaria w workerze: {e}")

        await asyncio.sleep(60)

# =========================================================================
# STRATEGIA 2: INDEPENDENT MOMENTUM WORKER (CRIT-01 & CRIT-03 FIXES)
# =========================================================================
async def independent_momentum_worker(session, redis_trade, tg, okx_client):
    logger.info("🚀 [MOMENTUM-WORKER] Uruchomiono wątek Momentum w tle.")
    instruments = [
        {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}_MOM", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
        {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}_MOM", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
        {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}_MOM", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
        {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}_MOM", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop(inst, "MOMENTUM", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                if await redis_trade.is_in_cooldown(inst["symbol"]):
                    continue

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                regime = MarketRegimeArbitrator.get_regime(candles_raw)
                if regime == "RANGING":
                    continue

                mom_metrics = MomentumQuantCore.calculate_momentum(
                    candles_raw, 
                    period=CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ROC_PERIOD"]
                )

                if mom_metrics and mom_metrics["signal"]:
                    # FILTR SMART MONEY
                    if CONFIG.get("SMART_MONEY", {}).get("ENABLED", True):
                        base_coin = inst["symbol"].split("-")[0]
                        max_ratio = CONFIG.get("SMART_MONEY", {}).get("MAX_LONG_SHORT_RATIO", 2.2)
                        sentiment_ratio = await inst["client"].get_smart_money_sentiment(base_coin)
                        if sentiment_ratio is not None and sentiment_ratio > max_ratio:
                            logger.warning(f"🛡️ [SMART-MONEY-BLOCK] Odrzucono wejście {inst['label']} (Momentum)! Skrajna euforia tłumu (L/S: {sentiment_ratio:.2f} > {max_ratio}).")
                            continue

                    async with GLOBAL_ALPHA_LOCK:
                        # TARCZA WYŁĄCZNIKA DZIENNEJ STRATY
                        wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        tot_eq = wallet_check.get("total_equity", 0.0)
                        if await redis_trade.is_daily_loss_exceeded(tot_eq):
                            continue

                        # TARCZA KORELACJI SEKTOROWEJ (MAX 1 ALTCOIN)
                        is_alt = not any(sym in inst["symbol"] for sym in ("BTC", "USDC"))
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        if is_alt:
                            alt_count = sum(1 for k in active_keys if not any(sym in k for sym in ("BTC", "USDC")))
                            if alt_count >= CONFIG.get("MAX_SIMULTANEOUS_ALTS", 1):
                                continue

                        clean_target = pos_key.replace(redis_trade.prefix, "")
                        if any(clean_target in k for k in active_keys):
                            continue

                        # STRAŻNIK SPREADU
                        spread_ok, _ = await inst["client"].check_spread_ok(inst["symbol"])
                        if not spread_ok:
                            continue

                        current_price = mom_metrics["current"]
                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_USDC"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, mom_metrics["atr"],
                            CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["MOMENTUM"]["RR_RATIO"],
                            inst["price_round"]
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_USDC"])
                        pos_value = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        calc_qty = floor_to_precision(pos_value / current_price, inst["round_digits"])
                        calc_qty = max(inst["min_qty"], calc_qty)
                        
                        # CRIT-03 FIX
                        if (calc_qty * current_price) < CONFIG["MIN_ORDER_VALUE_USDC"]:
                            min_needed_qty = ceil_to_precision(CONFIG["MIN_ORDER_VALUE_USDC"] / current_price, inst["round_digits"])
                            calc_qty = max(calc_qty, min_needed_qty)
                            calc_qty = max(inst["min_qty"], calc_qty)

                        if (calc_qty * current_price) > available_cash:
                            continue

                        logger.info(f"🚨 [MOMENTUM-TRIGGER] Kupno {inst['label']} | Ilość: {calc_qty} | SL: {price_sl} (-{round(sl_pct*100, 2)}%)")
                        order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calc_qty)

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            base_ccy = inst["symbol"].split("-")[0]
                            real_bal = await inst["client"].wait_for_settled_balance(base_ccy, calc_qty)
                            raw_target = min(calc_qty, real_bal) if real_bal > 0 else calc_qty * 0.995
                            oco_qty = floor_to_precision(raw_target, inst["round_digits"])
                            oco_qty = max(inst["min_qty"], oco_qty)

                            oco_res = await inst["client"].execute_oco_protection(inst["symbol"], oco_qty, price_tp, price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "qty": oco_qty,
                                    "buy_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "sl_pct": sl_pct,
                                    "time": now_ts,
                                    "type": "MOMENTUM",
                                    "be_applied": False
                                })
                                total_cost = round(oco_qty * current_price, 2)
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • MOMENTUM</b>\n"
                                    f"──────────────────────────────\n"
                                    f"💰 Kurs wejścia: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Wolumen: <b>{oco_qty}</b> (~{total_cost} {QUOTE_CCY})\n"
                                    f"──────────────────────────────\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"⏳ Strażnik Czasu: <b>8h</b> | OCO: <b>AKTYWNE</b>"
                                )
                            else:
                                # CRIT-01 FIX
                                logger.critical(f"🚨 [FAIL-SAFE] OCO odrzucone dla {inst['label']}! Pobieranie salda i likwidacja...")
                                avail_bal = await inst["client"].wait_for_settled_balance(base_ccy, calc_qty * 0.90, max_attempts=3)
                                if avail_bal >= inst["min_qty"]:
                                    fail_safe_qty = floor_to_precision(avail_bal, inst["round_digits"])
                                    fail_safe_qty = max(inst["min_qty"], fail_safe_qty)
                                    await inst["client"].execute_market_order(inst["symbol"], "sell", fail_safe_qty)
                                await redis_trade.delete_key(pos_key)
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: POZYCJA ZLIKWIDOWANA]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja <b>{inst['label']}</b> (MOMENTUM) została natychmiast zamknięta zleceniem Market ze względu na błąd zlecenia obronnego OCO."
                                )

        except Exception as e:
            logger.error(f"❌ [MOMENTUM-ERROR] Błąd w workerze: {e}")

        await asyncio.sleep(180)

# =========================================================================
# STRATEGIA 3: INDEPENDENT BREAKOUT WORKER (CRIT-01 & CRIT-03 FIXES)
# =========================================================================
async def independent_breakout_worker(session, redis_trade, tg, okx_client):
    logger.info("💥 [BREAKOUT-WORKER] Uruchomiono wątek Breakout w tle.")
    instruments = [
        {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}_BRK", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
        {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}_BRK", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
        {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}_BRK", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
        {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}_BRK", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            for inst in instruments:
                await reconcile_and_timestop(inst, "BREAKOUT", redis_trade, tg)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                if await redis_trade.is_in_cooldown(inst["symbol"]):
                    continue

                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                pos_check = await redis_trade.get_position_state(pos_key)
                if pos_check and pos_check.get("status") in ["OPEN", "WAITING_OCO"]:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                regime = MarketRegimeArbitrator.get_regime(candles_raw)
                brk_metrics = BreakoutQuantCore.calculate_breakout(
                    candles_raw, 
                    period=CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["BB_PERIOD"]
                )

                if brk_metrics and brk_metrics["signal"]:
                    # FILTR SMART MONEY
                    if CONFIG.get("SMART_MONEY", {}).get("ENABLED", True):
                        base_coin = inst["symbol"].split("-")[0]
                        max_ratio = CONFIG.get("SMART_MONEY", {}).get("MAX_LONG_SHORT_RATIO", 2.2)
                        sentiment_ratio = await inst["client"].get_smart_money_sentiment(base_coin)
                        if sentiment_ratio is not None and sentiment_ratio > max_ratio:
                            logger.warning(f"🛡️ [SMART-MONEY-BLOCK] Odrzucono wejście {inst['label']} (Breakout)! Skrajna euforia tłumu (L/S: {sentiment_ratio:.2f} > {max_ratio}).")
                            continue

                    async with GLOBAL_ALPHA_LOCK:
                        # TARCZA WYŁĄCZNIKA DZIENNEJ STRATY
                        wallet_check = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        tot_eq = wallet_check.get("total_equity", 0.0)
                        if await redis_trade.is_daily_loss_exceeded(tot_eq):
                            continue

                        # TARCZA KORELACJI SEKTOROWEJ (MAX 1 ALTCOIN)
                        is_alt = not any(sym in inst["symbol"] for sym in ("BTC", "USDC"))
                        url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                        async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as r_k:
                            active_keys = (await r_k.json()).get("result", []) if r_k.status == 200 else []

                        if len(active_keys) >= CONFIG["ALPHA_MAX_ACTIVE_SLOTS"]:
                            continue

                        if is_alt:
                            alt_count = sum(1 for k in active_keys if not any(sym in k for sym in ("BTC", "USDC")))
                            if alt_count >= CONFIG.get("MAX_SIMULTANEOUS_ALTS", 1):
                                continue

                        clean_target = pos_key.replace(redis_trade.prefix, "")
                        if any(clean_target in k for k in active_keys):
                            continue

                        # STRAŻNIK SPREADU
                        spread_ok, _ = await inst["client"].check_spread_ok(inst["symbol"])
                        if not spread_ok:
                            continue

                        current_price = brk_metrics["current"]
                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)

                        if available_cash < CONFIG["MIN_ORDER_VALUE_USDC"]:
                            continue

                        price_sl, price_tp, sl_pct = calculate_clamped_sl_tp(
                            current_price, brk_metrics["atr"],
                            CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["ATR_SL_MULT"],
                            CONFIG["STRATEGY_PARAMS"]["BREAKOUT"]["RR_RATIO"],
                            inst["price_round"]
                        )

                        risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                        safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_USDC"])
                        pos_value = min(risk_capital / sl_pct, total_balance * CONFIG["MAX_POSITION_PORTFOLIO_RATIO"], safe_cash * 0.95)

                        calc_qty = floor_to_precision(pos_value / current_price, inst["round_digits"])
                        calc_qty = max(inst["min_qty"], calc_qty)
                        
                        # CRIT-03 FIX
                        if (calc_qty * current_price) < CONFIG["MIN_ORDER_VALUE_USDC"]:
                            min_needed_qty = ceil_to_precision(CONFIG["MIN_ORDER_VALUE_USDC"] / current_price, inst["round_digits"])
                            calc_qty = max(calc_qty, min_needed_qty)
                            calc_qty = max(inst["min_qty"], calc_qty)

                        if (calc_qty * current_price) > available_cash:
                            continue

                        logger.info(f"🚨 [BREAKOUT-TRIGGER] Kupno {inst['label']} | Ilość: {calc_qty} | SL: {price_sl} (-{round(sl_pct*100, 2)}%)")
                        order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calc_qty)

                        if order_res and order_res.get("code") == "0":
                            now_ts = time.time()
                            base_ccy = inst["symbol"].split("-")[0]
                            real_bal = await inst["client"].wait_for_settled_balance(base_ccy, calc_qty)
                            raw_target = min(calc_qty, real_bal) if real_bal > 0 else calc_qty * 0.995
                            oco_qty = floor_to_precision(raw_target, inst["round_digits"])
                            oco_qty = max(inst["min_qty"], oco_qty)

                            oco_res = await inst["client"].execute_oco_protection(inst["symbol"], oco_qty, price_tp, price_sl)
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                algo_id = oco_res["data"][0].get("algoId", "")
                                await redis_trade.set_position_state(pos_key, {
                                    "status": "WAITING_OCO",
                                    "algo_id": algo_id,
                                    "qty": oco_qty,
                                    "buy_price": current_price,
                                    "tp_price": price_tp,
                                    "sl_price": price_sl,
                                    "sl_pct": sl_pct,
                                    "time": now_ts,
                                    "type": "BREAKOUT",
                                    "be_applied": False
                                })
                                total_cost = round(oco_qty * current_price, 2)
                                await tg.push(
                                    f"🟢 <b>[WEJŚCIE: {inst['label']}] • BREAKOUT</b>\n"
                                    f"──────────────────────────────\n"
                                    f"💰 Kurs wejścia: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Wolumen: <b>{oco_qty}</b> (~{total_cost} {QUOTE_CCY})\n"
                                    f"──────────────────────────────\n"
                                    f"🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-{round(sl_pct*100, 2)}%)\n"
                                    f"⏳ Strażnik Czasu: <b>8h</b> | OCO: <b>AKTYWNE</b>"
                                )
                            else:
                                # CRIT-01 FIX
                                logger.critical(f"🚨 [FAIL-SAFE] OCO odrzucone dla {inst['label']}! Pobieranie salda i likwidacja...")
                                avail_bal = await inst["client"].wait_for_settled_balance(base_ccy, calc_qty * 0.90, max_attempts=3)
                                if avail_bal >= inst["min_qty"]:
                                    fail_safe_qty = floor_to_precision(avail_bal, inst["round_digits"])
                                    fail_safe_qty = max(inst["min_qty"], fail_safe_qty)
                                    await inst["client"].execute_market_order(inst["symbol"], "sell", fail_safe_qty)
                                await redis_trade.delete_key(pos_key)
                                await tg.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: POZYCJA ZLIKWIDOWANA]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"Pozycja <b>{inst['label']}</b> (BREAKOUT) została natychmiast zamknięta zleceniem Market ze względu na błąd zlecenia obronnego OCO."
                                )

        except Exception as e:
            logger.error(f"❌ [BREAKOUT-ERROR] Błąd w workerze: {e}")

        await asyncio.sleep(180)

# =========================================================================
# STRATEGIA 4: INDEPENDENT GRID WORKER (DEDYKOWANY KOSZYK GRID MULTI-ASSET)
# =========================================================================
async def independent_grid_worker(session, redis_trade, tg, okx_client):
    logger.info("🧱 [GRID-WORKER] Uruchomiono wątek Grid Trading (Dynamic ATR Scaled) w tle.")
    instruments = [
        {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}_GRID", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
        {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}_GRID", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
        {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}_GRID", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
        {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}_GRID", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:GRID:*"
            grid_active_count = 0
            async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as resp_k:
                if resp_k.status == 200:
                    data_k = await resp_k.json()
                    grid_active_count = len(data_k.get("result", []))

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                pos_key = f"POS_ACTIVE:GRID:{inst['label']}"
                active_pos = await redis_trade.get_position_state(pos_key)

                if active_pos and active_pos.get("status") == "PENDING_BUY":
                    ord_id = active_pos.get("order_id")
                    state, fill_px = await inst["client"].get_order_state(inst["symbol"], ord_id)

                    if state != "filled":
                        curr_p = GLOBAL_WS_FEED.get_last_price(inst["symbol"]) if GLOBAL_WS_FEED else None
                        buy_p = float(active_pos.get("buy_price", 0.0))
                        order_time = float(active_pos.get("time", time.time()))
                        elapsed_time = time.time() - order_time

                        if (curr_p and buy_p > 0 and curr_p > buy_p * 1.020) or elapsed_time > 10800:
                            logger.info(f"🧹 [GRID-TIMEOUT] Anulowano przeterminowane zlecenie {ord_id} dla {inst['label']}.")
                            await inst["client"].cancel_order(inst["symbol"], ord_id)
                            await redis_trade.delete_key(pos_key)
                            continue

                    if state == "filled":
                        raw_qty = float(active_pos["qty"])
                        qty_to_sell = floor_to_precision(raw_qty * 0.998, inst["round_digits"])
                        qty_to_sell = max(inst["min_qty"], qty_to_sell)

                        actual_buy_p = fill_px if fill_px and fill_px > 0 else float(active_pos["buy_price"])
                        tp_price = float(active_pos["tp_price"])
                        sl_price = float(active_pos.get("sl_price", 0.0))

                        sell_res = await inst["client"].execute_limit_order(inst["symbol"], "sell", qty_to_sell, tp_price)
                        if sell_res and sell_res.get("code") == "0":
                            sell_ord_id = sell_res["data"][0]["ordId"]
                            await redis_trade.set_position_state(pos_key, {
                                "status": "WAITING_TP",
                                "sell_ord_id": sell_ord_id,
                                "buy_price": actual_buy_p,
                                "tp_price": tp_price,
                                "sl_price": sl_price,
                                "qty": qty_to_sell,
                                "time": time.time()
                            })
                            await tg.push(
                                f"🧱 <b>[GRID ENGINE: TAKE PROFIT DEPLOYED]</b>\n"
                                f"──────────────────────────────\n"
                                f"📈 Instrument: <b>{inst['label']}</b>\n"
                                f"💰 Kupiono po: <code>{actual_buy_p} {QUOTE_CCY}</code>\n"
                                f"📤 Wystawiono TP: <b>{tp_price} {QUOTE_CCY} (+0.5%)</b>\n"
                                f"🛑 Stop Loss: <code>{sl_price} {QUOTE_CCY} (-1.5%)</code>\n"
                                f"📦 Ilość: <b>{qty_to_sell}</b>"
                            )
                        continue

                    elif state in ["canceled", "cancelled"]:
                        await redis_trade.delete_key(pos_key)
                        continue

                elif active_pos and active_pos.get("status") == "WAITING_TP":
                    sell_ord_id = active_pos.get("sell_ord_id")
                    sell_state, sell_fill_px = await inst["client"].get_order_state(inst["symbol"], sell_ord_id)

                    if sell_state == "filled":
                        await redis_trade.delete_key(pos_key)
                        buy_p = float(active_pos.get("buy_price", 0.0))
                        qty_p = float(active_pos.get("qty", 0.0))
                        exit_p = sell_fill_px if sell_fill_px and sell_fill_px > 0 else float(active_pos.get("tp_price", buy_p))
                        pnl_gross = (exit_p - buy_p) * qty_p
                        fees = (buy_p * qty_p * 0.0008) + (exit_p * qty_p * 0.0008)
                        pnl_net = round(pnl_gross - fees, 2)
                        roe_net = round((pnl_net / (buy_p * qty_p)) * 100.0, 2) if buy_p > 0 else 0.0

                        await tg.push(
                            f"🧱 <b>[GRID PROFIT: {inst['label']}] • POZIOM L1</b>\n"
                            f"──────────────────────────────\n"
                            f"💰 Sprzedano po: <b>{exit_p} {QUOTE_CCY}</b> (Kupiono: {buy_p} {QUOTE_CCY})\n"
                            f"📦 Wolumen: <b>{qty_p}</b>\n"
                            f"──────────────────────────────\n"
                            f"💵 <b>Zysk siatki netto: +{pnl_net} {QUOTE_CCY} (+{roe_net}%)</b>\n"
                            f"🛡️ Prowizja giełdowa: uwzględniona\n"
                            f"Siatka resetuje poziom i poluje dalej."
                        )
                        continue

                    current_market_price = GLOBAL_WS_FEED.get_last_price(inst["symbol"]) if GLOBAL_WS_FEED else None
                    if not current_market_price:
                        candles_check = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                        if candles_check:
                            current_market_price = float(candles_check[-1][4])

                    sl_trigger_price = float(active_pos.get("sl_price", 0.0))
                    if current_market_price and sl_trigger_price > 0 and current_market_price <= sl_trigger_price:
                        logger.warning(f"🚨 [GRID-SL] Kurs {current_market_price} osiągnął próg SL ({sl_trigger_price}) dla {inst['label']}!")
                        await inst["client"].cancel_order(inst["symbol"], sell_ord_id)
                        await inst["client"].execute_market_order(inst["symbol"], "sell", float(active_pos["qty"]))
                        await redis_trade.delete_key(pos_key)

                        buy_p = float(active_pos.get("buy_price", 0.0))
                        qty_p = float(active_pos.get("qty", 0.0))
                        exit_p = current_market_price
                        pnl_gross = (exit_p - buy_p) * qty_p
                        fees = (buy_p * qty_p * 0.001) + (exit_p * qty_p * 0.001)
                        pnl_net = round(pnl_gross - fees, 2)
                        roe_net = round((pnl_net / (buy_p * qty_p)) * 100.0, 2) if buy_p > 0 else 0.0

                        await redis_trade.record_daily_loss(abs(pnl_net))
                        await tg.push(
                            f"🛑 <b>[GRID STOP-LOSS TRIGGERED]</b>\n"
                            f"──────────────────────────────\n"
                            f"Instrument: <b>{inst['label']}</b>\n"
                            f"💰 Wyjście awaryjne: <b>{exit_p} {QUOTE_CCY}</b> (Wejście: {buy_p} {QUOTE_CCY})\n"
                            f"📉 <b>Strata netto: {pnl_net} {QUOTE_CCY} ({roe_net}%)</b>"
                        )
                        continue

                if grid_active_count >= CONFIG["GRID_MAX_ACTIVE_LEVELS"]:
                    continue

                if await inst["client"].has_open_orders(inst["symbol"]):
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                regime = MarketRegimeArbitrator.get_regime(candles_raw)
                if regime == "TRENDING":
                    continue

                grid_metrics = GridQuantCore.calculate_grid_levels(
                    candles_raw, 
                    symbol=inst["symbol"],
                    grid_step_pct=CONFIG["STRATEGY_PARAMS"]["GRID"]["GRID_STEP_PCT"], 
                    levels=CONFIG["STRATEGY_PARAMS"]["GRID"]["LEVELS"]
                )

                if grid_metrics and grid_metrics["is_consolidation"]:
                    first_lvl = grid_metrics["levels"][0]
                    price_buy = round(first_lvl["buy_price"], inst["price_round"])
                    price_tp = round(first_lvl["tp_price"], inst["price_round"])
                    price_sl = round(price_buy * (1.0 - CONFIG["STRATEGY_PARAMS"]["GRID"]["SL_PCT"]), inst["price_round"])

                    wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                    total_balance = wallet.get("total_equity", 0.0)
                    available_cash = wallet.get("available_cash", 0.0)

                    if available_cash < CONFIG["MIN_ORDER_VALUE_USDC"]:
                        continue

                    risk_capital = total_balance * CONFIG["RISK_PER_TRADE_PCT"]
                    sl_pct = CONFIG["STRATEGY_PARAMS"]["GRID"]["SL_PCT"]
                    safe_cash = max(0.0, available_cash - CONFIG["RESERVE_CASH_BUFFER_USDC"])
                    pos_val = min(risk_capital / sl_pct, total_balance * 0.11, safe_cash * 0.95)

                    calc_qty = floor_to_precision(pos_val / price_buy, inst["round_digits"])
                    calc_qty = max(inst["min_qty"], calc_qty)
                    
                    # CRIT-03 FIX
                    if (calc_qty * price_buy) < CONFIG["MIN_ORDER_VALUE_USDC"]:
                        min_needed_qty = ceil_to_precision(CONFIG["MIN_ORDER_VALUE_USDC"] / price_buy, inst["round_digits"])
                        calc_qty = max(calc_qty, min_needed_qty)
                        calc_qty = max(inst["min_qty"], calc_qty)

                    if (calc_qty * price_buy) > available_cash:
                        continue

                    order_res = await inst["client"].execute_limit_order(inst["symbol"], "buy", calc_qty, price_buy)
                    if order_res and order_res.get("code") == "0":
                        ord_id = order_res["data"][0]["ordId"]
                        await redis_trade.set_position_state(pos_key, {
                            "status": "PENDING_BUY",
                            "order_id": ord_id,
                            "buy_price": price_buy,
                            "tp_price": price_tp,
                            "sl_price": price_sl,
                            "qty": calc_qty,
                            "time": time.time()
                        })
                        grid_active_count += 1
                        await tg.push(
                            f"🧱 <b>[GRID ENGINE: LIMIT ORDER PLACED]</b>\n"
                            f"──────────────────────────────\n"
                            f"📈 Instrument: <b>{inst['label']}</b>\n"
                            f"📥 Kupno (Limit L1): <b>{price_buy} {QUOTE_CCY}</b>\n"
                            f"📦 Wielkość: <b>{calc_qty}</b>\n"
                            f"🎯 Planowany TP: <code>{price_tp} {QUOTE_CCY}</code> (+0.5%)\n"
                            f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-1.5%)"
                        )
        except Exception as e:
            logger.error(f"❌ [GRID-ERROR] Błąd w workerze: {e}")

        await asyncio.sleep(180)

# =========================================================================
# ASYNCHRONICZNA PĘTLA GŁÓWNA (MULTI-TASKING CRON & WEBSOCKET)
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT, RATE_LIMITER, GLOBAL_WS_FEED, GLOBAL_ALPHA_LOCK, GLOBAL_REDIS_BRIDGE
    logger.info("⚡ [ENGINE ONLINE] Uruchamianie workerów v11.4...")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    GLOBAL_ALPHA_LOCK = asyncio.Lock()
    if RATE_LIMITER is None:
        RATE_LIMITER = TokenBucketRateLimiter()

    async with aiohttp.ClientSession() as session:
        # SINGLETON L1 RAM-FIRST CACHE BRIDGE
        GLOBAL_REDIS_BRIDGE = UpstashRedisTradingBridge(
            os.environ.get("UPSTASH_REDIS_REST_URL", ""),
            os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
            session
        )
        redis_trade = GLOBAL_REDIS_BRIDGE

        tg = TelegramThrottledDispatcher(
            os.environ.get("TELEGRAM_BOT_TOKEN", ""),
            os.environ.get("TELEGRAM_CHANNEL_ID", ""),
            session
        )
        okx_client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=IS_SANDBOX)
        ws_feed = OKXWebSocketPriceFeed(session, is_sandbox=IS_SANDBOX)
        GLOBAL_WS_FEED = ws_feed

        symbols_to_stream = [f"BTC-{QUOTE_CCY}", f"ETH-{QUOTE_CCY}", f"SOL-{QUOTE_CCY}", f"XRP-{QUOTE_CCY}"]

        tasks = []
        try:
            tasks.append(asyncio.create_task(ws_feed.start_listener(symbols_to_stream)))
            tasks.append(asyncio.create_task(independent_mean_reversion_worker(session, redis_trade, tg, okx_client)))
            tasks.append(asyncio.create_task(independent_momentum_worker(session, redis_trade, tg, okx_client)))
            tasks.append(asyncio.create_task(independent_breakout_worker(session, redis_trade, tg, okx_client)))
            tasks.append(asyncio.create_task(independent_grid_worker(session, redis_trade, tg, okx_client)))

            heartbeat_timer = 0
            while not ASYNC_SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)
                heartbeat_timer += 1
                if heartbeat_timer >= 60:
                    heartbeat_timer = 0
                    prices_count = len(ws_feed.latest_prices)
                    logger.info(f"💓 [ENGINE-HEARTBEAT] Wszystkie 4 workery aktywne | WebSocket Feed: {prices_count}/4 par | Pętla OK")

        except Exception as e:
            logger.error(f"❌ [CRON-FATAL] Awaria pętli: {e}")
        finally:
            logger.info("🛑 [SHUTDOWN] Wygaszanie workerów asynchronicznych...")
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

def background_scheduler_thread():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    BACKGROUND_LOOP = loop
    try:
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e:
        logger.error(f"[THREAD FAILURE] Awaria: {e}")
    finally:
        loop.close()

# =========================================================================
# ENDPOINTY STERUJĄCE FLASK (WSPÓŁDZIELONY SINGLETON CRIT-02 FIX)
# =========================================================================
@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    return jsonify({"status": "success", "message": "Wersja v11.4 realizuje strategie w pełni autonomicznie w tle."}), 200

@app.route('/emergency-liquidate', methods=['GET', 'POST'])
def emergency_liquidate_to_cash():
    """Awaryjne odwołanie zleceń, rynkowy zrzut SPOT do USDC i wyczyszczenie kluczy Redis oraz lokalnego bufora RAM."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest aktywna."}), 500

    async def _execute_flush():
        async with aiohttp.ClientSession() as session:
            client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=IS_SANDBOX)
            redis_trade = GLOBAL_REDIS_BRIDGE or UpstashRedisTradingBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                session
            )
            # CRIT-02 FIX: CZYSZCZENIE WSPÓŁDZIELONEGO BUFORA L1
            redis_trade.clear_local_caches()

            report = {"cancelled_orders": [], "liquidated": [], "redis_cleaned": False}
            symbols_to_flush = [
                ("BTC", f"BTC-{QUOTE_CCY}", 5),
                ("ETH", f"ETH-{QUOTE_CCY}", 4),
                ("SOL", f"SOL-{QUOTE_CCY}", 2),
                ("XRP", f"XRP-{QUOTE_CCY}", 2)
            ]

            # 1. Anulowanie zwykłych zleceń oczekujących
            for ccy, symbol, _ in symbols_to_flush:
                try:
                    await client.rate_limiter.consume()
                    req_p = f"/api/v5/trade/orders-pending?instId={symbol}"
                    async with session.get(f"{client.base_url}{req_p}", headers=client._get_headers("GET", req_p), timeout=4) as r_pend:
                        d_pend = await r_pend.json()
                        if d_pend.get("code") == "0":
                            for ord_item in d_pend.get("data", []):
                                o_id = ord_item.get("ordId")
                                await client.cancel_order(symbol, o_id)
                                report["cancelled_orders"].append({"symbol": symbol, "ordId": o_id})
                except Exception as ex:
                    logger.error(f"⚠️ [EMERGENCY] Błąd anulowania {symbol}: {ex}")

            # 2. Anulowanie algorytmicznych OCO
            url_pos = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:*"
            all_pos_keys = []
            async with session.get(url_pos, headers=redis_trade.headers) as r_pos:
                if r_pos.status == 200:
                    all_pos_keys = (await r_pos.json()).get("result", [])
                    for p_key in all_pos_keys:
                        raw_data = await redis_trade.get_position_state(p_key.replace(redis_trade.prefix, ""))
                        if raw_data and "algo_id" in raw_data:
                            inst_symbol = f"{p_key.split(':')[-1].split('_')[0]}-{QUOTE_CCY}"
                            await client.cancel_algo_order(inst_symbol, raw_data["algo_id"])

            await asyncio.sleep(0.5)

            # 3. Sprzedaż rynkowa wszystkich monet do USDC
            for ccy, symbol, round_d in symbols_to_flush:
                bal = await client.get_account_balance(ccy)
                if bal > 0.0001:
                    qty = floor_to_precision(bal * 0.999, round_d)
                    res = await client.execute_market_order(symbol, "sell", qty)
                    report["liquidated"].append({"symbol": symbol, "qty": qty, "res": res})

            # 4. Czyszczenie bazy Redis
            if all_pos_keys:
                del_payload = [["DEL"] + all_pos_keys]
                await session.post(f"{redis_trade.url}/pipeline", json=del_payload, headers=redis_trade.headers)
                report["redis_cleaned"] = True

            return report

    fut = asyncio.run_coroutine_threadsafe(_execute_flush(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=25)
        return jsonify({"status": "success", "report": res}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/export-analytics', methods=['GET'])
def export_analytics_safe_json():
    try:
        r_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip('/')
        r_tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        if not r_url or not r_tok:
            return jsonify({"status": "success", "trading_data": []}), 200

        headers = {"Authorization": f"Bearer {r_tok}", "Content-Type": "application/json"}
        req_keys = Request(f"{r_url}/keys/TRADE_ANALYTICS:*", headers=headers)
        with urlopen(req_keys, timeout=10) as resp:
            raw_keys_resp = json.loads(resp.read().decode('utf-8'))
            r_keys = raw_keys_resp.get("result", []) if isinstance(raw_keys_resp, dict) else []

        if not r_keys:
            return jsonify({"status": "success", "trading_data": []}), 200

        pipeline_payload = [["MGET"] + r_keys, ["DEL"] + r_keys]
        req_pipe = Request(f"{r_url}/pipeline", data=json.dumps(pipeline_payload).encode('utf-8'), headers=headers, method="POST")
        with urlopen(req_pipe, timeout=15) as resp:
            raw_pipe_resp = json.loads(resp.read().decode('utf-8'))

        r_values = raw_pipe_resp[0].get("result", []) if raw_pipe_resp and isinstance(raw_pipe_resp[0], dict) else []
        trading_output = []
        for index, key in enumerate(r_keys):
            if index >= len(r_values):
                break
            parts = key.split(":")
            val = r_values[index]
            try:
                val_int = int(val) if val is not None else 0
            except (ValueError, TypeError):
                val_int = 0
            trading_output.append({
                "data": parts[2] if len(parts) > 2 else "??",
                "metryka": parts[1],
                "wartosc": val_int
            })
        return jsonify({"status": "success", "trading_count": len(trading_output), "trading_data": trading_output}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# =========================================================================
# GŁÓWNY PUNKT WEJŚCIA I OBSŁUGA SYGNAŁÓW POSIX
# =========================================================================
if __name__ == "__main__":
    worker_thread = threading.Thread(target=background_scheduler_thread, daemon=True)
    worker_thread.start()

    def main_thread_shutdown_handler(signum, frame):
        logger.warning(f"🛑 [SIGTERM/SIGINT] Przechwycono sygnał {signum}. Zamykanie silnika...")
        if BACKGROUND_LOOP and ASYNC_SHUTDOWN_EVENT:
            BACKGROUND_LOOP.call_soon_threadsafe(ASYNC_SHUTDOWN_EVENT.set)
        time.sleep(1.5)
        sys.exit(0)

    signal.signal(signal.SIGTERM, main_thread_shutdown_handler)
    signal.signal(signal.SIGINT, main_thread_shutdown_handler)

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
