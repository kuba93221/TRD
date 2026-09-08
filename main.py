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
import gc
import hmac
import hashlib
import base64
from datetime import datetime, UTC
from flask import Flask, jsonify
from typing import Dict, Any, List, Optional
from urllib.parse import urlencode
from urllib.request import Request, urlopen

# =========================================================================
# SYSTEMOWY MODUŁ OBSERVABILITY & GLOBAL CONTEXT
# =========================================================================
LOG_LEVEL_CONFIG = os.environ.get("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL_CONFIG, logging.INFO),
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Algorithmic_Trading_Engine_v10.0_OKX_PRODUCTION")

logger.info("⚙️ [SYSTEM-INIT] Uruchamianie PEŁNEGO silnika v10.0 [OKX SPOT SANDBOX/PRODUCTION]")

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
PIPELINE_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
RATE_LIMITER: Optional[Any] = None

# =========================================================================
# SERWER MONITORINGU FLASK (URUCHAMIANY PRODUKCYJNIE NA RENDERZE)
# =========================================================================
app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return "TRADING_ENGINE_DOWN", 503
    return "OK", 200

# =========================================================================
# WIZJER DIAGNOSTYCZNY AUTORYZACJI OKX (ENDPOINT KONFIGURACJI KONTA)
# =========================================================================
@app.route('/test-auth', methods=['GET'])
def web_test_okx_handshake():
    """Weryfikacja autoryzacji OKX na bazowym endpoincie /api/v5/account/config."""
    import urllib.error

    api_key = str(os.environ.get("OKX_API_KEY", "")).strip()
    secret_key = str(os.environ.get("OKX_SECRET_KEY", "")).strip()
    passphrase = str(os.environ.get("OKX_PASSPHRASE", "")).strip()
    base_url = "https://www.okx.com"
    request_path = "/api/v5/account/config"

    report = {
        "key_prefix": f"{api_key[:8]}...{api_key[-4:]}" if len(api_key) >= 12 else "INVALID",
        "api_key_len": len(api_key),
        "secret_key_len": len(secret_key),
        "passphrase_len": len(passphrase),
        "trials": []
    }

    if not all([api_key, secret_key, passphrase]):
        report["error"] = "Brak wymaganych zmiennych środowiskowych w panelu Render!"
        return jsonify(report), 400

    timestamp = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
    message = f"{timestamp}GET{request_path}"
    mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
    signature = base64.b64encode(mac.digest()).decode('utf-8')

    headers = {
        "Content-Type": "application/json",
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "OK-ACCESS-KEY": api_key,
        "OK-ACCESS-SIGN": signature,
        "OK-ACCESS-TIMESTAMP": timestamp,
        "OK-ACCESS-PASSPHRASE": passphrase,
        "x-simulated-trading": "1"
    }

    try:
        req = Request(f"{base_url}{request_path}", headers=headers, method="GET")
        with urlopen(req, timeout=6) as resp:
            resp_data = json.loads(resp.read().decode('utf-8'))
            report["trials"].append({
                "endpoint": request_path,
                "http_status": resp.status,
                "okx_code": resp_data.get("code"),
                "okx_msg": resp_data.get("msg"),
                "account_level": resp_data.get("data", [{}])[0].get("acctLv") if resp_data.get("data") else None
            })
    except urllib.error.HTTPError as he:
        err_body = he.read().decode('utf-8', errors='ignore')
        report["trials"].append({
            "endpoint": request_path,
            "http_status": he.code,
            "response": err_body[:250]
        })
    except Exception as e:
        report["trials"].append({
            "endpoint": request_path,
            "exception": str(e)
        })

    return jsonify(report), 200
# =========================================================================
# REGULATOR PRZEPŁYWU SIECIOWEGO (TOKEN BUCKET RATE LIMITER)
# =========================================================================
class TokenBucketRateLimiter:
    """Regulator tokenowy dostosowany do limitów zapytań OKX V5 REST API."""
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
# POMOST UPSTASH REDIS (STRUKTURA BINARNA MSGPACK/HEX + AUTO-TTL)
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
        self._pipeline_cache: Dict[str, List[Dict[str, Any]]] = {}

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

    async def push_historical_tick(self, market_id: str, tick_data: Dict[str, Any], max_elements: int = 50) -> bool:
        if not self.url:
            return False
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            hex_str = msgpack.packb(tick_data, use_bin_type=True).hex()
            
            # Dodano komendę EXPIRE (7 dni = 604800s) dla ochrony limitu 256MB Upstash
            pipeline_payload = [
                ["LPUSH", safe_key, hex_str],
                ["LTRIM", safe_key, "0", str(max_elements - 1)],
                ["LRANGE", safe_key, "0", str(max_elements - 1)],
                ["EXPIRE", safe_key, "604800"]
            ]
            
            url = f"{self.url}/pipeline"
            async with self.session.post(url, json=pipeline_payload, headers=self.headers, timeout=5) as resp:
                if resp.status != 200:
                    return False
                
                results = await resp.json()
                if isinstance(results, list) and len(results) >= 3:
                    cmd_res = results[2]
                    hex_list = cmd_res.get("result", []) if isinstance(cmd_res, dict) else []
                    
                    parsed_ticks = []
                    for h in hex_list:
                        unpacked = self._safe_unpack_hex(h)
                        if unpacked:
                            parsed_ticks.append(unpacked)
                            
                    self._pipeline_cache[market_id] = parsed_ticks
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS PIPELINE ERROR] Błąd zapisu historii dla {market_id}: {e}")
            return False

    async def get_historical_ticks(self, market_id: str, max_elements: int = 50) -> List[Dict[str, Any]]:
        cached_data = self._pipeline_cache.pop(market_id, None)
        if cached_data is not None:
            return cached_data
            
        if not self.url:
            return []
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            url = f"{self.url}/lrange/{safe_key}/0/{max_elements - 1}"
            async with self.session.get(url, headers=self.headers, timeout=4) as response:
                if response.status != 200:
                    return []
                res_json = await response.json()
                hex_list = res_json.get("result", []) if isinstance(res_json, dict) else []
                
                parsed_ticks = []
                for h in hex_list:
                    unpacked = self._safe_unpack_hex(h)
                    if unpacked:
                        parsed_ticks.append(unpacked)
                return parsed_ticks
        except Exception as e:
            logger.error(f"❌ [REDIS FALLBACK ERROR] Błąd odczytu historii {market_id}: {e}")
            return []

    async def incr_metric(self, field_name: str):
        if not self.url:
            return
        current_date = datetime.now(UTC).strftime('%Y-%m-%d')
        key = self._enforce_prefix(f"ANALYTICS:{field_name}:{current_date}")
        try:
            async with self.session.get(f"{self.url}/incr/{key}", headers=self.headers, timeout=3) as resp:
                await resp.read()
        except Exception:
            pass

# =========================================================================
# MOSTEK POWIADOMIEŃ TELEGRAM
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
# RDZEŃ QUANT: Z-SCORE + FILTRY TRENDU MACRO 1H, RSI I ATR
# =========================================================================
class AlgorithmicQuantCore:
    """Aparat analityczny powrotu do średniej (Mean Reversion) z filtrami momentum."""

    @staticmethod
    def _calculate_ema(prices: List[float], period: int = 15) -> float:
        """Kalkulacja EMA w kolejności ściśle chronologicznej (od najstarszej do najnowszej)."""
        if len(prices) < period:
            return prices[-1] if prices else 0.0
        k = 2.0 / (period + 1.0)
        ema = sum(prices[:period]) / period  # Inicjalizacja SMA z pierwszego okna
        for p in prices[period:]:
            ema = (p * k) + (ema * (1.0 - k))
        return ema

    @staticmethod
    def _calculate_rsi(prices: List[float], period: int = 14) -> float:
        """Kalkulacja RSI w ujęciu chronologicznym."""
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
    def calculate_z_score(ticks: List[Dict[str, Any]], macro_prices: List[float]) -> Optional[Dict[str, Any]]:
        prices = [float(t.get("last", 0)) for t in ticks if t.get("last")]
        n = len(prices)
        if n < 20:
            return None

        sma = sum(prices) / n
        variance = sum((x - sma) ** 2 for x in prices) / n
        std_dev = math.sqrt(variance)
        if std_dev == 0:
            std_dev = 1e-6
            
        current_price = prices[0]  # Najświeższy zarejestrowany tick
        z_score = (current_price - sma) / std_dev

        # Filtr trendu na bazie świec 1H (lub cen tickowych w razie braku świec)
        use_prices = macro_prices if len(macro_prices) >= 15 else list(reversed(prices))
        ema_trend = AlgorithmicQuantCore._calculate_ema(use_prices, period=15)
        trend_direction = "LONG_ONLY" if current_price >= ema_trend else "SHORT_ONLY"

        use_rsi_prices = macro_prices if len(macro_prices) >= 15 else list(reversed(prices))
        rsi_val = AlgorithmicQuantCore._calculate_rsi(use_rsi_prices, period=14)

        bandwidth = (std_dev * 4.0) / sma if sma != 0 else 0.0
        atr_estimated = std_dev * 0.5

        return {
            "current": current_price,
            "sma": round(sma, 6),
            "z_score": round(z_score, 4),
            "trend": trend_direction,
            "rsi": round(rsi_val, 2),
            "bandwidth": round(bandwidth, 4),
            "atr": round(atr_estimated, 6)
        }

# =========================================================================
# SYSTEMOWY KLIENT GIEŁDY OKX SPOT (V5 REST API - SANDBOX & PRODUCTION)
# =========================================================================
class OKXSpotClient:
    """Kompletny asynchroniczny klient OKX V5 REST API dla rynku SPOT."""
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter, is_sandbox: bool = True):
        self.base_url = os.environ.get("OKX_API_URL", "https://www.okx.com").rstrip('/')
        self.session = session
        self.rate_limiter = rate_limiter
        self.is_sandbox = is_sandbox
        
        self.api_key = os.environ.get("OKX_API_KEY", "").strip()
        self.secret_key = os.environ.get("OKX_SECRET_KEY", "").strip()
        self.passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()

    def _generate_timestamp(self) -> str:
        """Format ISO 8601 UTC wymagany przez silnik autoryzacji OKX."""
        return datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'

    def _sign(self, timestamp: str, method: str, request_path: str, body: str = "") -> str:
        """Podpis kryptograficzny HMAC-SHA256 zakodowany w formacie Base64."""
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

    async def get_account_balance(self, ccy: str = "USDT") -> float:
        """Pobiera wolne saldo (availBal) z portfela handlowego SPOT z obsługą pauzy autoryzacji."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            logger.warning("[OKX-WARN] Brak pełnych danych uwierzytelniających. Zlecenia wstrzymane (saldo: 0.0).")
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
                    details = data["data"][0].get("details", [])
                    for bal in details:
                        if bal.get("ccy") == ccy:
                            return float(bal.get("availBal", 0.0))
                    # Fallback na ogólną wycenę portfela
                    return float(data["data"][0].get("totalEq", 0.0))

                # Obsługa braku aktywacji klucza w klastrze API
                if code == "50119":
                    logger.warning("⚠️ [OKX-AUTH-SUSPENDED] Klucz API nie jest aktywny w klastrze Demo. Zlecenia zablokowane (saldo: 0.0).")
                    return 0.0

                logger.error(f"[OKX-BALANCE-FAIL] Odpowiedź giełdy: code={code}, msg={data.get('msg')}")
                return 0.0
        except Exception as e:
            logger.error(f"[OKX-BALANCE-EXCEPTION] Błąd pobierania salda konta: {e}")
            return 0.0

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Pobiera kurs instrumentu SPOT (np. 'BTC-USDT')."""
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
                    return {
                        "source": "OKX_SPOT",
                        "symbol": symbol,
                        "last": float(ticker_info.get("last", 0.0))
                    }
                return None
        except Exception as e:
            logger.error(f"[OKX-TICKER-EXCEPTION] Błąd pobierania kursu {symbol}: {e}")
            return None

    async def get_macro_candles(self, symbol: str, bar: str = "1H", limit: int = 30) -> List[float]:
        """Pobiera świece historyczne z OKX (kolejność chronologiczna od najstarszej do najnowszej)."""
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
                    # data['data']: [ts, o, h, l, c, vol, ...], od najświeższej do najstarszej
                    candles = data["data"]
                    return [float(c[4]) for c in reversed(candles)]
                return []
        except Exception as e:
            logger.error(f"[OKX-CANDLES-EXCEPTION] Błąd pobierania świec makro {symbol}: {e}")
            return []

    async def execute_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict[str, Any]]:
        """Składa zlecenie rynkowe na rynku SPOT OKX."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()
        
        request_path = "/api/v5/trade/order"
        body_dict = {
            "instId": symbol,
            "tdMode": "cash",       # Czysty handel kasowy SPOT bez dźwigni
            "side": side.lower(),   # 'buy' lub 'sell'
            "ordType": "market",
            "sz": str(quantity)     # Wielkość zlecenia
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"[OKX-ORDER-ERROR] Błąd wysyłania zlecenia {side} dla {symbol}: {e}")
            return None

    async def execute_oco_protection(self, symbol: str, quantity: float, price_tp: float, price_sl: float) -> Optional[Dict[str, Any]]:
        """
        Wysyła zlecenie ochronne Algo OCO (Take Profit + Stop Loss) na OKX.
        Wykorzystuje dedykowany endpoint zasileń warunkowych /api/v5/trade/order-algo.
        """
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
            "tpOrdPx": "-1",               # -1 oznacza realizację po cenie rynkowej po wyzwoleniu TP
            "slTriggerPx": str(price_sl),
            "slOrdPx": "-1"                # -1 oznacza realizację po cenie rynkowej po wyzwoleniu SL
        }
        body_json = json.dumps(body_dict)
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("POST", request_path, body_json)

        try:
            async with self.session.post(url, data=body_json, headers=headers, timeout=5) as r:
                res = await r.json()
                logger.info(f"🛡️ [OKX-OCO-DEPLOYED] Zlecenie obronne OCO wysłane do OKX dla {symbol}: {res}")
                return res
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-CRITICAL-ERROR] Awaria składania zlecenia OCO dla {symbol}: {e}")
            return None

# =========================================================================
# CENTRALNY ASYNCHRONICZNY POTOK WYKONAWCZY (OKX SPOT ENGINE)
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None:
        PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked():
        logger.debug("[POTOK-WARN] Poprzednia analiza wciąż trwa. Pomijam cykl.")
        return

    async with PIPELINE_LOCK:
        logger.info("🕵️ [POTOK OKX] Skanowanie koszyka rynków SPOT i analiza wielokryteriowa...")
        if RATE_LIMITER is None:
            RATE_LIMITER = TokenBucketRateLimiter()

        async with aiohttp.ClientSession() as session:
            redis_trade = UpstashRedisTradingBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                session
            )
            tg = TelegramThrottledDispatcher(
                os.environ.get("TELEGRAM_BOT_TOKEN", ""),
                os.environ.get("TELEGRAM_CHANNEL_ID", ""),
                session
            )

            # Inicjalizacja klienta OKX (domyślnie Sandbox / Demo Trading)
            okx_client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=True)
            total_balance = await okx_client.get_account_balance("USDT")

            # Koszyk par w standardzie nazewnictwa OKX (BASE-QUOTE)
            instruments = [
                {"client": okx_client, "symbol": "BTC-USDT", "label": "BTC_USDT", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
                {"client": okx_client, "symbol": "ETH-USDT", "label": "ETH_USDT", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
                {"client": okx_client, "symbol": "SOL-USDT", "label": "SOL_USDT", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
                {"client": okx_client, "symbol": "BNB-USDT", "label": "BNB_USDT", "min_qty": 0.001, "round_digits": 3, "price_round": 1},
                {"client": okx_client, "symbol": "LINK-USDT", "label": "LINK_USDT", "min_qty": 0.01, "round_digits": 2, "price_round": 3},
                {"client": okx_client, "symbol": "XRP-USDT", "label": "XRP_USDT", "min_qty": 0.1, "round_digits": 1, "price_round": 4}
            ]

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                ticker = await inst["client"].get_market_ticker(inst["symbol"])

                if not ticker:
                    logger.warning(f"⚠️ [{inst['label']}] Brak odpowiedzi z giełdy dla kursu SPOT.")
                    continue

                current_price = ticker.get("last", 0.0)
                # Zapis do odizolowanej bazy z prefiksem TRADE_ i auto-TTL 7 dni
                await redis_trade.push_historical_tick(inst["label"], ticker, max_elements=50)
                history = await redis_trade.get_historical_ticks(inst["label"], max_elements=50)
                samples_count = len(history)

                logger.info(f"📥 [{inst['label']}] Kurs SPOT: {current_price} USDT | Bufor Redis: {samples_count}/20 próbek")

                if samples_count < 20:
                    logger.info(f"⏳ [{inst['label']}] Zbieranie historii próbek ({samples_count}/20)... Silnik wstrzymuje analizę.")
                    continue

                macro_candles = await inst["client"].get_macro_candles(inst["symbol"], bar="1H", limit=30)
                metrics = AlgorithmicQuantCore.calculate_z_score(history, macro_candles)

                if metrics:
                    z = metrics["z_score"]
                    rsi = metrics["rsi"]
                    bandwidth = metrics["bandwidth"]
                    trend = metrics["trend"]
                    atr = metrics["atr"]

                    logger.info(f"📊 [{inst['label']}] P: {current_price} | Z: {z} | RSI: {rsi} | Bw: {bandwidth} | T: {trend}")
                    await redis_trade.incr_metric(f"ticks_{inst['label']}")

                    if bandwidth < 0.001:
                        logger.info(f"⚠️ [{inst['label']}] Blokada: BandWidth skrajnie niski ({bandwidth}). Rynek w kompresji.")
                        continue

                        # Zarządzanie ryzykiem: 1% salda kapitału
                        risk_capital = total_balance * 0.01
                        stop_loss_distance = atr * 2.0

                        if stop_loss_distance > 0:
                            calculated_qty = risk_capital / stop_loss_distance
                            calculated_qty = max(inst["min_qty"], round(calculated_qty, inst["round_digits"]))
                        else:
                            calculated_qty = inst["min_qty"]

                        # Wymóg minimalnej wartości zlecenia OKX (> 11 USDT)
                        order_value_usdt = calculated_qty * current_price
                        if order_value_usdt < 11.0:
                            calculated_qty = max(calculated_qty, round(11.0 / current_price, inst["round_digits"]))
                            calculated_qty = max(inst["min_qty"], calculated_qty)

                        # Decyzja wejścia: Standardowe odbicie lub wyprzedaż krachowa
                        standard_buy = (z <= -1.5 and trend == "LONG_ONLY" and rsi <= 35)
                        crash_buy = (z <= -2.5 and rsi <= 20)

                        if standard_buy or crash_buy:
                            logger.info(f"🚨 [EXECUTION-TRIGGER] Kupno SPOT dla {inst['label']} (Standard: {standard_buy}, Crash: {crash_buy})")
                            order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calculated_qty)

                            if order_res and order_res.get("code") == "0":
                                actual_qty = calculated_qty
                                price_tp = round(current_price + (stop_loss_distance * 1.5), inst["price_round"])
                                price_sl = round(current_price - stop_loss_distance, inst["price_round"])

                                # Aktywacja obrony OCO po rozliczeniu zakupu
                                await asyncio.sleep(0.3)
                                await inst["client"].execute_oco_protection(inst["symbol"], actual_qty, price_tp, price_sl)

                                await tg.push(
                                    f"🟩 <b>[OKX TRADING ENGINE: OCO DEPLOYED]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"🤖 Tryb: <b>SPOT (Demo Sandbox)</b>\n"
                                    f"📈 Instrument: <b>{inst['label']}</b>\n"
                                    f"💰 Kurs wejścia: <b>{current_price} USDT</b>\n"
                                    f"📦 Wielkość: <b>{actual_qty}</b> (Ryzyko: 1% konta)\n"
                                    f"──────────────────────────────\n"
                                    f"🛡️ <b>OCHRONA OCO (ALGO):</b>\n"
                                    f"  • 🎯 Take Profit: <code>{price_tp} USDT</code>\n"
                                    f"  • 🛑 Stop Loss: <code>{price_sl} USDT</code>\n"
                                    f"──────────────────────────────"
                                )
            gc.collect()

# =========================================================================
# ASYNCHRONICZNY WĄTEK SPOCZYNKOWY
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT
    logger.info("⚡ [TRADING ONLINE] Silnik OKX gotowy do wyzwalania zewnętrznego przez endpoint.")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    while not ASYNC_SHUTDOWN_EVENT.is_set():
        await asyncio.sleep(1)
    logger.info("👋 [SHUTDOWN] Potok zamknięty bezpiecznie. Wszystkie stany skonsolidowane.")

def background_scheduler_thread():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    BACKGROUND_LOOP = loop
    try:
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e:
        logger.error(f"[CRITICAL THREAD FAILURE] Awaria wątku tła: {e}")
    finally:
        loop.close()

# =========================================================================
# PUBLICZNE ENDPOINTY STERUJĄCE (DLA CRON-JOB.ORG)
# =========================================================================
@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Potok tradingu nie jest gotowy."}), 500
    asyncio.run_coroutine_threadsafe(run_async_pipeline(), BACKGROUND_LOOP)
    return jsonify({"status": "success", "message": "Analiza rynków OKX uruchomiona pomyślnie."}), 200

@app.route('/export-analytics', methods=['GET'])
def export_analytics_safe_json():
    """Lekki eksport analityki bez zewnętrznej zależności requests."""
    try:
        r_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip('/')
        r_tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        if not r_url or not r_tok:
            return jsonify({"status": "success", "trading_data": []}), 200

        headers = {"Authorization": f"Bearer {r_tok}", "Content-Type": "application/json"}
        req_keys = Request(f"{r_url}/keys/TRADE_ANALYTICS:*", headers=headers)
        with urlopen(req_keys, timeout=10) as resp:
            keys_data = json.loads(resp.read().decode('utf-8'))
            r_keys = keys_data.get("result", [])

        if not r_keys:
            return jsonify({"status": "success", "trading_data": []}), 200

        pipeline_payload = [["MGET"] + r_keys, ["DEL"] + r_keys]
        req_pipe = Request(
            f"{r_url}/pipeline",
            data=json.dumps(pipeline_payload).encode('utf-8'),
            headers=headers,
            method="POST"
        )
        with urlopen(req_pipe, timeout=15) as resp:
            pipeline_result = json.loads(resp.read().decode('utf-8')).get("result", [])

        r_values = pipeline_result[0] if pipeline_result else []
        trading_output = []
        for index, key in enumerate(r_keys):
            if index >= len(r_values):
                break
            parts = key.split(":")
            trading_output.append({
                "data": parts[2] if len(parts) > 2 else "??",
                "metryka": parts[1],
                "wartosc": int(r_values[index] or 0)
            })
        return jsonify({"status": "success", "trading_count": len(trading_output), "trading_data": trading_output}), 200
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# =========================================================================
# INICJACJA I BEZPIECZNA OBSŁUGA SYGNAŁÓW W WĄTKU GŁÓWNYM
# =========================================================================
if __name__ == "__main__":
    import sys

    worker_thread = threading.Thread(target=background_scheduler_thread, daemon=True)
    worker_thread.start()

    def main_thread_shutdown_handler(signum, frame):
        logger.warning(f"🛑 [SIGTERM/SIGINT] Przechwycono sygnał {signum} w wątku głównym. Wyłączanie bota...")
        if BACKGROUND_LOOP and ASYNC_SHUTDOWN_EVENT:
            BACKGROUND_LOOP.call_soon_threadsafe(ASYNC_SHUTDOWN_EVENT.set)
        time.sleep(1.5)
        sys.exit(0)

    signal.signal(signal.SIGTERM, main_thread_shutdown_handler)
    signal.signal(signal.SIGINT, main_thread_shutdown_handler)

    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
