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

logger.info("⚙️ [SYSTEM-INIT] Uruchamianie PEŁNEGO silnika v10.0 [OKX SPOT USDC PRODUCTION]")

BACKGROUND_LOOP: Optional[asyncio.AbstractEventLoop] = None
PIPELINE_LOCK: Optional[asyncio.Lock] = None
ASYNC_SHUTDOWN_EVENT: Optional[asyncio.Event] = None
RATE_LIMITER: Optional[Any] = None
GLOBAL_WS_FEED: Optional[Any] = None

# Globalna definicja waluty kwotowanej
QUOTE_CCY = "USDC"

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
# WIZJER DIAGNOSTYCZNY AUTORYZACJI OKX (EUROPEJSKI KLASTER EEA)
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
                "tryb": "EEA_DEMO_SANDBOX",
                "http_status": resp.status,
                "okx_code": resp_data.get("code"),
                "okx_msg": resp_data.get("msg"),
                "data": resp_data.get("data")
            })
    except urllib.error.HTTPError as he:
        err_body = he.read().decode('utf-8', errors='ignore')
        report["trials"].append({
            "tryb": "EEA_DEMO_SANDBOX",
            "http_status": he.code,
            "response": err_body[:200]
        })
    except Exception as e:
        report["trials"].append({
            "tryb": "EEA_DEMO_SANDBOX",
            "exception": str(e)
        })

    return jsonify(report), 200

# =========================================================================
# REGULATOR PRZEPŁYWU SIECIOWEGO (TOKEN BUCKET RATE LIMITER)
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
# RDZEŃ QUANT 1: MEAN REVERSION (Z-SCORE + EMA 1H + RSI + ATR)
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
            
        current_price = prices[0] 
        z_score = (current_price - sma) / std_dev

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
# RDZEŃ QUANT 2: MOMENTUM (ROC TREND FOLLOWING)
# =========================================================================
class MomentumQuantCore:
    @staticmethod
    def calculate_momentum(candles: List[List[str]], period: int = 10) -> Optional[Dict[str, Any]]:
        if len(candles) < period + 1:
            return None
        closes = [float(c[4]) for c in candles]
        current_price = closes[-1]
        past_price = closes[-period - 1]
        if past_price == 0:
            return None
        roc = ((current_price - past_price) / past_price) * 100.0
        return {
            "roc": round(roc, 2),
            "current": current_price,
            "signal": roc > 2.0
        }

# =========================================================================
# RDZEŃ QUANT 3: BREAKOUT (BOLLINGER COMPRESSION + SQUEEZE)
# =========================================================================
class BreakoutQuantCore:
    @staticmethod
    def calculate_breakout(candles: List[List[str]], period: int = 20) -> Optional[Dict[str, Any]]:
        if len(candles) < period:
            return None
        closes = [float(c[4]) for c in candles]
        current_price = closes[-1]
        
        sma = sum(closes[-period:]) / period
        variance = sum((x - sma) ** 2 for x in closes[-period:]) / period
        std_dev = math.sqrt(variance) if variance > 0 else 1e-6
        
        upper_band = sma + (2.0 * std_dev)
        lower_band = sma - (2.0 * std_dev)
        bandwidth = (upper_band - lower_band) / sma if sma > 0 else 0.0
        
        is_compression = bandwidth < 0.015
        is_breakout_up = current_price > upper_band
        
        return {
            "bandwidth": round(bandwidth, 4),
            "upper_band": round(upper_band, 4),
            "signal": is_compression and is_breakout_up
        }

# =========================================================================
# RDZEŃ QUANT 4: GRID TRADING (SIATKA KONSOLIDACYJNA)
# =========================================================================
class GridQuantCore:
    @staticmethod
    def calculate_grid_levels(
        candles: List[List[str]], 
        grid_step_pct: float = 0.005, 
        levels: int = 3
    ) -> Optional[Dict[str, Any]]:
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

        is_consolidation = (0.2 <= atr_pct <= 0.8) and (abs(roc) < 1.0)

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
            "roc": round(roc, 2),
            "is_consolidation": is_consolidation,
            "levels": buy_levels
        }

# =========================================================================
# RDZEŃ ARBITRAŻU REŻIMÓW RYNKOWYCH & PAMIĘĆ PODRĘCZNA ŚWIEC
# =========================================================================
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
# KLIENT ASYNCHRONICZNY WEBSOCKET OKX (NASŁUCH CEN W CZASIE RZECZYWISTYM)
# =========================================================================
class OKXWebSocketPriceFeed:
    """Lekki, asynchroniczny klient WebSocket do ciągłego nasłuchu cen SPOT."""
    
    def __init__(self, session: aiohttp.ClientSession, is_sandbox: bool = True):
        self.session = session
        self.is_sandbox = is_sandbox
        self.ws_url = "wss://wspap.okx.com:8443/ws/v5/public" if is_sandbox else "wss://ws.okx.com:8443/ws/v5/public"
        self.latest_prices: Dict[str, float] = {}
        self._running: bool = False

    async def start_listener(self, symbols: list):
        """Utrzymuje stałe połączenie, obsługuje ping-pong i odnawia sesję po błędzie."""
        self._running = True
        sub_args = [{"channel": "tickers", "instId": sym} for sym in symbols]
        subscribe_msg = json.dumps({"op": "subscribe", "args": sub_args})

        while self._running and not (ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set()):
            try:
                logger.info(f"🌐 [WS-CONNECT] Łączenie ze strumieniem WebSocket OKX: {self.ws_url}...")
                async with self.session.ws_connect(self.ws_url, heartbeat=20) as ws:
                    await ws.send_str(subscribe_msg)
                    logger.info(f"📡 [WS-SUBSCRIBED] Aktywny nasłuch WebSocket dla: {symbols}")

                    async for msg in ws:
                        if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                            break
                        
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if "data" in data and len(data["data"]) > 0:
                                ticker = data["data"][0]
                                inst_id = ticker.get("instId")
                                last_price = ticker.get("last")
                                if inst_id and last_price:
                                    self.latest_prices[inst_id] = float(last_price)
                        elif msg.type in (aiohttp.WSMsgType.CLOSED, aiohttp.WSMsgType.ERROR):
                            logger.warning("⚠️ [WS-DISCONNECTED] Rozłączenie WebSocket. Ponawianie...")
                            break

            except Exception as e:
                logger.error(f"❌ [WS-ERROR] Błąd strumienia cen: {e}. Ponawianie za 5s...")
                await asyncio.sleep(5)

    def get_last_price(self, symbol: str) -> Optional[float]:
        return self.latest_prices.get(symbol)

# =========================================================================
# SYSTEMOWY KLIENT GIEŁDY OKX SPOT (V5 REST API - SPOT USDC)
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

    async def get_wallet_balances(self, ccy: str = "USDC") -> Dict[str, float]:
        """Pobiera całkowity kapitał (totalEq) oraz wolną gotówkę (availBal) dla waluty kwotowanej."""
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

                    return {
                        "total_equity": total_eq,
                        "available_cash": avail_cash
                    }
                return {"total_equity": 0.0, "available_cash": 0.0}
        except Exception as e:
            logger.error(f"❌ [OKX-WALLET-EXCEPTION] Błąd odczytu portfela: {e}")
            return {"total_equity": 0.0, "available_cash": 0.0}

    async def get_account_balance(self, ccy: str = "USDC") -> float:
        """
        Uniwersalna metoda pobierania salda dostępnego (availBal):
        Dla USDC zwraca wolne środki gotówkowe.
        Dla kryptowalut (BTC, ETH, SOL, XRP) zwraca faktyczną dostępną ilość w portfelu SPOT.
        """
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
                    details = account_data.get("details", [])
                    for bal in details:
                        if bal.get("ccy") == ccy:
                            return float(bal.get("availBal", 0.0))
                    if ccy == QUOTE_CCY:
                        return float(account_data.get("totalEq", 0.0))
                return 0.0
        except Exception as e:
            logger.error(f"❌ [OKX-BALANCE-EXCEPTION] Błąd pobierania salda dla {ccy}: {e}")
            return 0.0

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        if GLOBAL_WS_FEED:
            ws_price = GLOBAL_WS_FEED.get_last_price(symbol)
            if ws_price and ws_price > 0.0:
                return {
                    "source": "OKX_WS",
                    "symbol": symbol,
                    "last": ws_price
                }

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
                    candles = data["data"]
                    return [float(c[4]) for c in reversed(candles)]
                return []
        except Exception as e:
            logger.error(f"[OKX-CANDLES-EXCEPTION] Błąd pobierania świec makro {symbol}: {e}")
            return []

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
            logger.error(f"[OKX-RAW-CANDLES-EXCEPTION] Błąd pobierania surowych świec {symbol}: {e}")
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
            logger.error(f"[OKX-ORDER-ERROR] Błąd zlecenia Market {side} dla {symbol}: {e}")
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
                    orders = data.get("data", [])
                    return len(orders) > 0
                return True
        except Exception as e:
            logger.error(f"❌ [OKX-PENDING-CHECK-ERROR] Błąd sprawdzania otwartych zleceń {symbol}: {e}")
            return True
            
    async def get_algo_order_state(self, algo_id: str) -> Optional[str]:
        """Sprawdza status zlecenia algorytmicznego (OCO) na OKX."""
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()

        request_path = f"/api/v5/trade/order-algo?algoId={algo_id}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return data["data"][0].get("state")
                return None
        except Exception as e:
            logger.error(f"❌ [OKX-ALGO-STATE-ERROR] Błąd sprawdzania statusu zlecenia Algo {algo_id}: {e}")
            return None
            
    async def get_order_state(self, symbol: str, ord_id: str) -> Optional[str]:
        if not self.api_key or not self.secret_key or not self.passphrase:
            return None
        await self.rate_limiter.consume()

        request_path = f"/api/v5/trade/order?instId={symbol}&ordId={ord_id}"
        url = f"{self.base_url}{request_path}"
        headers = self._get_headers("GET", request_path)

        try:
            async with self.session.get(url, headers=headers, timeout=5) as resp:
                if resp.status != 200:
                    return None
                data = await resp.json()
                if data.get("code") == "0" and data.get("data"):
                    return data["data"][0].get("state")
                return None
        except Exception as e:
            logger.error(f"❌ [OKX-ORDER-STATE-ERROR] Błąd sprawdzania statusu zlecenia {ord_id}: {e}")
            return None

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
            logger.error(f"[OKX-LIMIT-ORDER-ERROR] Błąd zlecenia Limit {side} dla {symbol}: {e}")
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
            logger.error(f"❌ [OKX-CANCEL-ORDER-ERROR] Błąd anulowania zlecenia {ord_id}: {e}")
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
                logger.info(f"🛡️ [OKX-OCO-DEPLOYED] Zlecenie obronne OCO wysłane do OKX dla {symbol}: {res}")
                return res
        except Exception as e:
            logger.error(f"❌ [OKX-OCO-CRITICAL-ERROR] Awaria składania zlecenia OCO dla {symbol}: {e}")
            return None

# =========================================================================
# STRATEGIA 1: CENTRALNY POTOK POWROTU DO ŚREDNIEJ (KOSZYK ALFA: MAX 2)
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None:
        PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked():
        logger.debug("[POTOK-WARN] Poprzednia analiza wciąż trwa. Pomijam cykl.")
        return

    async with PIPELINE_LOCK:
        logger.info("🕵️ [POTOK OKX] Skanowanie koszyka 4 rynków SPOT USDC...")
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

            okx_client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=True)
            wallet = await okx_client.get_wallet_balances(QUOTE_CCY)
            total_balance = wallet.get("total_equity", 0.0)
            available_cash = wallet.get("available_cash", 0.0)

            # SPRAWDZENIE ZAJĘTOŚCI KOSZYKA ALFA (MAX 2 POZYCJE)
            alpha_active_count = 0
            try:
                url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
                async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as resp_k:
                    if resp_k.status == 200:
                        data_k = await resp_k.json()
                        alpha_active_count = len(data_k.get("result", []))
            except Exception as e:
                logger.error(f"⚠️ [SLOTS CHECK ERROR] Błąd weryfikacji slotów ALFA: {e}")

            instruments = [
                {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
                {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
                {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
                {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
            ]

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break

                ticker = await inst["client"].get_market_ticker(inst["symbol"])
                if not ticker:
                    logger.warning(f"⚠️ [{inst['label']}] Oczekiwanie na kwotowanie SPOT... Pomijam w tym cyklu.")
                    continue

                current_price = ticker.get("last", 0.0)
                await redis_trade.push_historical_tick(inst["label"], ticker, max_elements=50)
                history = await redis_trade.get_historical_ticks(inst["label"], max_elements=50)
                samples_count = len(history)

                logger.info(f"📥 [{inst['label']}] Kurs SPOT: {current_price} {QUOTE_CCY} | Bufor Redis: {samples_count}/20 próbek")

                if samples_count < 20:
                    logger.info(f"⏳ [{inst['label']}] Zbieranie historii ({samples_count}/20)... Silnik wstrzymuje analizę.")
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

                    if -1.5 < z <= -1.2 and trend == "LONG_ONLY":
                        await tg.push(
                            f"👀 <b>[OBSERWACJA: {inst['label']}]</b>\n"
                            f"Cena zbliża się do strefy wejścia!\n"
                            f"Z-Score: <code>{z}</code> | RSI: <code>{rsi}</code> | P: <code>{current_price}</code>"
                        )

                    if alpha_active_count >= 2:
                        logger.info(f"🛡️ [ALPHA LIMIT] Pozycje ALFA: {alpha_active_count}/2. Blokada nowych zakupów dla {inst['label']}.")
                        continue

                    if available_cash < 11.0:
                        logger.warning(f"⚠️ [MEAN-REV-LIQUIDITY] Wolna gotówka ({available_cash} {QUOTE_CCY}) < 11.0. Wstrzymuję zakup.")
                        continue

                    risk_capital = total_balance * 0.01
                    stop_loss_distance = atr * 2.0
                    sl_pct = (stop_loss_distance / current_price) if current_price > 0 else 0.02
                    sl_pct = max(0.01, sl_pct)
                    
                    position_value = min(risk_capital / sl_pct, total_balance * 0.25, available_cash * 0.95)
                    calculated_qty = round(position_value / current_price, inst["round_digits"])
                    calculated_qty = max(inst["min_qty"], calculated_qty)

                    order_value_quote = calculated_qty * current_price
                    if order_value_quote < 11.0:
                        calculated_qty = max(calculated_qty, round(11.0 / current_price, inst["round_digits"]))
                        calculated_qty = max(inst["min_qty"], calculated_qty)

                    if (calculated_qty * current_price) > available_cash:
                        continue

                    FORCE_TEST_EXECUTION = False  
                    standard_buy = FORCE_TEST_EXECUTION or (z <= -1.5 and trend == "LONG_ONLY" and rsi <= 35)
                    crash_buy = (z <= -2.5 and rsi <= 20)

                    if standard_buy or crash_buy:
                        logger.info(f"🚨 [EXECUTION-TRIGGER] Kupno SPOT dla {inst['label']} | Ilość: {calculated_qty}")
                        order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calculated_qty)

                        if order_res and order_res.get("code") == "0":
                            pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                            await redis_trade.push_historical_tick(
                                pos_key, 
                                {"status": "OPEN", "type": "MEAN_REVERSION", "qty": calculated_qty, "buy_price": current_price, "time": time.time()}, 
                                max_elements=1
                            )
                            alpha_active_count += 1

                            price_tp = round(current_price + (stop_loss_distance * 1.5), inst["price_round"])
                            price_sl = round(current_price - stop_loss_distance, inst["price_round"])

                            await asyncio.sleep(0.5)
                            base_ccy = inst["symbol"].split("-")[0]
                            real_avail_bal = await inst["client"].get_account_balance(base_ccy)
                            oco_qty = min(calculated_qty, real_avail_bal) if real_avail_bal > 0 else calculated_qty * 0.995
                            oco_qty = round(oco_qty, inst["round_digits"])
                            oco_qty = max(inst["min_qty"], oco_qty)

                            oco_res = await inst["client"].execute_oco_protection(inst["symbol"], oco_qty, price_tp, price_sl)
                            
                            oco_success = False
                            algo_id = ""
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                item = oco_res["data"][0]
                                if item.get("sCode") == "0" and item.get("algoId"):
                                    oco_success = True
                                    algo_id = item["algoId"]

                            if oco_success:
                                await redis_trade.push_historical_tick(
                                    pos_key, 
                                    {
                                        "status": "WAITING_OCO", 
                                        "algo_id": algo_id, 
                                        "qty": oco_qty, 
                                        "buy_price": current_price, 
                                        "tp_price": price_tp, 
                                        "sl_price": price_sl, 
                                        "time": time.time()
                                    }, 
                                    max_elements=1
                                )
                                await tg.push(
                                    f"🟩 <b>[OKX TRADING ENGINE: OCO DEPLOYED]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"🤖 Tryb: <b>SPOT (Mean Reversion - ALFA)</b>\n"
                                    f"📈 Instrument: <b>{inst['label']}</b>\n"
                                    f"💰 Kurs wejścia: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Wielkość: <b>{oco_qty}</b> (Ryzyko: 1% konta)\n"
                                    f"──────────────────────────────\n"
                                    f"🛡️ <b>OCHRONA OCO (ALGO):</b>\n"
                                    f"  • 🎯 Take Profit: <code>{price_tp} {QUOTE_CCY}</code>\n"
                                    f"  • 🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code>\n"
                                    f"──────────────────────────────"
                                )
                            else:
                                err_c = oco_res.get("code") if oco_res else "ERR"
                                err_m = oco_res.get("msg") if oco_res else "Timeout OCO"
                                logger.critical(f"🚨 [MEAN-REV-FAIL-SAFE] OCO odrzucone dla {inst['label']} ({err_c}: {err_m})! Natychmiastowa likwidacja...")
                                await inst["client"].execute_market_order(inst["symbol"], "sell", calculated_qty)
                                await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                        else:
                            err_c = order_res.get("code") if order_res else "ERR"
                            err_m = order_res.get("msg") if order_res else "Connection error"
                            logger.error(f"❌ [MEAN-REV-REJECTED] Błąd zlecenia {inst['label']}: Code {err_c} -> {err_m}")

            gc.collect()

# =========================================================================
# STRATEGIA 2: WORKER MOMENTUM W TLE (KOSZYK ALFA: MAX 2)
# =========================================================================
async def independent_momentum_worker(session, redis_trade, tg_dispatcher, okx_client):
    logger.info("🚀 [MOMENTUM-WORKER] Uruchomiono niezależny wątek analityczny Momentum w tle.")
    
    instruments = [
        {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}_MOM", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
        {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}_MOM", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
        {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}_MOM", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
        {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}_MOM", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
            async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as resp_k:
                active_keys = (await resp_k.json()).get("result", []) if resp_k.status == 200 else []
                active_count = len(active_keys)
            
            if active_count >= 2:
                logger.info("🛡️ [MOMENTUM] Limit 2 pozycji ALFA osiągnięty. Worker wstrzymuje skanowanie.")
                await asyncio.sleep(60)
                continue

            # RECONCILER DLA OCO MOMENTUM
            for inst in instruments:
                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                ticks = await redis_trade.get_historical_ticks(pos_key, max_elements=1)
                if ticks:
                    pos_data = ticks[0]
                    if pos_data.get("status") == "WAITING_OCO" and "algo_id" in pos_data:
                        algo_id = pos_data["algo_id"]
                        algo_state = await okx_client.get_algo_order_state(algo_id)
                        if algo_state in ["filled", "canceled", "order_failed"]:
                            logger.info(f"🧹 [MOMENTUM-RECONCILE] Zlecenie OCO dla {inst['label']} zakończone ({algo_state}). Zwalniam slot.")
                            await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                            target_k = f"{redis_trade.prefix}{pos_key}"
                            if target_k in active_keys:
                                active_keys.remove(target_k)

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break
                
                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                if f"{redis_trade.prefix}{pos_key}" in active_keys:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                regime = MarketRegimeArbitrator.get_regime(candles_raw)
                if regime == "RANGING":
                    logger.debug(f"🛑 [MOMENTUM-BLOCK] {inst['label']} w reżimie RANGING. Momentum wygaszone.")
                    continue

                mom_metrics = MomentumQuantCore.calculate_momentum(candles_raw, period=10)
                if mom_metrics:
                    logger.info(f"📈 [MOMENTUM-SCAN] {inst['label']} | Reżim: {regime} | ROC: {mom_metrics['roc']}% (Próg: > +2.0%) | P: {mom_metrics['current']}")
                    
                    if mom_metrics["signal"]:
                        logger.info(f"🚨 [MOMENTUM-TRIGGER] Spełniono warunek impulsu dla {inst['label']}!")
                        current_price = mom_metrics["current"]
                        
                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)
                        
                        if available_cash < 11.0:
                            logger.warning(f"⚠️ [MOMENTUM-LIQUIDITY] Wolna gotówka ({available_cash} {QUOTE_CCY}) < 11.0. Pomijam {inst['label']}.")
                            continue

                        risk_capital = total_balance * 0.01
                        sl_pct = 0.02
                        position_value = min(risk_capital / sl_pct, total_balance * 0.25, available_cash * 0.95)
                        
                        calculated_qty = round(position_value / current_price, inst["round_digits"])
                        calculated_qty = max(inst["min_qty"], calculated_qty)
                        
                        if (calculated_qty * current_price) < 11.0:
                            calculated_qty = max(calculated_qty, round(11.0 / current_price, inst["round_digits"]))
                            calculated_qty = max(inst["min_qty"], calculated_qty)

                        if (calculated_qty * current_price) > available_cash:
                            logger.warning(f"⚠️ [MOMENTUM-MARGIN] Zlecenie przekracza dostępne saldo. Pomijam {inst['label']}.")
                            continue

                        order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calculated_qty)
                        if order_res and order_res.get("code") == "0":
                            await redis_trade.push_historical_tick(
                                pos_key, 
                                {
                                    "status": "OPEN", 
                                    "type": "MOMENTUM", 
                                    "qty": calculated_qty, 
                                    "buy_price": current_price, 
                                    "time": time.time()
                                }, 
                                max_elements=1
                            )
                            active_keys.append(f"{redis_trade.prefix}{pos_key}")
                            logger.info(f"🔒 [MOMENTUM-STATE-LOCKED] Pozycja {inst['label']} atomowo zabezpieczona w Redis.")

                            price_tp = round(current_price * 1.03, inst["price_round"])
                            price_sl = round(current_price * 0.98, inst["price_round"])
                            
                            await asyncio.sleep(0.5)
                            base_ccy = inst["symbol"].split("-")[0]
                            real_avail_bal = await inst["client"].get_account_balance(base_ccy)
                            
                            oco_qty = min(calculated_qty, real_avail_bal) if real_avail_bal > 0 else calculated_qty * 0.995
                            oco_qty = round(oco_qty, inst["round_digits"])
                            oco_qty = max(inst["min_qty"], oco_qty)
                            
                            oco_res = await inst["client"].execute_oco_protection(inst["symbol"], oco_qty, price_tp, price_sl)
                            
                            oco_success = False
                            algo_id = ""
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                item = oco_res["data"][0]
                                if item.get("sCode") == "0" and item.get("algoId"):
                                    oco_success = True
                                    algo_id = item["algoId"]

                            if oco_success:
                                await redis_trade.push_historical_tick(
                                    pos_key, 
                                    {
                                        "status": "WAITING_OCO", 
                                        "algo_id": algo_id, 
                                        "qty": oco_qty, 
                                        "buy_price": current_price, 
                                        "tp_price": price_tp, 
                                        "sl_price": price_sl, 
                                        "time": time.time()
                                    }, 
                                    max_elements=1
                                )
                                await tg_dispatcher.push(
                                    f"🚀 <b>[MOMENTUM ENGINE: TRADE DEPLOYED]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"📈 Instrument: <b>{inst['label']}</b> | ROC: <code>{mom_metrics['roc']}%</code>\n"
                                    f"💰 Wejście: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Wielkość: <b>{oco_qty}</b> (Ochrona OCO aktywna)\n"
                                    f"🎯 TP (+3%): <code>{price_tp} {QUOTE_CCY}</code> | 🛑 SL (-2%): <code>{price_sl} {QUOTE_CCY}</code>"
                                )
                            else:
                                err_c = oco_res.get("code") if oco_res else "ERR"
                                err_m = oco_res.get("msg") if oco_res else "Timeout OCO"
                                logger.critical(f"🚨 [MOMENTUM-FAIL-SAFE] OCO odrzucone dla {inst['label']} ({err_c}: {err_m})! Likwidacja...")
                                await inst["client"].execute_market_order(inst["symbol"], "sell", calculated_qty)
                                await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                                if f"{redis_trade.prefix}{pos_key}" in active_keys:
                                    active_keys.remove(f"{redis_trade.prefix}{pos_key}")
                        else:
                            err_c = order_res.get("code") if order_res else "ERR"
                            err_m = order_res.get("msg") if order_res else "Connection error"
                            logger.error(f"❌ [MOMENTUM-REJECTED] Błąd zlecenia {inst['label']}: Code {err_c} -> {err_m}")
        except Exception as e:
            logger.error(f"❌ [MOMENTUM-ERROR] Błąd w workerze Momentum: {e}")
        
        await asyncio.sleep(180)

# =========================================================================
# STRATEGIA 3: WORKER BREAKOUT W TLE (KOSZYK ALFA: MAX 2)
# =========================================================================
async def independent_breakout_worker(session, redis_trade, tg_dispatcher, okx_client):
    logger.info("💥 [BREAKOUT-WORKER] Uruchomiono niezależny wątek Breakout w tle.")
    
    instruments = [
        {"client": okx_client, "symbol": f"BTC-{QUOTE_CCY}", "label": f"BTC_{QUOTE_CCY}_BRK", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
        {"client": okx_client, "symbol": f"ETH-{QUOTE_CCY}", "label": f"ETH_{QUOTE_CCY}_BRK", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
        {"client": okx_client, "symbol": f"SOL-{QUOTE_CCY}", "label": f"SOL_{QUOTE_CCY}_BRK", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
        {"client": okx_client, "symbol": f"XRP-{QUOTE_CCY}", "label": f"XRP_{QUOTE_CCY}_BRK", "min_qty": 1.0, "round_digits": 2, "price_round": 4}
    ]

    while not ASYNC_SHUTDOWN_EVENT.is_set():
        try:
            url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:ALPHA:*"
            async with session.get(url_keys, headers=redis_trade.headers, timeout=3) as resp_k:
                active_keys = (await resp_k.json()).get("result", []) if resp_k.status == 200 else []
                active_count = len(active_keys)
            
            if active_count >= 2:
                logger.info(f"🛡️ [BREAKOUT] Limit {active_count}/2 pozycji ALFA osiągnięty. Worker wstrzymuje skanowanie.")
                await asyncio.sleep(60)
                continue

            # RECONCILER DLA OCO BREAKOUT
            for inst in instruments:
                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                ticks = await redis_trade.get_historical_ticks(pos_key, max_elements=1)
                if not ticks:
                    continue
                pos_data = ticks[0]

                if pos_data.get("status") == "WAITING_OCO" and "algo_id" in pos_data:
                    algo_id = pos_data["algo_id"]
                    algo_state = await okx_client.get_algo_order_state(algo_id)

                    if algo_state in ["filled", "canceled", "order_failed"]:
                        logger.info(f"🧹 [BREAKOUT-RECONCILE] Zlecenie OCO dla {inst['label']} zmieniło stan na '{algo_state}'. Zwalniam slot Alfa.")
                        await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                        
                        target_key = f"{redis_trade.prefix}{pos_key}"
                        if target_key in active_keys:
                            active_keys.remove(target_key)

                        await tg_dispatcher.push(
                            f"🏁 <b>[BREAKOUT ENGINE: POSITION CLOSED]</b>\n"
                            f"Instrument: <b>{inst['label']}</b>\n"
                            f"Status OCO na giełdzie: <code>{algo_state}</code>. Slot Alfa zwolniony."
                        )

            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set():
                    break
                
                pos_key = f"POS_ACTIVE:ALPHA:{inst['label']}"
                if f"{redis_trade.prefix}{pos_key}" in active_keys:
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                brk_metrics = BreakoutQuantCore.calculate_breakout(candles_raw, period=20)
                
                if brk_metrics:
                    current_price = float(candles_raw[-1][4])
                    logger.info(f"💥 [BREAKOUT-SCAN] {inst['label']} | Bw: {brk_metrics['bandwidth']} (Komp < 0.015) | P: {current_price} vs Banda: {brk_metrics['upper_band']}")
                    
                    if brk_metrics["signal"]:
                        logger.info(f"🚨 [BREAKOUT-TRIGGER] Wykryto potwierdzone wybicie dla {inst['label']}!")
                        
                        wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                        total_balance = wallet.get("total_equity", 0.0)
                        available_cash = wallet.get("available_cash", 0.0)
                        
                        if available_cash < 11.0:
                            logger.warning(f"⚠️ [BREAKOUT-LIQUIDITY] Wolna gotówka ({available_cash} {QUOTE_CCY}) < 11.0. Wstrzymuję zakup {inst['label']}.")
                            continue

                        risk_capital = total_balance * 0.01
                        sl_pct = 0.02
                        position_value = min(risk_capital / sl_pct, total_balance * 0.25, available_cash * 0.95)
                        
                        calculated_qty = round(position_value / current_price, inst["round_digits"])
                        calculated_qty = max(inst["min_qty"], calculated_qty)
                        
                        if (calculated_qty * current_price) < 11.0:
                            calculated_qty = max(calculated_qty, round(11.0 / current_price, inst["round_digits"]))
                            calculated_qty = max(inst["min_qty"], calculated_qty)

                        if (calculated_qty * current_price) > available_cash:
                            logger.warning(f"⚠️ [BREAKOUT-MARGIN] Zlecenie przekracza dostępne saldo gotówki. Pomijam {inst['label']}.")
                            continue

                        order_res = await inst["client"].execute_market_order(inst["symbol"], "buy", calculated_qty)
                        
                        if order_res and order_res.get("code") == "0":
                            await redis_trade.push_historical_tick(
                                pos_key, 
                                {
                                    "status": "OPEN", 
                                    "type": "BREAKOUT", 
                                    "qty": calculated_qty, 
                                    "buy_price": current_price, 
                                    "time": time.time()
                                }, 
                                max_elements=1
                            )
                            active_keys.append(f"{redis_trade.prefix}{pos_key}")
                            logger.info(f"🔒 [BREAKOUT-STATE-LOCKED] Pozycja {inst['label']} atomowo zabezpieczona w Redis.")

                            price_tp = round(current_price * 1.04, inst["price_round"])
                            price_sl = round(current_price * 0.98, inst["price_round"])
                            
                            # DYNAMICZNY ODCZYT RZECZYWISTEGO SALDA DLA OCO
                            await asyncio.sleep(0.5)
                            base_ccy = inst["symbol"].split("-")[0]
                            real_avail_bal = await inst["client"].get_account_balance(base_ccy)
                            
                            oco_qty = min(calculated_qty, real_avail_bal) if real_avail_bal > 0 else calculated_qty * 0.995
                            oco_qty = round(oco_qty, inst["round_digits"])
                            oco_qty = max(inst["min_qty"], oco_qty)
                            
                            logger.info(f"📦 [BREAKOUT-OCO-CALC] Kupiono: {calculated_qty} | W portfelu: {real_avail_bal} | Do OCO: {oco_qty} {base_ccy}")
                            
                            oco_res = await inst["client"].execute_oco_protection(inst["symbol"], oco_qty, price_tp, price_sl)
                            
                            oco_success = False
                            algo_id = ""
                            if oco_res and oco_res.get("code") == "0" and oco_res.get("data"):
                                item = oco_res["data"][0]
                                if item.get("sCode") == "0" and item.get("algoId"):
                                    oco_success = True
                                    algo_id = item["algoId"]

                            if oco_success:
                                await redis_trade.push_historical_tick(
                                    pos_key, 
                                    {
                                        "status": "WAITING_OCO", 
                                        "algo_id": algo_id, 
                                        "qty": oco_qty, 
                                        "buy_price": current_price, 
                                        "tp_price": price_tp, 
                                        "sl_price": price_sl, 
                                        "time": time.time()
                                    }, 
                                    max_elements=1
                                )
                                await tg_dispatcher.push(
                                    f"💥 <b>[BREAKOUT ENGINE: TRADE DEPLOYED]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"📈 Instrument: <b>{inst['label']}</b> | Bw: <code>{brk_metrics['bandwidth']}</code>\n"
                                    f"💰 Wejście: <b>{current_price} {QUOTE_CCY}</b>\n"
                                    f"📦 Wielkość: <b>{oco_qty}</b> (Ochrona OCO aktywna)\n"
                                    f"🎯 TP (+4%): <code>{price_tp} {QUOTE_CCY}</code> | 🛑 SL (-2%): <code>{price_sl} {QUOTE_CCY}</code>"
                                )
                            else:
                                err_c = oco_res.get("code") if oco_res else "ERR"
                                err_m = oco_res.get("msg") if oco_res else "Timeout OCO"
                                logger.critical(f"🚨 [FAIL-SAFE-TRIGGERED] OCO odrzucone dla {inst['label']} (Kod: {err_c} | Msg: {err_m})! Awaryjna likwidacja do gotówki...")
                                
                                await inst["client"].execute_market_order(inst["symbol"], "sell", calculated_qty)
                                await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                                if f"{redis_trade.prefix}{pos_key}" in active_keys:
                                    active_keys.remove(f"{redis_trade.prefix}{pos_key}")

                                await tg_dispatcher.push(
                                    f"🚨 <b>[FAIL-SAFE KILL: POZYCJA ZLIKWIDOWANA]</b>\n"
                                    f"Pozycja <b>{inst['label']}</b> została natychmiast zamknięta zleceniem Market z powodu błędu zlecenia obronnego OCO.\n"
                                    f"Błąd OKX: <code>{err_c}</code> - <i>{err_m}</i>"
                                )
                        else:
                            err_code = order_res.get("code") if order_res else "BRAK_ODPOWIEDZI"
                            err_msg = order_res.get("msg") if order_res else "Timeout lub błąd połączenia"
                            logger.error(f"❌ [BREAKOUT-REJECTED] Odrzucono zlecenie dla {inst['label']}! Kod: {err_code} | Komunikat: {err_msg}")
        except Exception as e:
            logger.error(f"❌ [BREAKOUT-ERROR] Błąd w workerze Breakout: {e}")
        
        await asyncio.sleep(180)

# =========================================================================
# STRATEGIA 4: WORKER GRID TRADING (DEDYKOWANY KOSZYK GRID: MAX 3 SLOTY)
# =========================================================================
async def independent_grid_worker(session, redis_trade, tg_dispatcher, okx_client):
    logger.info("🧱 [GRID-WORKER] Uruchomiono niezależny wątek Grid Trading w tle (Limit: 3 sloty).")
    
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
                existing_ticks = await redis_trade.get_historical_ticks(pos_key, max_elements=1)
                active_pos = existing_ticks[0] if existing_ticks else None

                if active_pos and active_pos.get("status") == "PENDING_BUY":
                    ord_id = active_pos.get("order_id")
                    state = await inst["client"].get_order_state(inst["symbol"], ord_id)

                    if state == "filled":
                        raw_qty = float(active_pos["qty"])
                        qty_to_sell = round(raw_qty * 0.998, inst["round_digits"])
                        qty_to_sell = max(inst["min_qty"], qty_to_sell)

                        tp_price = float(active_pos["tp_price"])
                        sl_price = float(active_pos.get("sl_price", 0.0))
                        logger.info(f"🎯 [GRID-FILL-DETECTED] Kupno {ord_id} dla {inst['label']} zrealizowane! Wystawiam TP: {tp_price} (ilość: {qty_to_sell}) | SL: {sl_price}")

                        sell_res = await inst["client"].execute_limit_order(inst["symbol"], "sell", qty_to_sell, tp_price)
                        
                        if sell_res and sell_res.get("code") == "0":
                            sell_ord_id = sell_res["data"][0]["ordId"]
                            await redis_trade.push_historical_tick(
                                pos_key, 
                                {
                                    "status": "WAITING_TP", 
                                    "sell_ord_id": sell_ord_id, 
                                    "buy_price": active_pos["buy_price"], 
                                    "tp_price": tp_price, 
                                    "sl_price": sl_price, 
                                    "qty": qty_to_sell, 
                                    "time": time.time()
                                }, 
                                max_elements=1
                            )
                            await tg_dispatcher.push(
                                f"🎯 <b>[GRID ENGINE: TAKE PROFIT DEPLOYED]</b>\n"
                                f"──────────────────────────────\n"
                                f"📈 Instrument: <b>{inst['label']}</b>\n"
                                f"💰 Kupiono po: <code>{active_pos['buy_price']} {QUOTE_CCY}</code>\n"
                                f"📤 Wystawiono TP: <b>{tp_price} {QUOTE_CCY} (+0.5%)</b>\n"
                                f"🛑 Stop Loss: <code>{sl_price} {QUOTE_CCY} (-1.5%)</code>\n"
                                f"📦 Ilość: <b>{qty_to_sell}</b>"
                            )
                        else:
                            err_c = sell_res.get("code") if sell_res else "BRAK_ODPOWIEDZI"
                            err_m = sell_res.get("msg") if sell_res else "Timeout lub błąd sieci"
                            logger.error(f"❌ [GRID-TP-REJECTED] Odrzucono zlecenie TP dla {inst['label']}! Kod: {err_c} | Msg: {err_m}")
                        continue

                    elif state in ["canceled", "cancelled"]:
                        logger.info(f"🧹 [GRID-CLEANUP] Zlecenie {ord_id} dla {inst['label']} anulowane. Zwalniam slot GRID.")
                        await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                        continue
                    else:
                        continue

                elif active_pos and active_pos.get("status") == "WAITING_TP":
                    sell_ord_id = active_pos.get("sell_ord_id")
                    sell_state = await inst["client"].get_order_state(inst["symbol"], sell_ord_id)
                    
                    if sell_state == "filled":
                        logger.info(f"🎉 [GRID-CYCLE-COMPLETE] Zrealizowano TP na {inst['label']}! Zwalniam slot GRID.")
                        await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                        await tg_dispatcher.push(f"✅ <b>[GRID PROFIT TAKEN]</b> Pozycja na <b>{inst['label']}</b> zamknięta z zyskiem (+0.5%)!")
                        continue

                    current_market_price = None
                    if GLOBAL_WS_FEED:
                        current_market_price = GLOBAL_WS_FEED.get_last_price(inst["symbol"])
                    
                    if not current_market_price:
                        candles_check = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                        if candles_check:
                            current_market_price = float(candles_check[-1][4])

                    sl_trigger_price = float(active_pos.get("sl_price", 0.0))
                    if current_market_price and sl_trigger_price > 0 and current_market_price <= sl_trigger_price:
                        logger.warning(f"🚨 [GRID-STOP-LOSS] Cena {current_market_price} osiągnęła próg SL ({sl_trigger_price}) dla {inst['label']}! Awaryjne wyjście...")
                        await inst["client"].cancel_order(inst["symbol"], sell_ord_id)
                        await inst["client"].execute_market_order(inst["symbol"], "sell", float(active_pos["qty"]))
                        await redis_trade.push_historical_tick(pos_key, {"status": "CLOSED"}, max_elements=1)
                        await tg_dispatcher.push(
                            f"🛑 <b>[GRID STOP-LOSS TRIGGERED]</b>\n"
                            f"Pozycja <b>{inst['label']}</b> zamknięta obronnie po cenie <code>{current_market_price} {QUOTE_CCY}</code> (-1.5%)."
                        )
                        continue
                    continue

                # POLOWANIE NA NOWE WEJŚCIE W SIATKĘ
                if grid_active_count >= 3:
                    continue

                if await inst["client"].has_open_orders(inst["symbol"]):
                    continue

                candles_raw = await MarketRegimeArbitrator.get_candles(inst["client"], inst["symbol"])
                if not candles_raw:
                    continue

                regime = MarketRegimeArbitrator.get_regime(candles_raw)
                if regime == "TRENDING":
                    continue

                grid_metrics = GridQuantCore.calculate_grid_levels(candles_raw, grid_step_pct=0.005, levels=3)

                if grid_metrics and grid_metrics["is_consolidation"]:
                    first_level = grid_metrics["levels"][0]
                    price_buy = round(first_level["buy_price"], inst["price_round"])
                    price_tp = round(first_level["tp_price"], inst["price_round"])
                    price_sl = round(price_buy * 0.985, inst["price_round"])

                    wallet = await inst["client"].get_wallet_balances(QUOTE_CCY)
                    total_balance = wallet.get("total_equity", 0.0)
                    available_cash = wallet.get("available_cash", 0.0)

                    if available_cash < 11.0:
                        continue

                    risk_capital = total_balance * 0.01
                    sl_pct = 0.015
                    position_value = min(risk_capital / sl_pct, total_balance * 0.15, available_cash * 0.95)

                    calculated_qty = round(position_value / price_buy, inst["round_digits"])
                    calculated_qty = max(inst["min_qty"], calculated_qty)

                    if (calculated_qty * price_buy) < 11.0:
                        calculated_qty = max(calculated_qty, round(11.0 / price_buy, inst["round_digits"]))
                        calculated_qty = max(inst["min_qty"], calculated_qty)

                    if (calculated_qty * price_buy) > available_cash:
                        continue

                    logger.info(f"🚨 [GRID-TRIGGER] Składanie zlecenia Limit dla {inst['label']} | Cena: {price_buy} | Ilość: {calculated_qty}")
                    order_res = await inst["client"].execute_limit_order(inst["symbol"], "buy", calculated_qty, price_buy)

                    if order_res and order_res.get("code") == "0":
                        ord_id = order_res["data"][0]["ordId"]
                        await redis_trade.push_historical_tick(
                            pos_key, 
                            {
                                "status": "PENDING_BUY", 
                                "order_id": ord_id, 
                                "buy_price": price_buy, 
                                "tp_price": price_tp, 
                                "sl_price": price_sl, 
                                "qty": calculated_qty, 
                                "time": time.time()
                            }, 
                            max_elements=1
                        )
                        grid_active_count += 1

                        await tg_dispatcher.push(
                            f"🧱 <b>[GRID ENGINE: LIMIT ORDER PLACED]</b>\n"
                            f"──────────────────────────────\n"
                            f"📈 Instrument: <b>{inst['label']}</b>\n"
                            f"📥 Kupno (Limit L1): <b>{price_buy} {QUOTE_CCY}</b>\n"
                            f"📦 Wielkość: <b>{calculated_qty}</b>\n"
                            f"🎯 Planowany TP: <code>{price_tp} {QUOTE_CCY}</code> (+0.5%)\n"
                            f"🛑 Stop Loss: <code>{price_sl} {QUOTE_CCY}</code> (-1.5%)"
                        )

        except Exception as e:
            logger.error(f"❌ [GRID-ERROR] Błąd w workerze Grid: {e}")

        await asyncio.sleep(180)

# =========================================================================
# ASYNCHRONICZNY WĄTEK SPOCZYNKOWY (MULTI-TASKING CRON + WEBSOCKET FEED)
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT, RATE_LIMITER, GLOBAL_WS_FEED
    logger.info("⚡ [TRADING MULTI-TASKING ONLINE] Uruchamianie workerów tła (WS + Momentum + Breakout + Grid)...")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
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
        okx_client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=True)
        ws_feed = OKXWebSocketPriceFeed(session, is_sandbox=True)
        GLOBAL_WS_FEED = ws_feed

        symbols_to_stream = [f"BTC-{QUOTE_CCY}", f"ETH-{QUOTE_CCY}", f"SOL-{QUOTE_CCY}", f"XRP-{QUOTE_CCY}"]

        ws_task = None
        momentum_task = None
        breakout_task = None
        grid_task = None
        try:
            ws_task = asyncio.create_task(ws_feed.start_listener(symbols_to_stream))
            momentum_task = asyncio.create_task(independent_momentum_worker(session, redis_trade, tg, okx_client))
            breakout_task = asyncio.create_task(independent_breakout_worker(session, redis_trade, tg, okx_client))
            grid_task = asyncio.create_task(independent_grid_worker(session, redis_trade, tg, okx_client))

            while not ASYNC_SHUTDOWN_EVENT.is_set():
                await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"❌ [CRON-LOOP-ERROR] Krytyczny błąd w pętli wielozadaniowej: {e}")
        finally:
            tasks = [t for t in [ws_task, momentum_task, breakout_task, grid_task] if t]
            for t in tasks:
                t.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

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
# PUBLICZNE ENDPOINTY STERUJĄCE
# =========================================================================
@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Potok tradingu nie jest gotowy."}), 500
    asyncio.run_coroutine_threadsafe(run_async_pipeline(), BACKGROUND_LOOP)
    return jsonify({"status": "success", "message": f"Analiza rynków OKX ({QUOTE_CCY}) uruchomiona pomyślnie."}), 200

@app.route('/emergency-liquidate', methods=['GET', 'POST'])
def emergency_liquidate_to_cash():
    """Awaryjne odwołanie zleceń, zrzucenie wszystkich pozycji SPOT do USDC i wyczyszczenie Redis."""
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla bota nie jest aktywna."}), 500

    async def _execute_flush():
        async with aiohttp.ClientSession() as session:
            client = OKXSpotClient(session, RATE_LIMITER, is_sandbox=True)
            redis_trade = UpstashRedisTradingBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""),
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""),
                session
            )
            
            report = {"cancelled_orders": [], "liquidated": [], "redis_cleaned": False}
            symbols_to_flush = [
                ("BTC", f"BTC-{QUOTE_CCY}", 5), 
                ("ETH", f"ETH-{QUOTE_CCY}", 4), 
                ("SOL", f"SOL-{QUOTE_CCY}", 2), 
                ("XRP", f"XRP-{QUOTE_CCY}", 2)
            ]

            # 1. ANULOWANIE WSZELKICH ZLECEŃ ZWYKŁYCH PRZED WYPRZEDAŻĄ
            for ccy, symbol, _ in symbols_to_flush:
                try:
                    await client.rate_limiter.consume()
                    req_p_pend = f"/api/v5/trade/orders-pending?instId={symbol}"
                    headers = client._get_headers("GET", req_p_pend)
                    async with session.get(f"{client.base_url}{req_p_pend}", headers=headers, timeout=4) as r_pend:
                        d_pend = await r_pend.json()
                        if d_pend.get("code") == "0":
                            for ord_item in d_pend.get("data", []):
                                o_id = ord_item.get("ordId")
                                await client.cancel_order(symbol, o_id)
                                report["cancelled_orders"].append({"symbol": symbol, "ordId": o_id})
                except Exception as ex:
                    logger.error(f"⚠️ [EMERGENCY] Błąd czyszczenia zleceń dla {symbol}: {ex}")

            # 2. RYNKOWA WYPRZEDAŻ AKTYWÓW BAZOWYCH DO USDC
            for ccy, symbol, round_d in symbols_to_flush:
                bal = await client.get_account_balance(ccy)
                if bal > 0.0001:
                    qty = round(bal * 0.999, round_d)
                    res = await client.execute_market_order(symbol, "sell", qty)
                    report["liquidated"].append({"symbol": symbol, "qty": qty, "res": res})
                    logger.info(f"🚨 [EMERGENCY-FLUSH] Awaryjnie sprzedano {qty} {ccy} do {QUOTE_CCY}: {res}")
            
            # 3. CZYSZCZENIE WSZYSTKICH KLUCZY W REDIS Z PREFIKSEM TRADE_
            url_keys = f"{redis_trade.url}/keys/{redis_trade.prefix}POS_ACTIVE:*"
            async with session.get(url_keys, headers=redis_trade.headers) as r_keys:
                if r_keys.status == 200:
                    keys = (await r_keys.json()).get("result", [])
                    if keys:
                        del_payload = [["DEL"] + keys]
                        await session.post(f"{redis_trade.url}/pipeline", json=del_payload, headers=redis_trade.headers)
                        report["redis_cleaned"] = True
                        logger.info(f"🧹 [EMERGENCY-FLUSH] Usunięto zablokowane klucze Redis: {keys}")
            
            return report

    fut = asyncio.run_coroutine_threadsafe(_execute_flush(), BACKGROUND_LOOP)
    try:
        res = fut.result(timeout=20)
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
