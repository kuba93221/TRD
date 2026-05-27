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
import requests
from datetime import datetime, timedelta
from flask import Flask, jsonify
from typing import Dict, Any, List, Optional

# =========================================================================
# SYSTEMOWY MODUŁ OBSERVABILITY & GLOBAL CONTEXT
# =========================================================================
LOG_LEVEL_CONFIG = os.environ.get("LOG_LEVEL", "DEBUG").upper()  
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL_CONFIG, logging.DEBUG), 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Algorithmic_Trading_Engine_v5.5_DEV")

logger.info("⚙️ [SYSTEM-INIT] Uruchamianie PEŁNEGO bota w bezpiecznej gałęzi DEV (Crypto.com + XTB xAPI na Tokenach)")

BACKGROUND_LOOP = None
PIPELINE_LOCK = None  
ASYNC_SHUTDOWN_EVENT = None 

ASSET_MUTEXES: Dict[str, asyncio.Lock] = {}
ASSET_MUTEX_LOCK = threading.Lock()

def get_asset_lock(asset_id_str: str) -> asyncio.Lock:
    with ASSET_MUTEX_LOCK:
        if asset_id_str not in ASSET_MUTEXES:
            ASSET_MUTEXES[asset_id_str] = asyncio.Lock()
        return ASSET_MUTEXES[asset_id_str]

def clear_asset_lock(asset_id_str: str):
    with ASSET_MUTEX_LOCK:
        if asset_id_str in ASSET_MUTEXES:
            del ASSET_MUTEXES[asset_id_str]

def sigterm_handler(signum, frame):
    logger.info("📥 [SIGTERM] Sygnał zamknięcia od Render. Aktywacja Graceful Shutdown dla XTB i Crypto...")
    if BACKGROUND_LOOP and ASYNC_SHUTDOWN_EVENT:
        BACKGROUND_LOOP.call_soon_threadsafe(ASYNC_SHUTDOWN_EVENT.set)

signal.signal(signal.SIGTERM, sigterm_handler)

# =========================================================================
# SERWER MONITORINGU FLASK (HEALTH CHECK & API ENDPOINTS)
# =========================================================================
app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return "TRADING_ENGINE_DOWN", 503
    return "OK", 200

RATE_LIMITER = None

# =========================================================================
# REGULATOR PRZEPŁUWU SIECIOWEGO (TOKEN BUCKET RATE LIMITER)
# =========================================================================
class TokenBucketRateLimiter:
    def __init__(self, tokens_per_second: float = 3.0, max_capacity: float = 6.0):
        self.rate = tokens_per_second
        self.capacity = max_capacity
        self.tokens = max_capacity
        self.last_check = time.monotonic()
        self._lock = None  

    async def consume(self):
        if self._lock is None: 
            self._lock = asyncio.Lock()
        async with self._lock:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.last_check) * self.rate)
            self.last_check = now
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                logger.debug(f"⏳ [RATE LIMITER] Oczekiwanie na token sieciowy: {wait_time:.4f}s...")
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
                self.last_check = time.monotonic()
            else:
                self.tokens -= 1.0

# =========================================================================
# POMOST UPSTASH REDIS (RYGORYSTYCZNA SEPARACJA PRZESTRZENI 'TRADE_')
# =========================================================================
class UpstashRedisTradingBridge:
    def __init__(self, url: str, token: str, session: aiohttp.ClientSession):
        self.url = url.rstrip('/') if url else ""
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.session = session
        self.prefix = "TRADE_"

    def _enforce_prefix(self, key: str) -> str:
        return key if key.startswith(self.prefix) else f"{self.prefix}{key}"

    async def get_state(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.url: return None
        safe_key = self._enforce_prefix(key)
        try:
            async with self.session.get(f"{self.url}/get/{safe_key}", headers=self.headers, timeout=4) as response:
                if response.status != 200: return None
                hex_res = (await response.json()).get("result")
                if hex_res and hex_res not in ["None", "NULL"]:
                    return msgpack.unpackb(bytes.fromhex(hex_res), strict_map_key=False)
                return None
        except Exception as e:
            logger.error(f"❌ [REDIS GET ERROR] Błąd klucza {safe_key}: {e}")
            return None

    async def set_state(self, key: str, value: Dict[str, Any], ttl: Optional[int] = 86400) -> bool:
        if not self.url: return False
        safe_key = self._enforce_prefix(key)
        try:
            hex_str = msgpack.packb(value, use_bin_type=True).hex()
            suffix = f"?EX={ttl}" if ttl else ""
            async with self.session.get(f"{self.url}/set/{safe_key}/{hex_str}{suffix}", headers=self.headers, timeout=4) as response:
                if response.status == 200:
                    await response.read()
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS SET ERROR] Błąd zapisu {safe_key}: {e}")
            return False

    async def push_historical_tick(self, market_id: str, tick_data: Dict[str, Any], max_elements: int = 50) -> bool:
        if not self.url: return False
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            hex_str = msgpack.packb(tick_data, use_bin_type=True).hex()
            async with self.session.get(f"{self.url}/lpush/{safe_key}/{hex_str}", headers=self.headers, timeout=3) as resp:
                if resp.status != 200: return False
                await resp.read()
            async with self.session.get(f"{self.url}/ltrim/{safe_key}/0/{max_elements - 1}", headers=self.headers, timeout=3) as resp:
                if resp.status == 200:
                    await resp.read()
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS FIFO ERROR] Blad kolejki dla {market_id}: {e}")
            return False

    async def incr_metric(self, field_name: str):
        if not self.url: return
        key = self._enforce_prefix(f"ANALYTICS:{field_name}:{datetime.utcnow().strftime('%Y-%m-%d')}")
        try:
            async with self.session.get(f"{self.url}/incr/{key}", headers=self.headers, timeout=3) as resp: 
                await resp.read()
        except Exception: 
            pass

# =========================================================================
# MOSTEK POWIADOMIEŃ TELEGRAM (PACZKOWANIE KOMUNIKATÓW)
# =========================================================================
class TelegramThrottledDispatcher:
    def __init__(self, token: str, chat_id: str, session: aiohttp.ClientSession):
        self.token = token
        self.chat_id = chat_id
        self.session = session
        self.buffer: List[str] = []
        self.batch_size = 5
        self._lock = None  

    async def push(self, text: str, json_payload: Dict[str, Any]):
        if self._lock is None: 
            self._lock = asyncio.Lock()
        async with self._lock:
            self.buffer.append(text)
            if len(self.buffer) >= self.batch_size: 
                await self._flush_buffer()

    async def force_flush(self):
        if self._lock is None: 
            self._lock = asyncio.Lock()
        async with self._lock:
            if self.buffer: 
                await self._flush_buffer()

    async def _flush_buffer(self):
        if not self.token or not self.chat_id:
            self.buffer.clear()
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {
                "chat_id": self.chat_id, 
                "text": "\n\n=====================\n\n".join(self.buffer), 
                "parse_mode": "HTML"
            }
            async with self.session.post(url, json=payload, timeout=10) as response: 
                await response.read()
            self.buffer.clear()
            await asyncio.sleep(2.0)
        except Exception: 
            pass

# =========================================================================
# BROKER CORE 1: ASYNCHRONICZNY KLIENT CRYPTO.COM (REST API)
# =========================================================================
class CryptoComExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter):
        self.base_url = "https://api.crypto.com/v2"
        self.session = session
        self.rate_limiter = rate_limiter

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.consume()
        try:
            async with self.session.get(f"{self.base_url}/public/get-ticker?instrument_name={symbol}", timeout=5) as response:
                if response.status != 200: return None
                data_list = (await response.json()).get("result", {}).get("data", [])
                if data_list:
                    ticker = data_list[0]
                    return {
                        "source": "CRYPTO_COM", 
                        "symbol": symbol,
                        "bid": float(ticker.get("b", 0)), 
                        "ask": float(ticker.get("k", 0)), 
                        "last": float(ticker.get("a", 0)),
                        "timestamp": int(time.time() * 1000)
                    }
                return None
        except Exception as e:
            logger.error(f"❌ [CRYPTO.COM TICKER ERROR] Awaria pobierania ceny dla {symbol}: {e}")
            return None

# =========================================================================
# BROKER CORE 2: BEZPIECZNY KLIENT GIEŁDY XTB (PROTOKÓŁ xAPI WEBSOCKET)
# =========================================================================
class XtbXapiExchangeClient:
    """
    Asynchroniczny klient giełdy XTB oparty w 100% na bezpiecznej autoryzacji tokenowej.
    BRAK przetwarzania haseł i loginów użytkownika w kodzie bota.
    """
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter):
        self.session = session
        self.rate_limiter = rate_limiter
        self.ws_url = "wss://ws.xtb.com/demo"
        # Pobieranie sprofilowanych, bezpiecznych kluczy bez uprawnień do wypłat
        self.app_key = os.environ.get("XTB_APP_KEY", "")
        self.app_token = os.environ.get("XTB_APP_TOKEN", "")

    async def get_asset_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Loguje się przy użyciu bezpiecznego klucza i tokenu aplikacji, pobiera cenę i zamyka gniazdo."""
        if not self.app_key or not self.app_token:
            logger.error("❌ [XTB AUTH SECURITY BREACH] Brak wymaganych kluczy XTB_APP_KEY / XTB_APP_TOKEN w środowisku!")
            return None

        await self.rate_limiter.consume()
        try:
            async with self.session.ws_connect(self.ws_url, timeout=10) as ws:
                # Krok 1: Tokenowa komenda logowania (Zgodna ze standardem xAPI)
                login_cmd = {
                    "command": "loginWithToken",
                    "arguments": {
                        "appKey": self.app_key,
                        "token": self.app_token
                    }
                }
                await ws.send_str(json.dumps(login_cmd))
                
                login_resp = await ws.receive_json(timeout=5)
                if not login_resp.get("status"):
                    logger.error("❌ [XTB TOKEN AUTH FAILED] Giełda odrzuciła token autoryzacyjny aplikacji.")
                    return None
                
                # Krok 2: Odpytanie o stan kwotowania instrumentu
                price_cmd = {"command": "getSymbol", "arguments": {"symbol": symbol}}
                await ws.send_str(json.dumps(price_cmd))
                
                price_resp = await ws.receive_json(timeout=5)
                if price_resp.get("status") and "returnArgument" in price_resp:
                    symbol_info = price_resp["returnArgument"]
                    return {
                        "source": "XTB", 
                        "symbol": symbol,
                        "bid": float(symbol_info.get("bid", 0)), 
                        "ask": float(symbol_info.get("ask", 0)), 
                        "last": float(symbol_info.get("ask", 0)),
                        "timestamp": int(time.time() * 1000)
                    }
                return None
        except Exception as e:
            logger.error(f"❌ [XTB xAPI WEBSOCKET ERROR] Krytyczny błąd połączenia dla {symbol}: {e}")
            return None

# =========================================================================
# GŁÓWNY ASYNCHRONICZNY POTOK TRADINGOWY (PIPELINE)
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None: 
        PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked(): 
        return
    
    async with PIPELINE_LOCK:
        logger.info("🕵️ [HYBRID PIPELINE] Inicjalizacja asynchronicznego skanowania Crypto.com oraz XTB...")
        if RATE_LIMITER is None: 
            RATE_LIMITER = TokenBucketRateLimiter()
        
        async with aiohttp.ClientSession() as session:
            redis_trade = UpstashRedisTradingBridge(
                os.environ.get("UPSTASH_REDIS_REST_URL", ""), 
                os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""), 
                session
            )
            
            # 1. CYKL CRYPTO.COM REST API
            crypto_client = CryptoComExchangeClient(session, RATE_LIMITER)
            for symbol in ["BTC_USDT", "ETH_USDT"]:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set(): 
                    break
                ticker = await crypto_client.get_market_ticker(symbol)
                if ticker:
                    await redis_trade.push_historical_tick(symbol, ticker, max_elements=50)
                    await redis_trade.incr_metric("crypto_ticks")
                    logger.info(f"🪙 [CRYPTO RECORD] Zapisano {symbol} -> Ostatnia cena: {ticker['last']}")

            # 2. CYKL XTB xAPI WEBSOCKET
            xtb_client = XtbXapiExchangeClient(session, RATE_LIMITER)
            for symbol in ["EURUSD", "GOLD"]:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set(): 
                    break
                ticker = await xtb_client.get_asset_ticker(symbol)
                if ticker:
                    await redis_trade.push_historical_tick(symbol, ticker, max_elements=50)
                    await redis_trade.incr_metric("xtb_ticks")
                    logger.info(f"📈 [XTB RECORD] Zapisano {symbol} -> Cena Ask: {ticker['ask']}")
            
            # Wymuszenie czyszczenia nieużywanych obiektów w RAM dla maszyn Render < 512 MB
            gc.collect()

# =========================================================================
# MENEDŻER CRONA SYSTEMOWEGO (TAKTOWANIE CO 2 MINUTY)
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT
    logger.info("⚡ [TRADING ONLINE] Silnik giełdowy wszedł w tryb aktywnego monitorowania rynków.")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    
    while not ASYNC_SHUTDOWN_EVENT.is_set():
        await run_async_pipeline()
        
        # Bezpieczne próbkowanie flagi zamknięcia procesu (Graceful Shutdown Co 10s)
        for _ in range(12):
            if ASYNC_SHUTDOWN_EVENT.is_set(): 
                break
            await asyncio.sleep(10)

def background_scheduler_thread():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    BACKGROUND_LOOP = loop  
    try: 
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e: 
        logger.error(f"[CRITICAL THREAD FAILURE] Awaria watku tła tradingu: {e}")
    finally: 
        loop.close()

# =========================================================================
# ATOMOWY EKSPORT METRYK PIPELINE (0 MB RAM OVERHEAD)
# =========================================================================
@app.route('/export-analytics', methods=['GET'])
def export_analytics_safe_json():
    """Pobiera i atomowo usuwa wyłącznie dane z prefiksem TRADE_ANALYTICS za pomocą żądania pakietowego Pipeline."""
    logger.info("📊 [ENDPOINT] Żążądanie zrzutu metryk tradingu.")
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla tła tradingu jest niedostępna"}), 503
    try:
        r_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip('/')
        r_tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        headers = {"Authorization": f"Bearer {r_tok}", "Content-Type": "application/json"}
        
        keys_resp = requests.get(f"{r_url}/keys/TRADE_ANALYTICS:*", headers=headers, timeout=10)
        r_keys = keys_resp.json().get("result", [])
        if not r_keys: 
            return jsonify({"status": "success", "message": "Brak danych giełdowych do eksportu", "trading_data": []}), 200
            
        pipeline_payload = [["MGET"] + r_keys, ["DEL"] + r_keys]
        pipeline_result = requests.post(f"{r_url}/pipeline", json=pipeline_payload, headers=headers, timeout=15).json().get("result", [])
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

@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running(): 
        return jsonify({"status": "error", "message": "Potok tradingu nie jest gotowy."}), 500
    asyncio.run_coroutine_threadsafe(run_async_pipeline(), BACKGROUND_LOOP)
    return jsonify({"status": "success", "message": "Potok tradingowy wymuszony ręcznie."}), 200

if __name__ == "__main__":
    worker_thread = threading.Thread(target=background_scheduler_thread, daemon=True)
    worker_thread.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
