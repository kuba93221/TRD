import os
import asyncio
import math
import time
import logging
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
logger = logging.getLogger("Algorithmic_Trading_Engine_v5.0")

logger.info(f"⚙️ [SYSTEM-INIT] Uruchamianie niezależnego bota: Algorithmic Trading Engine v5.0")

BACKGROUND_LOOP = None
PIPELINE_LOCK = None  
ASYNC_SHUTDOWN_EVENT = None 

# Blokady rozproszonych stanów instrumentów (np. "BTC_USDT") przed wyścigiem transakcji
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
    logger.info("📥 [SIGTERM] Otrzymano sygnał zamknięcia od Render. Aktywuję Graceful Shutdown giełdy...")
    if BACKGROUND_LOOP and ASYNC_SHUTDOWN_EVENT:
        BACKGROUND_LOOP.call_soon_threadsafe(ASYNC_SHUTDOWN_EVENT.set)

signal.signal(signal.SIGTERM, sigterm_handler)

# =========================================================================
# NATYWNY SERWER FLASK (MONITORING & LEKKI EKSPORT JSON)
# =========================================================================
app = Flask(__name__)
logging.getLogger('werkzeug').setLevel(logging.WARNING)

@app.route('/', methods=['GET'])
def health_check():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return "TRADING_ENGINE_DOWN", 503
    return "OK", 200

CIRCUIT_BREAKER_UNTIL = 0.0
RATE_LIMITER = None

# =========================================================================
# REGULATOR PRZEPŁUWU SIECIOWEGO (TOKEN BUCKET RATE LIMITER)
# =========================================================================
class TokenBucketRateLimiter:
    def __init__(self, tokens_per_second: float = 4.0, max_capacity: float = 8.0):
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
            elapsed = now - self.last_check
            self.last_check = now
            self.tokens = min(self.capacity, self.tokens + elapsed * self.rate)
            
            if self.tokens < 1.0:
                wait_time = (1.0 - self.tokens) / self.rate
                logger.debug(f"⏳ [RATE LIMITER] Kubełek pusty. Wstrzymuję zapytanie giełdowe na {wait_time:.4f}s...")
                await asyncio.sleep(wait_time)
                self.tokens = 0.0
                self.last_check = time.monotonic()
            else:
                self.tokens -= 1.0

# =========================================================================
# DEDYROWANY POMOST UPSTASH REDIS (RYGOR PREFIKSU TRADE_)
# =========================================================================
class UpstashRedisTradingBridge:
    def __init__(self, url: str, token: str, session: aiohttp.ClientSession):
        self.url = url.rstrip('/') if url else ""
        self.headers = {"Authorization": f"Bearer {token}"} if token else {}
        self.session = session
        self.prefix = "TRADE_"  # Absolutna separacja od starego bota sportowego

    def _enforce_prefix(self, key: str) -> str:
        if not key.startswith(self.prefix):
            return f"{self.prefix}{key}"
        return key

    async def get_state(self, key: str) -> Optional[Dict[str, Any]]:
        if not self.url: return None
        safe_key = self._enforce_prefix(key)
        try:
            target_url = f"{self.url}/get/{safe_key}"
            async with self.session.get(target_url, headers=self.headers, timeout=4) as response:
                if response.status != 200: return None
                payload = await response.json()
                hex_res = payload.get("result")
         
                if hex_res and hex_res not in ["None", "NULL"]:
                    return msgpack.unpackb(bytes.fromhex(hex_res), strict_map_key=False)
                return None
        except Exception as e:
            logger.error(f"❌ [REDIS GET ERROR] Klucz {safe_key}: {e}")
            return None

    async def set_state(self, key: str, value: Dict[str, Any], ttl: Optional[int] = 86400) -> bool:
        if not self.url: return False
        safe_key = self._enforce_prefix(key)
        try:
            packed_bytes = msgpack.packb(value, use_bin_type=True)
            hex_str = packed_bytes.hex()
            suffix = f"?EX={ttl}" if ttl else ""
    
            target_url = f"{self.url}/set/{safe_key}/{hex_str}{suffix}"
            async with self.session.get(target_url, headers=self.headers, timeout=4) as response:
                if response.status == 200:
                    await response.read()
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS SET ERROR] Klucz {safe_key}: {e}")
            return False

    async def push_historical_tick(self, market_id: str, tick_data: Dict[str, Any], max_elements: int = 50) -> bool:
        """Zapisuje ceny rynkowe w pętli FIFO (LPUSH + LTRIM). Stały rozmiar bazy Upstash."""
        if not self.url: return False
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            packed_bytes = msgpack.packb(tick_data, use_bin_type=True)
            hex_str = packed_bytes.hex()
            
            # Dodaj na początek listy
            async with self.session.get(f"{self.url}/lpush/{safe_key}/{hex_str}", headers=self.headers, timeout=3) as resp:
                if resp.status != 200: return False
                await resp.read()
                
            # Przytnij do max_elements (usuń najstarsze wpisy)
            async with self.session.get(f"{self.url}/ltrim/{safe_key}/0/{max_elements - 1}", headers=self.headers, timeout=3) as resp:
                if resp.status == 200:
                    await resp.read()
                    return True
                return False
        except Exception as e:
            logger.error(f"❌ [REDIS FIFO ERROR] Rynek {market_id}: {e}")
            return False

    async def incr_metric(self, field_name: str):
        if not self.url: return
        date_str = datetime.utcnow().strftime("%Y-%m-%d")
        safe_key = self._enforce_prefix(f"ANALYTICS:{field_name}:{date_str}")
        try:
            target_url = f"{self.url}/incr/{safe_key}"
            async with self.session.get(target_url, headers=self.headers, timeout=3) as resp:
                await resp.read()
        except Exception as e:
            logger.error(f"❌ [REDIS INCR ERROR] Metryka {field_name}: {e}")

# =========================================================================
# MOSTEK POWIADOMIEŃ TELEGRAM (PACZKOWANIE PO 5)
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
        if self._lock is None: self._lock = asyncio.Lock()
        async with self._lock:
            self.buffer.append(text)
            if len(self.buffer) >= self.batch_size: await self._flush_buffer()

    async def force_flush(self):
        if self._lock is None: self._lock = asyncio.Lock()
        async with self._lock:
            if self.buffer: await self._flush_buffer()

    async def _flush_buffer(self):
        if not self.token or not self.chat_id:
            self.buffer.clear()
            return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": "\n\n=====================\n\n".join(self.buffer), "parse_mode": "HTML"}
            async with self.session.post(url, json=payload, timeout=10) as response:
                await response.read()
            self.buffer.clear()
            await asyncio.sleep(2.0)
        except Exception as e:
            logger.error(f"[TELEGRAM BATCH ERROR] Awaria wysyłki alertu: {e}")

# =========================================================================
# ASYNCHRONICZNY ORKIESTRATOR GIEŁDOWY (ZARZĄDZANIE CACHE STANÓW)
# =========================================================================
class TradingAssetOrchestrator:
    def __init__(self, redis: UpstashRedisTradingBridge, telegram: TelegramThrottledDispatcher):
        self.engine_version = "Algorithmic_Trading_v5.0_Core"
        self.redis = redis          
        self.telegram = telegram    
        self.state_cache: Dict[str, Any] = {}  

    async def update_asset_state(self, asset_id: str, new_state: Dict[str, Any], ttl: int = 86400) -> bool:
        self.state_cache[asset_id] = (new_state, time.time())
        return await self.redis.set_state(f"ASSET_STATE:{asset_id}", new_state, ttl=ttl)

    async def get_asset_state(self, asset_id: str) -> Optional[Dict[str, Any]]:
        current_time = time.time()
        if asset_id in self.state_cache:
            cache_data, cache_time = self.state_cache[asset_id]
            if current_time - cache_time < 300: return cache_data
        state = await self.redis.get_state(f"ASSET_STATE:{asset_id}")
        if state: self.state_cache[asset_id] = (state, current_time)
        return state

    def clear_expired_ram_cache(self):
        current_time = time.time()
        expired = [k for k, v in self.state_cache.items() if current_time - v[1] > 1800]
        for k in expired: del self.state_cache[k]

# =========================================================================
# BROKER CORE: ASYNCHRONICZNY KLIENT GIEŁDY CRYPTO.COM
# =========================================================================
class CryptoComExchangeClient:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter):
        self.base_url = "https://api.crypto.com/v2"
        self.session = session
        self.rate_limiter = rate_limiter
        self.api_key = os.environ.get("CRYPTO_COM_API_KEY", "")
        self.api_secret = os.environ.get("CRYPTO_COM_API_SECRET", "")

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.consume()
        try:
            async with self.session.get(f"{self.base_url}/public/get-ticker?instrument_name={symbol}", timeout=5) as response:
                if response.status != 200: return None
                data_list = (await response.json()).get("result", {}).get("data", [])
                if data_list:
                    ticker = data_list[0]
                    return {
                        "symbol": symbol,
                        "bid": float(ticker.get("b", 0)),
                        "ask": float(ticker.get("k", 0)),
                        "last": float(ticker.get("a", 0)),
                        "timestamp": int(time.time() * 1000)
                    }
                return None
        except Exception as e:
            logger.error(f"❌ [CRYPTO.COM TICKER ERROR] Awaria ceny dla {symbol}: {e}")
            return None

    async def execute_spot_buy_order(self, symbol: str, cash_amount: float) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.api_secret:
            logger.error("❌ [API EXECUTION BLOCKED] Brak kluczy API giełdy w os.environ!")
            return None
        await self.rate_limiter.consume()
        logger.warning(f"⚠️ [LIVE SPOT TRADE] Wysłano MARKET BUY SPOT: {symbol} kwota: {cash_amount} USDT.")
        return {"status": "DRY_RUN_SUCCESS", "symbol": symbol, "volume": cash_amount}

# =========================================================================
# GŁÓWNY ASYNCHRONICZNY POTOK POTOKU TRADINGOWEGO
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None: PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked(): return
    
    async with PIPELINE_LOCK:
        logger.info("🕵️ [TRADING PIPELINE] Rozpoczynam pobieranie cen rynkowych i analizę giełdową...")
        if RATE_LIMITER is None: RATE_LIMITER = TokenBucketRateLimiter()
        
        async with aiohttp.ClientSession() as session:
            redis_trade = UpstashRedisTradingBridge(os.environ.get("UPSTASH_REDIS_REST_URL", ""), os.environ.get("UPSTASH_REDIS_REST_TOKEN", ""), session)
            tg = TelegramThrottledDispatcher(os.environ.get("TELEGRAM_BOT_TOKEN", ""), os.environ.get("TELEGRAM_CHANNEL_ID", ""), session)
            orch_trade = TradingAssetOrchestrator(redis_trade, tg)
            crypto_client = CryptoComExchangeClient(session, RATE_LIMITER)
            
            # Cykl pobierania głównych par kryptowalutowych
            target_symbols = ["BTC_USDT", "ETH_USDT", "SOL_USDT"]
            for symbol in target_symbols:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set(): break
                ticker = await crypto_client.get_market_ticker(symbol)
                if ticker:
                    # Zapis FIFO do bazy pod obliczenia Mean Reversion
                    await redis_trade.push_historical_tick(symbol, ticker, max_elements=50)
                    await redis_trade.incr_metric("ticks_processed")
                    logger.info(f"📈 [TICK RECORD] Zapisano cenę dla {symbol}: {ticker['last']} USDT")
            
            orch_trade.clear_expired_ram_cache()
            await tg.force_flush()
            gc.collect()

# =========================================================================
# SYSTEMOWY MENEDŻER SCHEDULERA (TAKTOWANIE TRANSAKCJI)
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT
    logger.info("⚡ [TRADING ONLINE] Niezależna pętla tła bota tradingowego uruchomiona prawidłowo.")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    
    while not ASYNC_SHUTDOWN_EVENT.is_set():
        # Tradycyjne taktowanie testowe co 2 minuty (w kolejnych krokach przejdziemy na tryb WebSocket / Real-Time)
        await run_async_pipeline()
        
        # Bezpieczne sprawdzanie zakończenia procesu co 10 sekund wewnątrz pętli oczekiwania
        for _ in range(12):
            if ASYNC_SHUTDOWN_EVENT.is_set(): break
            await asyncio.sleep(10)

def background_scheduler_thread():
    global BACKGROUND_LOOP
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    BACKGROUND_LOOP = loop  
    try:
        loop.run_until_complete(continuous_async_cron(loop))
    except Exception as e:
        logger.error(f"[CRITICAL THREAD FAILURE] Awaria wątku tła tradingu: {e}")
    finally:
        loop.close()

# =========================================================================
# ENDPOINT AUTOMATYCZNEGO I ATOMOWEGO EKSPORTU METRYK GIEŁDOWYCH
# =========================================================================
@app.route('/export-analytics', methods=['GET'])
def export_analytics_safe_json():
    """Pobiera i automatycznie usuwa wyłącznie metryki giełdowe (z prefiksem TRADE_) jednym strzałem Pipeline."""
    logger.info("📊 [ENDPOINT] Żądanie zrzutu metryk tradingu algorytmicznego.")
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running():
        return jsonify({"status": "error", "message": "Pętla tła tradingu jest niedostępna"}), 503

    try:
        r_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip('/')
        r_tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        headers = {"Authorization": f"Bearer {r_tok}", "Content-Type": "application/json"}
        
        # 1. Pobieramy wyłącznie klucze giełdowe
        keys_resp = requests.get(f"{r_url}/keys/TRADE_ANALYTICS:*", headers=headers, timeout=10)
        r_keys = keys_resp.json().get("result", [])
        
        if not r_keys:
            return jsonify({"status": "success", "message": "Brak metryk handlowych do eksportu", "trading_data": []}), 200
            
        # 2. Transakcyjny Pipeline bazy Upstash (MGET + DEL w jednym pakiecie)
        pipeline_payload = [["MGET"] + r_keys, ["DEL"] + r_keys]
        pipeline_resp = requests.post(f"{r_url}/pipeline", json=pipeline_payload, headers=headers, timeout=15)
        pipeline_result = pipeline_resp.json().get("result", [])
        
        r_values = pipeline_result[0] if pipeline_result else []
        
        # 3. Parsowanie struktury bez użycia Pandas (0 MB RAM Overhead)
        trading_output = []
        for index, key in enumerate(r_keys):
            if index >= len(r_values): break
            parts = key.split(":")
            trading_output.append({
                "data": parts[2] if len(parts) > 2 else "??",
                "metryka": parts[1],
                "wartosc": int(r_values[index] or 0)
            })

        logger.info(f"✅ [EXPORT TRADING SUCCESS] Pobrano i wyczyszczono {len(trading_output)} metryk giełdowych.")
        return jsonify({
            "status": "success",
            "timestamp": time.time(),
            "trading_count": len(trading_output),
            "trading_data": trading_output
        }), 200

    except Exception as e:
        logger.error(f"❌ [EXPORT CRITICAL ERROR] Awaria potoku analityki handlowej: {e}")
        return jsonify({"status": "error", "message": "Błąd generowania struktur JSON tradingu"}), 500

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
