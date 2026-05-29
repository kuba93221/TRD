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
from datetime import datetime
from flask import Flask, jsonify
from typing import Dict, Any, List, Optional

# =========================================================================
# SYSTEMOWY MODUŁ OBSERVABILITY & GLOBAL CONTEXT
# =========================================================================
LOG_LEVEL_CONFIG = os.environ.get("LOG_LEVEL", "INFO").upper()  
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL_CONFIG, logging.INFO), 
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger("Algorithmic_Trading_Engine_v6.0_DEV")

logger.info("⚙️ [SYSTEM-INIT] Uruchamianie PEŁNEGO bota w bezpiecznej gałęzi DEV [Pancerny Rdzeń Binance Only]")

BACKGROUND_LOOP = None
PIPELINE_LOCK = None  
ASYNC_SHUTDOWN_EVENT = None 

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

    async def push_historical_tick(self, market_id: str, tick_data: Dict[str, Any], max_elements: int = 50) -> bool:
        """Wpycha najświeższą cenę do kolejki kołowej Redis (FIFO)."""
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
            logger.error(f"❌ [REDIS FIFO ERROR] Błąd kolejki dla {market_id}: {e}")
            return False

    async def get_historical_ticks(self, market_id: str, max_elements: int = 50) -> List[Dict[str, Any]]:
        """Pobiera zmagazynowaną serię czasową próbek cenowych dla wskaźnika Z-Score."""
        if not self.url: return []
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            url = f"{self.url}/lrange/{safe_key}/0/{max_elements - 1}"
            async with self.session.get(url, headers=self.headers, timeout=4) as response:
                if response.status != 200: return []
                hex_list = (await response.json()).get("result", [])
                return [msgpack.unpackb(bytes.fromhex(h), strict_map_key=False) for h in hex_list if h and h not in ["None", "NULL"]]
        except Exception as e:
            logger.error(f"❌ [REDIS LRANGE ERROR] {market_id}: {e}")
            return []

    async def incr_metric(self, field_name: str):
        if not self.url: return
        key = self._enforce_prefix(f"ANALYTICS:{field_name}:{datetime.utcnow().strftime('%Y-%m-%d')}")
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
        if not self.token or not self.chat_id: return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with self.session.post(url, json=payload, timeout=10) as response: 
                await response.read()
        except Exception: 
            pass

# =========================================================================
# RDZEŃ QUANT: ANALIZA STATYSTYCZNA Z-SCORE (MEAN REVERSION)
# =========================================================================
class AlgorithmicQuantCore:
    """Ultra-lekki aparat matematyczny. Wylicza standaryzowane odchylenie Z-Score."""
    @staticmethod
    def calculate_z_score(ticks: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
        prices = [float(t.get("last", 0)) for t in ticks if t.get("last")]
        n = len(prices)
        if n < 10: return None  # Minimalna wielkość próby statystycznej

        sma = sum(prices) / n
        variance = sum((x - sma) ** 2 for x in prices) / n
        std_dev = math.sqrt(variance)
        if std_dev == 0: std_dev = 1e-6
        
        current_price = prices[0]
        z_score = (current_price - sma) / std_dev
        return {"current": current_price, "sma": round(sma, 6), "z_score": round(z_score, 4)}

# =========================================================================
# NOWE MODUŁY POBIERANIA DANYCH RYNKOWYCH V3
# =========================================================================
class BinanceTestnetClient:
    """Pobiera publiczne ceny spot z oficjalnego środowiska testowego Binance."""
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter):
        self.base_url = "https://testnet.binance.vision/api/v3"
        self.session = session
        self.rate_limiter = rate_limiter

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.consume()
        try:
            url = f"{self.base_url}/ticker/price?symbol={symbol}"
            async with self.session.get(url, timeout=5) as response:
                if response.status != 200: return None
                data = await response.json()
                return {"source": "BINANCE_TESTNET", "symbol": symbol, "last": float(data.get("price", 0))}
        except Exception:
            return None

# =========================================================================
# CENTRALNY ASYNCHRONICZNY POTOK WYKONAWCZY (PIPELINE)
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None: 
        PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked(): 
        return
    
    async with PIPELINE_LOCK:
        logger.info("🕵️ [POTOK V4] Pobieranie próbek z silnika Binance Testnet...")
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
            
            binance = BinanceTestnetClient(session, RATE_LIMITER)
            
            # Mapowanie rynków oparte w 100% o stabilną infrastrukturę Binance (Krypto + Syntetyczne EUR i GOLD)
            instruments = [
                {"client": binance, "symbol": "BTCUSDT", "label": "BTC_USDT"},
                {"client": binance, "symbol": "ETHUSDT", "label": "ETH_USDT"},
                {"client": binance, "symbol": "EURUSDT", "label": "EUR_USD"},
                {"client": binance, "symbol": "PAXGUSDT", "label": "GOLD_XAU"}
            ]
            
            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set(): 
                    break
                
                ticker = await inst["client"].get_market_ticker(inst["symbol"])
                
                if ticker:
                    await redis_trade.push_historical_tick(inst["label"], ticker, max_elements=50)
                    history = await redis_trade.get_historical_ticks(inst["label"], max_elements=50)
                    
                    metrics = AlgorithmicQuantCore.calculate_z_score(history)
                    if metrics:
                        z = metrics["z_score"]
                        logger.info(f"📊 [{inst['label']}] Price: {metrics['current']} | Z-Score: {z}")
                        await redis_trade.incr_metric(f"ticks_{inst['label']}")
                        
                        if z <= -2.0:
                            await tg.push(f"🟩 <b>[BUY SIGNAL]</b>\nRynek: <b>{inst['label']}</b>\nZ-Score: <b>{z}</b>\nCena: <b>{metrics['current']}</b>")
                        elif z >= 2.0:
                            await tg.push(f"🟥 <b>[SELL SIGNAL]</b>\nRynek: <b>{inst['label']}</b>\nZ-Score: <b>{z}</b>\nCena: <b>{metrics['current']}</b>")
            
            gc.collect()

# =========================================================================
# ASYNCHRONICZNY CRON I WĄTEK SPOCZYNKOWY
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT
    logger.info("⚡ [TRADING ONLINE] Silnik matematyczny gotowy na wyzwalanie zewnętrzne przez endpoint.")
    ASYNC_SHUTDOWN_EVENT = asyncio.Event()
    while not ASYNC_SHUTDOWN_EVENT.is_set():
        await asyncio.sleep(5)

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
# PUBLICZNE ENDPOINTY STERUJĄCE (DLA CRON-JOB.ORG)
# =========================================================================
@app.route('/run-analysis', methods=['GET', 'POST'])
def manual_analysis_trigger():
    if BACKGROUND_LOOP is None or not BACKGROUND_LOOP.is_running(): 
        return jsonify({"status": "error", "message": "Potok tradingu nie jest gotowy."}), 500
    asyncio.run_coroutine_threadsafe(run_async_pipeline(), BACKGROUND_LOOP)
    return jsonify({"status": "success", "message": "Analiza rynków uruchomiona pomyślnie."}), 200

@app.route('/export-analytics', methods=['GET'])
def export_analytics_safe_json():
    try:
        r_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip('/')
        r_tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        headers = {"Authorization": f"Bearer {r_tok}", "Content-Type": "application/json"}
        
        keys_resp = requests.get(f"{r_url}/keys/TRADE_ANALYTICS:*", headers=headers, timeout=10)
        r_keys = keys_resp.json().get("result", [])
        if not r_keys: 
            return jsonify({"status": "success", "trading_data": []}), 200
            
        pipeline_payload = [["MGET"] + r_keys, ["DEL"] + r_keys]
        pipeline_result = requests.post(f"{r_url}/pipeline", json=pipeline_payload, headers=headers, timeout=15).json().get("result", [])
        r_values = pipeline_result[0] if pipeline_result else []
        
        trading_output = []
        for index, key in enumerate(r_keys):
            if index >= len(r_values): break
            parts = key.split(":")
            trading_output.append({
                "data": parts[2] if len(parts) > 2 else "??",
                "metryka": parts[1],
                "wartosc": int(r_values[index] or 0)
            })
        return jsonify({"status": "success", "trading_count": len(trading_output), "trading_data": trading_output}), 200
    except Exception as e: 
        return jsonify({"status": "error", "message": str(e)}), 500

if __name__ == "__main__":
    worker_thread = threading.Thread(target=background_scheduler_thread, daemon=True)
    worker_thread.start()
    app.run(host="0.0.0.0", port=int(os.environ.get("PORT", 10000)), debug=False)
