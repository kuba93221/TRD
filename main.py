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
import hmac
import hashlib
from urllib.parse import urlencode
from datetime import datetime, UTC
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
logger = logging.getLogger("Algorithmic_Trading_Engine_v9.0_PRODUCTION")

logger.info("⚙️ [SYSTEM-INIT] Uruchamianie PEŁNEGO silnika v9.0 [Binance OCO PURE SPOT]")

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
# POMOST UPSTASH REDIS (PANCERNE PARSOWANIE STRUKTURY UPSTASH PIPELINE)
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
        self._pipeline_cache = {}

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
        if not self.url: return False
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            hex_str = msgpack.packb(tick_data, use_bin_type=True).hex()
            
            pipeline_payload = [
                ["LPUSH", safe_key, hex_str],
                ["LTRIM", safe_key, "0", str(max_elements - 1)],
                ["LRANGE", safe_key, "0", str(max_elements - 1)]
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
            logger.error(f"❌ [REDIS PIPELINE ERROR] Błąd optymalizacji potoku dla {market_id}: {e}")
            return False

    async def get_historical_ticks(self, market_id: str, max_elements: int = 50) -> List[Dict[str, Any]]:
        cached_data = self._pipeline_cache.pop(market_id, None)
        if cached_data is not None:
            logger.debug(f"[REDIS-CACHE] Pobrano serię historyczną {market_id} z pamięci podręcznej (Oszczędność I/O)")
            return cached_data
            
        if not self.url: return []
        safe_key = self._enforce_prefix(f"HISTORY:{market_id}")
        try:
            url = f"{self.url}/lrange/{safe_key}/0/{max_elements - 1}"
            async with self.session.get(url, headers=self.headers, timeout=4) as response:
                if response.status != 200: return []
                res_json = await response.json()
                hex_list = res_json.get("result", []) if isinstance(res_json, dict) else []
                
                parsed_ticks = []
                for h in hex_list:
                    unpacked = self._safe_unpack_hex(h)
                    if unpacked:
                        parsed_ticks.append(unpacked)
                return parsed_ticks
        except Exception as e:
            logger.error(f"❌ [REDIS FALLBACK ERROR] {market_id}: {e}")
            return []

    async def incr_metric(self, field_name: str):
        if not self.url: return
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
        if not self.token or not self.chat_id: return
        try:
            url = f"https://api.telegram.org/bot{self.token}/sendMessage"
            payload = {"chat_id": self.chat_id, "text": text, "parse_mode": "HTML"}
            async with self.session.post(url, json=payload, timeout=10) as response: 
                await response.read()
        except Exception: 
            pass

# =========================================================================
# RDZEŃ QUANT: Z-SCORE + FILTRY TRENDU MACRO 1H, MOMENTUM I RYZYKA
# =========================================================================
class AlgorithmicQuantCore:
    """Aparat matematyczny kasowego powrotu do średniej opartego na SPOT."""
    
    @staticmethod
    def _calculate_ema(prices: List[float], period: int = 15) -> float:
        if len(prices) < period: return prices[0]
        k = 2 / (period + 1)
        ema = prices[-1]
        for p in reversed(prices[:-1]):
            ema = p * k + ema * (1 - k)
        return ema

    @staticmethod
    def _calculate_rsi(prices: List[float], period: int = 14) -> float:
        if len(prices) < period + 1: return 50.0
        gains = 0.0
        losses = 0.0
        for i in range(len(prices) - 1, len(prices) - 1 - period, -1):
            diff = prices[i-1] - prices[i]
            if diff > 0: gains += diff
            else: losses -= diff
        if losses == 0: return 100.0
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
        if std_dev == 0: std_dev = 1e-6
        current_price = prices[0]
        z_score = (current_price - sma) / std_dev

        # Wyliczanie filtrów EMA i RSI z prawdziwych świec makro (1H bezpośrednio z giełdy)
        use_prices = macro_prices if len(macro_prices) >= 15 else prices
        ema_trend = AlgorithmicQuantCore._calculate_ema(use_prices, period=15)
        trend_direction = "LONG_ONLY" if current_price >= ema_trend else "SHORT_ONLY"
        
        use_rsi_prices = macro_prices if len(macro_prices) >= 15 else prices
        rsi_val = AlgorithmicQuantCore._calculate_rsi(use_rsi_prices, period=14)
        
        bandwidth = (std_dev * 4) / sma if sma != 0 else 0.0
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
# SYSTEMOWY KLIENT BINANCE SPOT (PEŁNA STRUKTURA OCO I ŚWIEC MAKRO)
# =========================================================================
class BinanceSpotClient:
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter):
        self.base_url = os.environ.get("BINANCE_API_URL", "https://testnet.binance.vision/api/v3").rstrip('/')
        self.session = session
        self.rate_limiter = rate_limiter
        self.api_key = os.environ.get("BINANCE_TESTNET_API_KEY", "")
        self.secret_key = os.environ.get("BINANCE_TESTNET_SECRET_KEY", "")

    def _generate_signature(self, query_string: str) -> str:
        return hmac.new(self.secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def get_account_balance(self) -> float:
        if not self.api_key or not self.secret_key: 
            return 10000.0  
        await self.rate_limiter.consume()
        timestamp = int(time.time() * 1000)
        query = f"timestamp={timestamp}"
        signature = self._generate_signature(query)
        url = f"{self.base_url}/account?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            async with self.session.get(url, headers=headers, timeout=5) as r:
                data = await r.json()
                balances = data.get("balances", [])
                for b in balances:
                    if b.get("asset") == "USDT": 
                        return float(b.get("free", 0))
                return 10000.0
        except Exception as e:
            logger.error(f"[BINANCE-ERROR] Błąd pobierania salda: {e}")
            return 10000.0

    async def get_market_ticker(self, symbol: str) -> Optional[Dict[str, Any]]:
        await self.rate_limiter.consume()
        try:
            url = f"{self.base_url}/ticker/price?symbol={symbol}"
            async with self.session.get(url, timeout=5) as response:
                if response.status != 200: return None
                data = await response.json()
                return {"source": "BINANCE_SPOT", "symbol": symbol, "last": float(data.get("price", 0))}
        except Exception:
            return None

    async def get_macro_candles(self, symbol: str, interval: str = "1h", limit: int = 30) -> List[float]:
        """Pobiera historyczne świece z Binance, aby wyznaczyć prawdziwy trend makro."""
        await self.rate_limiter.consume()
        url = f"{self.base_url}/klines?symbol={symbol}&interval={interval}&limit={limit}"
        try:
            async with self.session.get(url, timeout=5) as response:
                if response.status != 200: return []
                data = await response.json()
                # Indeks 4 to cena zamknięcia (Close Price) w strukturze klines Binance
                return [float(candle[4]) for candle in data]
        except Exception as e:
            logger.error(f"[BINANCE-CANDLES-ERROR] Błąd pobierania świec makro dla {symbol}: {e}")
            return []

    async def execute_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict[str, Any]]:
        if not self.api_key or not self.secret_key: return None
        await self.rate_limiter.consume()
        timestamp = int(time.time() * 1000)
        params = {
            "symbol": symbol,
            "side": side.upper(),
            "type": "MARKET",
            "quantity": quantity,
            "timestamp": timestamp
        }
        query_string = urlencode(params)
        signature = self._generate_signature(query_string)
        url = f"{self.base_url}/order?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            async with self.session.post(url, headers=headers, timeout=5) as r:
                return await r.json()
        except Exception as e:
            logger.error(f"[TRANSACTION-ERROR] Krytyczny błąd zlecenia {side} dla {symbol}: {e}")
            return None

    async def execute_oco_protection(self, symbol: str, quantity: float, price_tp: float, price_sl: float) -> Optional[Dict[str, Any]]:
        """Wysyła zautomatyzowane, podwójne zlecenie obronne OCO na serwery Binance SPOT."""
        if not self.api_key or not self.secret_key: return None
        await self.rate_limiter.consume()
        timestamp = int(time.time() * 1000)
        
        params = {
            "symbol": symbol,
            "side": "SELL",
            "quantity": quantity,
            "price": price_tp,          # Poziom realizacji zysku (Limit Take Profit)
            "stopPrice": price_sl,      # Poziom wyzwolenia cięcia strat (Stop Trigger)
            "stopLimitPrice": price_sl, # Poziom egzekucji cięcia strat (Stop Limit)
            "timestamp": timestamp
        }
        
        query_string = urlencode(params)
        signature = self._generate_signature(query_string)
        url = f"{self.base_url}/order/oco?{query_string}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            async with self.session.post(url, headers=headers, timeout=5) as r:
                res = await r.json()
                logger.info(f"🛡️ [OCO-DEPLOYED] Automatyczna ochrona OCO wysłana na Binance dla {symbol}: {res}")
                return res
        except Exception as e:
            logger.error(f"❌ [OCO-CRITICAL-ERROR] Awaria wysyłania zlecenia obronnego OCO dla {symbol}: {e}")
            return None

# =========================================================================
# CENTRALNY ASYNCHRONICZNY POTOK WYKONAWCZY (BRAMKA v9.0 OCO PRO)
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None: 
        PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked(): 
        logger.debug("[POTOK-WARN] Poprzednia pętla analizy wciąż trwa. Blokuję nakładanie wątków.")
        return
    
    async with PIPELINE_LOCK:
        logger.info("🕵️ [POTOK V9.0] Pobieranie próbek z silnika Binance i analiza wielokryteriowa...")
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
            
            binance = BinanceSpotClient(session, RATE_LIMITER)
            total_balance = await binance.get_account_balance()
            
            instruments = [
                {"client": binance, "symbol": "BTCUSDT", "label": "BTC_USDT", "min_qty": 0.00001, "round_digits": 5, "price_round": 2},
                {"client": binance, "symbol": "ETHUSDT", "label": "ETH_USDT", "min_qty": 0.0001, "round_digits": 4, "price_round": 2},
                {"client": binance, "symbol": "SOLUSDT", "label": "SOL_USDT", "min_qty": 0.01, "round_digits": 2, "price_round": 2},
                {"client": binance, "symbol": "BNBUSDT", "label": "BNB_USDT", "min_qty": 0.001, "round_digits": 3, "price_round": 1},
                {"client": binance, "symbol": "LINKUSDT", "label": "LINK_USDT", "min_qty": 0.01, "round_digits": 2, "price_round": 3},
                {"client": binance, "symbol": "XRPUSDT", "label": "XRP_USDT", "min_qty": 0.1, "round_digits": 1, "price_round": 4}
            ]
            
            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set(): 
                    break
                
                ticker = await inst["client"].get_market_ticker(inst["symbol"])
                
                if ticker:
                    await redis_trade.push_historical_tick(inst["label"], ticker, max_elements=50)
                    history = await redis_trade.get_historical_ticks(inst["label"], max_elements=50)
                    
                    # Pobieranie świec 1-godzinnych bezpośrednio z Binance do wyznaczenia trendu makro
                    macro_candles = await inst["client"].get_macro_candles(inst["symbol"], interval="1h", limit=30)
                    
                    metrics = AlgorithmicQuantCore.calculate_z_score(history, macro_candles)
                    if metrics:
                        z = metrics["z_score"]
                        rsi = metrics["rsi"]
                        bandwidth = metrics["bandwidth"]
                        trend = metrics["trend"]
                        atr = metrics["atr"]
                        current_price = metrics["current"]
                        
                        logger.info(f"📊 [{inst['label']}] P: {current_price} | Z: {z} | RSI: {rsi} | Bw: {bandwidth} | T: {trend}")
                        await redis_trade.incr_metric(f"ticks_{inst['label']}")
                        
                        logger.debug(
                            f"[DECISION-TREE-{inst['label']}] Ocena filtrów SPOT: "
                            f"Z-Score Standard ok? {z <= -1.5} | Z-Score Crash ok? {z <= -2.5} (Wartość: {z}) | "
                            f"RSI 1H ok? {rsi <= 35} | RSI Crash ok? {rsi <= 20} (Wartość: {rsi}) | "
                            f"Trend Makro 1H ok? {trend == 'LONG_ONLY'} (Wartość: {trend})"
                        )
                        
                        if bandwidth < 0.001:
                            logger.info(f"⚠️ [{inst['label']}] Blokada strategii: Skrajnie niski BandWidth ({bandwidth}). Rynek w fazie ścisku.")
                            continue

                        # 2. ZARZĄDZANIE RYZYKIEM (Position Sizing - Ryzyko 1% kapitału konta)
                        risk_capital = total_balance * 0.01  
                        stop_loss_distance = atr * 2         
                        
                        if stop_loss_distance > 0:
                            calculated_qty = risk_capital / stop_loss_distance
                            calculated_qty = max(inst["min_qty"], round(calculated_qty, inst["round_digits"]))
                        else:
                            calculated_qty = inst["min_qty"]

                        # Zabezpieczenie przed progiem wartości minimalnej giełdy (< 11 USDT)
                        order_value_usdt = calculated_qty * current_price
                        if order_value_usdt < 11.0:
                            calculated_qty = max(calculated_qty, round(11.0 / current_price, inst["round_digits"]))
                            calculated_qty = max(inst["min_qty"], calculated_qty)

                        # --- DWUTOROWA BRAMKA DECYZYJNA ---
                        standard_buy = (z <= -1.5 and trend == "LONG_ONLY" and rsi <= 35)
                        crash_buy = (z <= -2.5 and rsi <= 20)
                        
                        if standard_buy or crash_buy:
                            logger.info(f"🚨 [EXECUTION-TRIGGER] Wyzwolenie zakupu SPOT dla {inst['label']}. (Standard: {standard_buy}, Crash: {crash_buy})")
                            order_res = await inst["client"].execute_market_order(inst["symbol"], "BUY", calculated_qty)
                            
                            if order_res and order_res.get("status") == "FILLED":
                                # Precyzyjne wyliczenie poziomów cenowych dla zleceń OCO
                                actual_qty = float(order_res.get("executedQty", calculated_qty))
                                price_tp = round(current_price + (stop_loss_distance * 1.5), inst["price_round"])
                                price_sl = round(current_price - stop_loss_distance, inst["price_round"])
                                
                                # Natychmiastowe wysłanie zautomatyzowanej ochrony OCO na serwery Binance
                                await asyncio.sleep(0.2) # Mały bufor dla rozliczenia silnika SPOT
                                await inst["client"].execute_oco_protection(inst["symbol"], actual_qty, price_tp, price_sl)
                                
                                await tg.push(
                                    f"🟩 <b>[TRADING SYSTEM v9.0: DEPLOYED WITH OCO]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"🤖 Strategia: <b>Mean Reversion (Pure SPOT v9.0)</b>\n"
                                    f"📈 Instrument: <b>{inst['label']}</b>\n"
                                    f"💰 Cena wejścia: <b>{current_price} USDT</b>\n"
                                    f"📦 Wielkość pozycji: <b>{actual_qty}</b> (Zaryzykowano 1% konta)\n"
                                    f"──────────────────────────────\n"
                                    f"🛡️ <b>AKTYWNA OCHRONA OCO NA BINANCE:</b>\n"
                                    f"  • 🎯 TAKE PROFIT: <code>{price_tp} USDT</code>\n"
                                    f"  • 🛑 STOP LOSS (Tnie 1% konta): <code>{price_sl} USDT</code>\n"
                                    f"──────────────────────────────"
                                )
            gc.collect()

# =========================================================================
# ASYNCHRONICZNY CRON I WĄTEK SPOCZYNKOWY
# =========================================================================
async def continuous_async_cron(loop):
    global ASYNC_SHUTDOWN_EVENT
    logger.info("⚡ [TRADING ONLINE] Silnik OCO gotowy na wyzwalanie zewnętrzne przez endpoint.")
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
