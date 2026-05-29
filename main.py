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
logger = logging.getLogger("Algorithmic_Trading_Engine_v6.5_PRO")

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
# RDZEŃ QUANT: Z-SCORE + FILTRY TRENDU, MOMENTUM, WOLUMENU I RYZYKA
# =========================================================================
class AlgorithmicQuantCore:
    """Aparat matematyczny wzbogacony o EMA200, RSI, ATR, BandWidth i Risk Sizing."""
    
    @staticmethod
    def _calculate_ema(prices: List[float], period: int = 15) -> float:
        """Szybkie wyliczenie EMA dla dostępnej podpróby."""
        if len(prices) < period: return prices[0]
        k = 2 / (period + 1)
        ema = prices[-1]
        for p in reversed(prices[:-1]):
            ema = p * k + ema * (1 - k)
        return ema

    @staticmethod
    def _calculate_rsi(prices: List[float], period: int = 14) -> float:
        """Klasyczny oscylator momentum RSI."""
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
    def calculate_z_score(ticks: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        prices = [float(t.get("last", 0)) for t in ticks if t.get("last")]
        n = len(prices)
        if n < 20: return None  

        # 1. Obliczenia bazowe Z-Score
        sma = sum(prices) / n
        variance = sum((x - sma) ** 2 for x in prices) / n
        std_dev = math.sqrt(variance)
        if std_dev == 0: std_dev = 1e-6
        current_price = prices[0]
        z_score = (current_price - sma) / std_dev

        # 2. FILTR TRENDU: Filtrowanie trendu (EMA zastępcze dla okna wektora)
        ema_trend = AlgorithmicQuantCore._calculate_ema(prices, period=15)
        trend_direction = "LONG_ONLY" if current_price >= ema_trend else "SHORT_ONLY"

        # 3. FILTR MOMENTUM: RSI
        rsi_val = AlgorithmicQuantCore._calculate_rsi(prices, period=14)

        # 4. ZMIENNOŚĆ: Bollinger BandWidth i ATR (szacowany z odchylenia/serii)
        bandwidth = (std_dev * 4) / sma if sma != 0 else 0.0
        atr_estimated = std_dev * 0.5  # Matematyczny ekwiwalent zmienności średniej

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
# NOWE MODUŁY POBIERANIA DANYCH RYNKOWYCH V3
# =========================================================================
class BinanceTestnetClient:
    """Pobiera publiczne ceny spot z oficjalnego środowiska testowego Binance."""
    def __init__(self, session: aiohttp.ClientSession, rate_limiter: TokenBucketRateLimiter):
        self.base_url = "https://testnet.binance.vision/api/v3"
        self.session = session
        self.rate_limiter = rate_limiter
        self.api_key = os.environ.get("BINANCE_TESTNET_API_KEY", "")
        self.secret_key = os.environ.get("BINANCE_TESTNET_SECRET_KEY", "")

    def _generate_signature(self, query_string: str) -> str:
        return hmac.new(self.secret_key.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

    async def get_account_balance(self) -> float:
        """Pobiera dostępne saldo portfela testowego USDT w celu wyliczenia wielkości pozycji (1% ryzyka)."""
        if not self.api_key or not self.secret_key: return 10000.0  # Wartość domyślna w razie awarii kluczy
        await self.rate_limiter.consume()
        timestamp = int(time.time() * 1000)
        query = f"timestamp={timestamp}"
        signature = self._generate_signature(query)
        url = f"{self.base_url}/account?{query}&signature={signature}"
        headers = {"X-MBX-APIKEY": self.api_key}
        try:
            async with self.session.get(url, headers=headers, timeout=5) as r:
                balances = (await r.json()).get("balances", [])
                for b in balances:
                    if b.get("asset") == "USDT": return float(b.get("free", 0))
                return 10000.0
        except Exception:
            return 10000.0

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

    async def execute_market_order(self, symbol: str, side: str, quantity: float) -> Optional[Dict[str, Any]]:
        """Wysyła zlecenie transakcyjne na giełdę."""
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
        except Exception:
            return None

# =========================================================================
# CENTRALNY ASYNCHRONICZNY POTOK WYKONAWCZY (PIPELINE V6.5 PRO)
# =========================================================================
async def run_async_pipeline():
    global RATE_LIMITER, PIPELINE_LOCK
    if PIPELINE_LOCK is None: 
        PIPELINE_LOCK = asyncio.Lock()
    if PIPELINE_LOCK.locked(): 
        return
    
    async with PIPELINE_LOCK:
        logger.info("🕵️ [POTOK V6.5] Pobieranie próbek z silnika Binance i analiza wielokryteriowa...")
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
            total_balance = await binance.get_account_balance()
            
            # ROZSZERZONY RADAR: BTC, ETH, SOL, BNB, LINK, XRP (Precyzyjnie dostosowane min_qty i round_digits)
            instruments = [
                {"client": binance, "symbol": "BTCUSDT", "label": "BTC_USDT", "min_qty": 0.00001, "round_digits": 5},
                {"client": binance, "symbol": "ETHUSDT", "label": "ETH_USDT", "min_qty": 0.0001, "round_digits": 4},
                {"client": binance, "symbol": "SOLUSDT", "label": "SOL_USDT", "min_qty": 0.01, "round_digits": 2},
                {"client": binance, "symbol": "BNBUSDT", "label": "BNB_USDT", "min_qty": 0.001, "round_digits": 3},
                {"client": binance, "symbol": "LINKUSDT", "label": "LINK_USDT", "min_qty": 0.01, "round_digits": 2},
                {"client": binance, "symbol": "XRPUSDT", "label": "XRP_USDT", "min_qty": 0.1, "round_digits": 1}
            ]
            
            for inst in instruments:
                if ASYNC_SHUTDOWN_EVENT and ASYNC_SHUTDOWN_EVENT.is_set(): 
                    break
                
                ticker = await inst["client"].get_market_ticker(inst["symbol"])
                
                if ticker:
                    await redis_trade.push_historical_tick(inst["label"], ticker, max_elements=50)
                    history = await redis_trade.get_historical_ticks(inst["label"], max_elements=50)
                    
                    # Wywołanie rozbudowanego rdzenia Quant
                    metrics = AlgorithmicQuantCore.calculate_z_score(history)
                    if metrics:
                        z = metrics["z_score"]
                        rsi = metrics["rsi"]
                        bandwidth = metrics["bandwidth"]
                        trend = metrics["trend"]
                        atr = metrics["atr"]
                        current_price = metrics["current"]
                        
                        logger.info(f"📊 [{inst['label']}] P: {current_price} | Z: {z} | RSI: {rsi} | Bw: {bandwidth} | T: {trend}")
                        await redis_trade.incr_metric(f"ticks_{inst['label']}")
                        
                        # 3. ZMIENNOŚĆ: Filtr Bollinger BandWidth (Blokada przed fałszywym wybiciem w ścisku)
                        if bandwidth < 0.001:
                            logger.info(f"⚠️ [{inst['label']}] Blokada strategii: Skrajnie niski BandWidth ({bandwidth}). Rynek w fazie ścisku.")
                            continue

                        # 4. MATEMATYKA PORTFELA (Position Sizing - Ryzyko 1% kapitału oparte na dynamicznym ATR)
                        risk_capital = total_balance * 0.01  # Dokładnie 1% konta
                        stop_loss_distance = atr * 2         # Odległość SL = 2 * ATR
                        
                        if stop_loss_distance > 0:
                            calculated_qty = risk_capital / stop_loss_distance
                            # Zaokrąglenie wielkości pozycji do dopuszczalnych kroków giełdowych określonej monety
                            calculated_qty = max(inst["min_qty"], round(calculated_qty, inst["round_digits"]))
                        else:
                            calculated_qty = inst["min_qty"]

                        # --- ARCHITEKTURA DECYZJI STRATEGICZNEJ NA PODSTAWIE EMY, RSI ORAZ Z-SCORE ---
                        if z <= -2.0 and trend == "LONG_ONLY" and rsi <= 35:
                            # 🟩 ZGODA NA KUPNO (Trend wzrostowy + Wyprzedanie RSI + Statystyczny dołek Z-Score)
                            order_res = await binance.execute_market_order(inst["symbol"], "BUY", calculated_qty)
                            if order_res and order_res.get("status") == "FILLED":
                                take_profit = current_price + (stop_loss_distance * 1.5) # R:R Ratio przynajmniej 1.5
                                await tg.push(
                                    f"🟩 <b>[TRADING SYSTEM V6.5: ORDER FILLED]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"🤖 Pozycja: <b>LONG (Kupno SPOT)</b>\n"
                                    f"📈 Instrument: <b>{inst['label']}</b>\n"
                                    f"💰 Cena wejścia: <b>{current_price} USDT</b>\n"
                                    f"📦 Wielkość pozycji: <b>{calculated_qty}</b> (Zaryzykowano 1% konta)\n"
                                    f"──────────────────────────────\n"
                                    f"📊 <b>PARAMETRY MATEMATYCZNE:</b>\n"
                                    f"  • Z-Score: <code>{z}</code> (Skrajne odchylenie)\n"
                                    f"  • RSI (14): <code>{rsi}</code> (Potwierdzone wyprzedanie)\n"
                                    f"  • Trend (EMA): <code>{trend}</code>\n"
                                    f"  • Zmienność (ATR): <code>{round(atr, 6)}</code>\n"
                                    f"──────────────────────────────\n"
                                    f"🛡️ <b>ZARZĄDZANIE RYZYKIEM (R:R 1:1.5):</b>\n"
                                    f"  • 🛑 <b>STOP LOSS:</b> <code>{round(current_price - stop_loss_distance, 4)} USDT</code>\n"
                                    f"  • 🎯 <b>TAKE PROFIT:</b> <code>{round(take_profit, 4)} USDT</code>\n"
                                    f"──────────────────────────────\n"
                                    f"<i>Wiadomość wygenerowana automatycznie przez silnik na Renderze.</i>"
                                )

                        elif z >= 2.0 and trend == "SHORT_ONLY" and rsi >= 65:
                            # 🟥 ZGODA NA SPRZEDAŻ (Trend spadkowy + Wykupienie RSI + Statystyczna górka Z-Score)
                            order_res = await binance.execute_market_order(inst["symbol"], "SELL", calculated_qty)
                            if order_res and order_res.get("status") == "FILLED":
                                take_profit = current_price - (stop_loss_distance * 1.5)
                                await tg.push(
                                    f"🟥 <b>[TRADING SYSTEM V6.5: ORDER FILLED]</b>\n"
                                    f"──────────────────────────────\n"
                                    f"🤖 Pozycja: <b>SHORT (Sprzedaż SPOT)</b>\n"
                                    f"📈 Instrument: <b>{inst['label']}</b>\n"
                                    f"💰 Cena wejścia: <b>{current_price} USDT</b>\n"
                                    f"📦 Wielkość pozycji: <b>{calculated_qty}</b> (Zaryzykowano 1% konta)\n"
                                    f"──────────────────────────────\n"
                                    f"📊 <b>PARAMETRY MATEMATYCZNE:</b>\n"
                                    f"  • Z-Score: <code>{z}</code> (Skrajne odchylenie)\n"
                                    f"  • RSI (14): <code>{rsi}</code> (Potwierdzone wykupienie)\n"
                                    f"  • Trend (EMA): <code>{trend}</code>\n"
                                    f"  • Zmienność (ATR): <code>{round(atr, 6)}</code>\n"
                                    f"──────────────────────────────\n"
                                    f"🛡️ <b>ZARZĄDZANIE RYZYKIEM (R:R 1:1.5):</b>\n"
                                    f"  • 🛑 <b>STOP LOSS:</b> <code>{round(current_price + stop_loss_distance, 4)} USDT</code>\n"
                                    f"  • 🎯 <b>TAKE PROFIT:</b> <code>{round(take_profit, 4)} USDT</code>\n"
                                    f"──────────────────────────────\n"
                                    f"<i>Wiadomość wygenerowana automatycznie przez silnik na Renderze.</i>"
                                )
            
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
