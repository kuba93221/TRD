import os
import asyncio
import time
import aiohttp
import msgpack
import hmac
import hashlib
import base64
from datetime import datetime, UTC

# =========================================================================
# IZOLOWANY TESTER INTEGRALNOŚCI: OKX + REDIS + TELEGRAM
# =========================================================================

async def run_diagnostics():
    print("\n🔍 [START DIAGNOSTYKI] Badanie spójności zmiennych i połączeń...")
    
    required_keys = [
        "OKX_API_KEY", "OKX_SECRET_KEY", "OKX_PASSPHRASE",
        "UPSTASH_REDIS_REST_URL", "UPSTASH_REDIS_REST_TOKEN",
        "TELEGRAM_BOT_TOKEN", "TELEGRAM_CHANNEL_ID"
    ]
    missing = [k for k in required_keys if not os.environ.get(k, "").strip()]
    if missing:
        print(f"❌ KRYTYCZNY BŁĄD: Brakujące zmienne w systemie: {missing}")
        return

    print("✅ Zmienne środowiskowe obecne w kontenerze.")

    async with aiohttp.ClientSession() as session:
        # 1. Test OKX REST API
        api_key = os.environ.get("OKX_API_KEY", "").strip()
        secret_key = os.environ.get("OKX_SECRET_KEY", "").strip()
        passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
        base_url = "https://www.okx.com"
        
        request_path = "/api/v5/account/balance?ccy=USDT"
        timestamp = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        message = f"{timestamp}GET{request_path}"
        mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
        signature = base64.b64encode(mac.digest()).decode('utf-8')

        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase,
            "x-simulated-trading": "1"
        }

        try:
            async with session.get(f"{base_url}{request_path}", headers=headers, timeout=6) as resp:
                okx_data = await resp.json()
                if okx_data.get("code") == "0":
                    print("✅ OKX SANDBOX: Połączenie autoryzowane pomyślnie. Dane konta odebrane.")
                else:
                    print(f"❌ OKX BŁĄD AUTORYZACJI: Code {okx_data.get('code')} -> {okx_data.get('msg')}")
        except Exception as e:
            print(f"❌ OKX BŁĄD POŁĄCZENIA: {e}")

        # 2. Test Upstash Redis
        r_url = os.environ.get("UPSTASH_REDIS_REST_URL", "").rstrip('/')
        r_tok = os.environ.get("UPSTASH_REDIS_REST_TOKEN", "")
        r_headers = {"Authorization": f"Bearer {r_tok}", "Content-Type": "application/json"}
        
        test_key = "TRADE_DIAGNOSTICS_PING"
        payload_bin = msgpack.packb({"status": "CONNECTED", "ts": time.time()}).hex()
        
        try:
            pipe = [
                ["SET", test_key, payload_bin],
                ["EXPIRE", test_key, "60"]
            ]
            async with session.post(f"{r_url}/pipeline", json=pipe, headers=r_headers, timeout=5) as resp:
                if resp.status == 200:
                    print("✅ UPSTASH REDIS: Klucz binarny z prefiksem TRADE_ zapisany poprawnie.")
                else:
                    print(f"❌ UPSTASH REDIS: Kod odpowiedzi {resp.status}")
        except Exception as e:
            print(f"❌ UPSTASH REDIS BŁĄD: {e}")

        # 3. Test Telegram Dispatcher
        tg_token = os.environ.get("TELEGRAM_BOT_TOKEN", "").strip()
        tg_chat = os.environ.get("TELEGRAM_CHANNEL_ID", "").strip()
        tg_url = f"https://api.telegram.org/bot{tg_token}/sendMessage"
        tg_msg = "🤖 <b>[SYSTEM-CHECK v10.0]</b>\nPoświadczenia OKX Sandbox, Redis oraz Telegram zweryfikowane pomyślnie."
        
        try:
            async with session.post(tg_url, json={"chat_id": tg_chat, "text": tg_msg, "parse_mode": "HTML"}, timeout=6) as resp:
                if resp.status == 200:
                    print("✅ TELEGRAM: Wiadomość testowa wysłana na kanał.")
                else:
                    print(f"⚠️ TELEGRAM: Status {resp.status}. Sprawdź uprawnienia bota.")
        except Exception as e:
            print(f"❌ TELEGRAM BŁĄD: {e}")

    print("\n🏁 [KONIEC DIAGNOSTYKI]")

if __name__ == "__main__":
    asyncio.run(run_diagnostics())
