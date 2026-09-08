import os
import asyncio
import aiohttp
import hmac
import hashlib
import base64
from datetime import datetime, UTC

# =========================================================================
# SONDA DIAGNOSTYCZNA TRYBÓW AUTORYZACJI OKX V5
# =========================================================================

async def test_okx_handshake():
    api_key = os.environ.get("OKX_API_KEY", "").strip()
    secret_key = os.environ.get("OKX_SECRET_KEY", "").strip()
    passphrase = os.environ.get("OKX_PASSPHRASE", "").strip()
    base_url = "https://www.okx.com"
    request_path = "/api/v5/account/balance?ccy=USDT"

    print("\n🔍 [START BADANIA KLUCZA]")
    print(f"Klucz API: {api_key[:6]}...{api_key[-4:] if len(api_key) > 10 else ''}")

    for mode_name, is_demo in [("TRYB DEMO (x-simulated-trading: 1)", True), ("TRYB LIVE (brak flagi demo)", False)]:
        timestamp = datetime.now(UTC).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3] + 'Z'
        message = f"{timestamp}GET{request_path}"
        mac = hmac.new(secret_key.encode('utf-8'), message.encode('utf-8'), hashlib.sha256)
        signature = base64.b64encode(mac.digest()).decode('utf-8')

        headers = {
            "Content-Type": "application/json",
            "OK-ACCESS-KEY": api_key,
            "OK-ACCESS-SIGN": signature,
            "OK-ACCESS-TIMESTAMP": timestamp,
            "OK-ACCESS-PASSPHRASE": passphrase
        }
        if is_demo:
            headers["x-simulated-trading"] = "1"

        async with aiohttp.ClientSession() as session:
            try:
                async with session.get(f"{base_url}{request_path}", headers=headers, timeout=5) as resp:
                    res_json = await resp.json()
                    code = res_json.get("code")
                    msg = res_json.get("msg")
                    print(f"-> {mode_name}: HTTP {resp.status} | Kod OKX: {code} | Komunikat: {msg}")
                    if code == "0":
                        print(f"🎉 SUKCES! Klucz działa w: {mode_name}")
            except Exception as e:
                print(f"-> {mode_name}: Błąd sieci: {e}")

if __name__ == "__main__":
    asyncio.run(test_okx_handshake())
