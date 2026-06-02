import base64
import hashlib
import hmac
import json
import os
from pathlib import Path

import requests


def load_env(env_path: Path) -> dict:
    values = {}
    if not env_path.exists():
        return values
    with env_path.open("r", encoding="utf-8") as f:
        for line in f:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            if "=" not in stripped:
                continue
            key, value = stripped.split("=", 1)
            values[key.strip()] = value.strip()
    return values


def get_webhook_secret() -> str:
    secret = os.getenv("SHOPIFY_WEBHOOK_SECRET")
    if secret:
        return secret
    env_values = load_env(Path(__file__).parent / ".env")
    if "SHOPIFY_WEBHOOK_SECRET" in env_values:
        return env_values["SHOPIFY_WEBHOOK_SECRET"]
    raise RuntimeError("SHOPIFY_WEBHOOK_SECRET not found in environment or .env")


def make_payload() -> dict:
    return {
        "id": 99998,
        "email": "test@example.com",
        "created_at": "2026-05-18T10:00:00Z",
        "total_price": "150.00",
        "currency": "USD",
        "shop_domain": "ekyam-store.myshopify.com",
        "customer": {"id": 111112},
        "line_items": [
            {
                "product_id": 223,
                "sku": "TEST-SKU",
                "quantity": 1,
                "price": "150.00",
            }
        ],
    }


def compute_hmac(secret: str, body_bytes: bytes) -> str:
    digest = hmac.new(secret.encode("utf-8"), body_bytes, hashlib.sha256).digest()
    return base64.b64encode(digest).decode("utf-8")


def main() -> None:
    secret = get_webhook_secret()
    payload = make_payload()
    body = json.dumps(payload, separators=(",", ":")).encode("utf-8")
    signature = compute_hmac(secret, body)

    url = os.getenv("WEBHOOK_TEST_URL", "http://localhost:8000/webhooks/shopify/orders")
    headers = {
        "Content-Type": "application/json",
        "X-Shopify-Hmac-SHA256": signature,
    }

    print("Using webhook secret from .env or environment")
    print(f"POST URL: {url}")
    print(f"Signature: {signature}")
    print("Payload:")
    print(json.dumps(payload, indent=2))

    response = requests.post(url, headers=headers, data=body, timeout=10)
    print("\nResponse status:", response.status_code)
    print("Response body:")
    print(response.text)


if __name__ == "__main__":
    main()
