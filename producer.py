from fastapi import FastAPI, Request, BackgroundTasks, HTTPException
from fastapi.responses import JSONResponse, RedirectResponse, Response
import json
import logging
import secrets

from app.config.settings import get_settings
from app.database import get_mongo_db, close_mongo_connection
from app.auth.shopify_oauth import ShopifyOAuth2
from app.utils.shopify_api import ShopifyAPI
from app.utils.kafka_utils import get_kafka_producer, push_to_kafka

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Initialize settings
settings = get_settings()

# Initialize FastAPI app
app = FastAPI(
    title="ekyam-shopify OAuth2 Integration",
    description="Shopify app with OAuth2 authentication and order processing",
    version="0.1.0",
)

# Initialize Kafka producer
kafka_producer = get_kafka_producer(settings.kafka_bootstrap_servers)

# Initialize MongoDB
db = get_mongo_db(settings)

# Initialize OAuth2 handler
oauth2 = ShopifyOAuth2(settings, db)


@app.on_event("shutdown")
async def shutdown_event():
    """Clean up resources on shutdown."""
    close_mongo_connection()
    kafka_producer.flush()


# ========================
# Root Endpoint
# ========================
@app.get("/")
def read_root():
    """Root endpoint to prevent 404s on the base URL."""
    return {
        "message": "ekyam-shopify App is running! Visit /install?shop=YOUR_SHOP.myshopify.com to install."
    }


# ========================
# Health Check
# ========================
@app.get("/health")
async def health_check():
    """Health check endpoint."""
    return {"status": "ok", "service": "ekyam-shopify-producer"}


# ========================
# OAuth2 Endpoints
# ========================
@app.get("/install")
def install_app(shop: str = None):
    """
    Initiates the OAuth2 authorization flow.

    GET /install?shop=myshop.myshopify.com
    Redirects the user to Shopify's OAuth authorization page.
    """
    if not shop:
        raise HTTPException(status_code=400, detail="shop parameter is required")

    # Validate shop domain format
    if not shop.endswith(".myshopify.com") and not shop.endswith("myshopify.com"):
        shop = f"{shop}.myshopify.com"

    # Generate a random state for CSRF protection
    state = secrets.token_urlsafe(32)

    # Get authorization URL
    auth_url = oauth2.get_authorization_url(shop, state)
    logger.info(f"Redirecting to Shopify OAuth for {shop}")

    response = RedirectResponse(url=auth_url)
    # Store state and shop in secure, HTTP-only cookies (valid for 10 minutes)
    response.set_cookie(
        key="oauth_state", value=state, max_age=600, httponly=True, secure=True
    )
    response.set_cookie(
        key="oauth_shop", value=shop, max_age=600, httponly=True, secure=True
    )
    return response


@app.get("/oauth/callback")
def oauth_callback(
    request: Request,
    code: str = None,
    shop: str = None,
    state: str = None,
    hmac: str = None,
):
    """
    OAuth2 callback endpoint.

    Shopify redirects here after user approves app installation.
    Exchanges the authorization code for an access token.
    """
    # Validate state (CSRF protection)
    stored_state = request.cookies.get("oauth_state")
    stored_shop = request.cookies.get("oauth_shop")

    if not state or state != stored_state:
        raise HTTPException(status_code=401, detail="Invalid state parameter")

    if not shop:
        raise HTTPException(status_code=400, detail="Missing shop parameter")

    if shop != stored_shop:
        raise HTTPException(status_code=401, detail="Shop parameter mismatch")

    if not code:
        raise HTTPException(status_code=400, detail="Authorization code not provided")

    # Exchange authorization code for access token
    token_data = oauth2.exchange_code_for_token(shop, code)
    if not token_data:
        raise HTTPException(status_code=500, detail="Failed to exchange code for token")

    # Store access token in MongoDB
    success = oauth2.store_access_token(shop, token_data)
    if not success:
        raise HTTPException(status_code=500, detail="Failed to store access token")

    api = ShopifyAPI(shop, token_data["access_token"], settings.api_version)
    api.ensure_webhook("orders/create", f"{settings.app_url}/webhooks/shopify/orders")
    api.ensure_webhook(
        "orders/updated", f"{settings.app_url}/webhooks/shopify/orders/updated"
    )

    logger.info(f"Successfully installed app for {shop}")

    return JSONResponse(
        {
            "status": "success",
            "message": f"App installed successfully for {shop}",
            "shop": shop,
        }
    )


@app.get("/register-webhooks")
def register_webhooks(shop: str = None):
    """
    Helper endpoint to manually re-register webhooks.
    Useful when your ngrok URL changes.
    """
    if not shop:
        raise HTTPException(status_code=400, detail="Missing shop parameter")

    if not shop.endswith(".myshopify.com") and not shop.endswith("myshopify.com"):
        shop = f"{shop}.myshopify.com"

    token = oauth2.get_access_token(shop)
    if not token:
        raise HTTPException(status_code=400, detail="App not installed for this shop. Please go to /install first.")

    api = ShopifyAPI(shop, token, settings.api_version)
    w1 = api.ensure_webhook("orders/create", f"{settings.app_url}/webhooks/shopify/orders")
    w2 = api.ensure_webhook("orders/updated", f"{settings.app_url}/webhooks/shopify/orders/updated")

    return JSONResponse({"status": "success", "webhooks_registered": [w1, w2]})


# ========================
# Webhook Endpoints
# ========================
@app.post("/webhooks/shopify/orders")
async def receive_shopify_order(request: Request, background_tasks: BackgroundTasks):
    """
    Shopify Orders Webhook Endpoint.

    Receives order webhooks from Shopify.
    Verifies HMAC signature and pushes to Kafka for processing.
    """
    # Get raw request body for HMAC verification
    request_body = await request.body()

    # Get HMAC header
    hmac_header = request.headers.get("X-Shopify-Hmac-SHA256")
    if not hmac_header:
        logger.warning("Webhook received without HMAC header")
        raise HTTPException(status_code=401, detail="Missing HMAC signature")

    # Verify webhook signature
    if not oauth2.verify_webhook(request_body, hmac_header):
        logger.warning("Webhook HMAC verification failed")
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    # Parse payload
    try:
        payload = json.loads(request_body.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    # Get shop domain from headers (Shopify includes this)
    shop_domain = request.headers.get("X-Shopify-Shop-Domain")
    payload["shop_domain"] = shop_domain

    # Log webhook receipt
    logger.info(f"Received order webhook for order {payload.get('id')}")

    # Push to Kafka in background
    order_id = str(payload.get("id", "unknown"))
    background_tasks.add_task(
        push_to_kafka,
        kafka_producer,
        "shopify.raw.orders",
        payload,
        key=order_id,
    )

    return JSONResponse({"status": "accepted"})


@app.post("/webhooks/shopify/orders/updated")
async def receive_shopify_order_updated(
    request: Request, background_tasks: BackgroundTasks
):
    """Shopify Orders Updated Webhook Endpoint."""
    # Same verification and processing as orders created
    request_body = await request.body()
    hmac_header = request.headers.get("X-Shopify-Hmac-SHA256")

    if not hmac_header:
        raise HTTPException(status_code=401, detail="Missing HMAC signature")

    if not oauth2.verify_webhook(request_body, hmac_header):
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    try:
        payload = json.loads(request_body.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")

    shop_domain = request.headers.get("X-Shopify-Shop-Domain")
    payload["shop_domain"] = shop_domain

    order_id = str(payload.get("id", "unknown"))
    background_tasks.add_task(
        push_to_kafka,
        kafka_producer,
        "shopify.raw.orders.updated",
        payload,
        key=order_id,
    )

    return JSONResponse({"status": "accepted"})
