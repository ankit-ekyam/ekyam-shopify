from fastapi import FastAPI, Request, BackgroundTasks, HTTPException, Depends, Query
from fastapi.responses import JSONResponse, RedirectResponse, Response
import json
import logging
import secrets
import re
import requests
from pydantic import BaseModel, Field

from app.config.settings import get_settings
from app.database import get_mongo_db, close_mongo_connection
from app.auth.shopify_oauth import ShopifyOAuth2
from app.utils.shopify_api import ShopifyAPI
from app.utils.kafka_utils import get_kafka_producer, push_to_kafka
from app.utils.shopify_pusher import ShopifyDataPusher

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Initialize settings
settings = get_settings()

# Pydantic Models for Validation
class MappingPayload(BaseModel):
    source: str = Field(..., description="The source system, e.g., 'shopify'")
    entity: str = Field(..., description="The entity name, e.g., 'orders'")
    primary_key: str = Field(..., description="The primary key field name in the standardized data")
    mapping: dict = Field(..., description="The mapping rules dictionary")

class PushPayload(BaseModel):
    target_shop: str = Field(..., description="Target Shopify store domain (e.g., 'other-shop.myshopify.com')")
    target_token: str = Field(..., description="Access token for the target store")
    entity: str = Field(..., description="Entity to push (singular), e.g., 'product', 'customer'")
    data: dict = Field(..., description="The data payload to push to the target store")

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
    if not re.match(r"^[a-zA-Z0-9-]+\.myshopify\.com$", shop):
        raise HTTPException(status_code=400, detail="Invalid shop domain format")

    # Generate a random state for CSRF protection
    state = secrets.token_urlsafe(32)

    #Generates a secure URL and redirects the user to Shopify to ask for permissions
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
    error: str = None,
):
    """
    OAuth2 callback endpoint.

    Shopify redirects here after user approves app installation.
    Exchanges the authorization code for an access token.
    """
    # Catch explicit Shopify errors (e.g., User declined permissions)
    if error:
        raise HTTPException(status_code=400, detail=f"OAuth Error: {error}")

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

    # Shopify sends the user back here with a 'code'. 
    # We trade that code for an 'access_token' (VIP pass)
    token_data = oauth2.exchange_code_for_token(shop, code)
    if not token_data:
        raise HTTPException(status_code=500, detail="Failed to exchange code for token")

    # Store access token in MongoDB # We store the VIP pass in MongoDB so we can use it later
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

    if not re.match(r"^[a-zA-Z0-9-]+\.myshopify\.com$", shop):
        raise HTTPException(status_code=400, detail="Invalid shop domain format")

    token = oauth2.get_access_token(shop)
    if not token:
        raise HTTPException(status_code=400, detail="App not installed for this shop. Please go to /install first.")

    api = ShopifyAPI(shop, token, settings.api_version)
    w1 = api.ensure_webhook("orders/create", f"{settings.app_url}/webhooks/shopify/orders")
    w2 = api.ensure_webhook("orders/updated", f"{settings.app_url}/webhooks/shopify/orders/updated")

    return JSONResponse({"status": "success", "webhooks_registered": [w1, w2]})


# ========================
# Background Tasks
# ========================
def process_entity_batch(records: list, shop: str, entity: str):
    """Process a batch of entity records and push them to Kafka."""
    for record in records:
        record["shop_domain"] = shop
        record_id = str(record.get("id", "unknown"))
        push_to_kafka(kafka_producer, f"shopify.raw.{entity}", record, key=record_id)

# ========================
# Sync Endpoints
# ========================
@app.get("/sync/{entity}")
async def sync_entity(
    entity: str,
    shop: str,
    background_tasks: BackgroundTasks,
    limit: int = Query(50, ge=1, le=250, description="Number of records to fetch per page"),
    page_info: str = None,
):
    """
    Pull old records from Shopify and push them to Kafka.
    Supports cursor-based pagination via the page_info parameter.
    """
    if not shop:
        raise HTTPException(status_code=400, detail="shop parameter is required")

    if not re.match(r"^[a-zA-Z0-9-]+\.myshopify\.com$", shop):
        raise HTTPException(status_code=400, detail="Invalid shop domain format")
        
    valid_entities = ["orders", "products", "customers"]
    if entity not in valid_entities:
        raise HTTPException(status_code=400, detail=f"Unsupported entity: {entity}. Must be one of {valid_entities}.")
        
    # Validate page_info cursor format if provided
    if page_info and not re.match(r"^[A-Za-z0-9+/=_-]+$", page_info):
        raise HTTPException(status_code=400, detail="Invalid cursor format for page_info")
    
    # Get the store's VIP pass
    store_data = oauth2.get_store_data(shop)
    if not store_data:
        raise HTTPException(status_code=400, detail="App not installed for this shop or store is inactive. Please go to /install first.")

    token = store_data.get("access_token")
    granted_scopes = store_data.get("scopes", [])

    # --- New Validation Block ---
    # Validate that the store has granted the necessary permissions (scopes) to fetch this entity.
    required_scopes_for_entity = {
        "orders": "read_orders",
        "products": "read_products",
        "customers": "read_customers",
    }
    required_scope = required_scopes_for_entity.get(entity)
    if required_scope and required_scope not in granted_scopes:
        raise HTTPException(
            status_code=403, 
            detail=f"Insufficient permissions. The app requires the '{required_scope}' scope to sync {entity}. Please reinstall the app to grant permissions."
        )

    api = ShopifyAPI(shop, token, settings.api_version)
    try:
        fetch_method = getattr(api, f"list_{entity}_graphql", None)
        if not fetch_method:
            raise HTTPException(status_code=501, detail=f"Syncing for '{entity}' is not implemented in ShopifyAPI yet.")
         
             
        records, next_page_info = fetch_method(limit=limit, page_info=page_info)
    except requests.exceptions.RequestException as e:
        error_msg = str(e)
        if hasattr(e, "response") and e.response is not None:
            # Extract the exact JSON or HTML response from Shopify
            error_msg = e.response.text
        raise HTTPException(status_code=502, detail=f"Shopify API Error: {error_msg}")

    if records:
        background_tasks.add_task(process_entity_batch, records, shop, entity)

    return JSONResponse(
        {
            "status": "success",
            "message": f"Queued {len(records)} {entity} for processing.",
            "next_page_info": next_page_info,
            "has_next_page": bool(next_page_info)
        }
    )


# ========================
# Webhook Endpoints
# ========================
async def verify_shopify_webhook(request: Request):
    """
    A FastAPI dependency that verifies the incoming webhook's HMAC signature
    and returns the parsed JSON payload.
    """
    request_body = await request.body()
    hmac_header = request.headers.get("X-Shopify-Hmac-SHA256")
    if not hmac_header:
        logger.warning("Webhook received without HMAC header")
        raise HTTPException(status_code=401, detail="Missing HMAC signature")

    if not oauth2.verify_webhook(request_body, hmac_header):
        logger.warning("Webhook HMAC verification failed")
        raise HTTPException(status_code=401, detail="Invalid HMAC signature")

    try:
        payload = json.loads(request_body.decode("utf-8"))
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON payload")
        
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="JSON payload must be a JSON object/dictionary")

    shop_domain = request.headers.get("X-Shopify-Shop-Domain")
    payload["shop_domain"] = shop_domain
    return payload


@app.post("/webhooks/shopify/orders")
async def receive_shopify_order(
    background_tasks: BackgroundTasks,
    payload: dict = Depends(verify_shopify_webhook),# Single dictionary, not an array
):
    """Shopify Orders Webhook Endpoint (for new orders)."""
    logger.info(f"Received order webhook for order {payload.get('id')}")
    
    # 1. We verify the message actually came from Shopify (security check)
    order_id = str(payload.get("id", "unknown"))
    
    def process_webhook():
        push_to_kafka(kafka_producer, "shopify.raw.orders", payload, key=order_id)
        # Removed flush() to prevent blocking FastAPI's worker threads under high load

    background_tasks.add_task(process_webhook)

    return JSONResponse({"status": "accepted"})


@app.post("/webhooks/shopify/orders/updated")
async def receive_shopify_order_updated(
    background_tasks: BackgroundTasks,
    payload: dict = Depends(verify_shopify_webhook),
):
    """Shopify Orders Updated Webhook Endpoint."""
    logger.info(f"Received order/updated webhook for order {payload.get('id')}")

    order_id = str(payload.get("id", "unknown"))
    
    def process_webhook():
        push_to_kafka(kafka_producer, "shopify.raw.orders.updated", payload, key=order_id)
        # Removed flush() to prevent blocking FastAPI's worker threads under high load

    background_tasks.add_task(process_webhook)

    return JSONResponse({"status": "accepted"})


# ========================
# Mappings Endpoints
# ========================
@app.get("/mappings")
async def get_all_mappings():
    """Retrieve all source mappings from MongoDB."""
    mappings_collection = db[settings.mongo_collection_mappings]
    mappings = list(mappings_collection.find({}, {"_id": 0}))
    return JSONResponse({"status": "success", "mappings": mappings})

@app.get("/mappings/{source}/{entity}")
async def get_mapping(source: str, entity: str):
    """Retrieve a specific mapping from MongoDB."""
    mappings_collection = db[settings.mongo_collection_mappings]
    mapping = mappings_collection.find_one({"source": source, "entity": entity}, {"_id": 0})
    if not mapping:
        raise HTTPException(status_code=404, detail="Mapping not found")
    return JSONResponse({"status": "success", "mapping": mapping})

@app.post("/mappings")
async def create_or_update_mapping(payload: MappingPayload):
    """
    Create or update a source mapping dynamically.
    Example Payload:
    {
        "source": "shopify",
        "entity": "orders",
        "primary_key": "ext_order_id",
        "mapping": { ... }
    }
    """
    source = payload.source
    entity = payload.entity
    
    mappings_collection = db[settings.mongo_collection_mappings]
    mappings_collection.update_one(
        {"source": source, "entity": entity},
        {"$set": payload.model_dump()},
        upsert=True
    )
    return JSONResponse({"status": "success", "message": f"Mapping for {source} -> {entity} saved successfully"})

# ========================
# Push Endpoints
# ========================
@app.post("/push-to-store")
async def push_data_to_store(payload: PushPayload):
    """
    Pushes data to a different Shopify account using the static ShopifyDataPusher class.
    """
    result = ShopifyDataPusher.push_entity(
        shop_domain=payload.target_shop,
        access_token=payload.target_token,
        api_version=settings.api_version,
        entity_name=payload.entity,
        entity_data=payload.data
    )

    if result is None:
        raise HTTPException(status_code=500, detail=f"Failed to push {payload.entity} to {payload.target_shop}")

    return JSONResponse({"status": "success", "data": result})
