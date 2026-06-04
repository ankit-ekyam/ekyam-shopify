import json
import logging
from datetime import datetime, UTC

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient

from app.config.settings import get_settings
from app.utils.shopify_api import ShopifyAPI

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

# Load settings
settings = get_settings()

# Kafka Consumer configuration
consumer_conf = {
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "group.id": settings.kafka_group_id,
    "auto.offset.reset": "earliest",
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["shopify.raw.orders", "shopify.raw.orders.updated"])

# MongoDB connection
client = MongoClient(settings.mongo_uri)
db = client[settings.mongo_db]
orders_collection = db[settings.mongo_collection_orders]
stores_collection = db[settings.mongo_collection_stores]

logger.info(
    f"MongoDB configured -> DB: '{settings.mongo_db}', Orders Collection: '{settings.mongo_collection_orders}'"
)

# Safely log the Mongo URI to verify which server we are connected to
safe_uri = settings.mongo_uri
if "@" in safe_uri:
    parts = safe_uri.split("@")
    safe_uri = f"mongodb://[hidden_credentials]@{parts[1]}"
logger.info(f"Python App is writing to MongoDB Server: {safe_uri}")

# Cache for access tokens: shop_domain -> {"token": str, "expires": float}
_token_cache = {}
# Cache for Shopify API clients: shop_domain -> {"api": ShopifyAPI, "expires": float}
_api_cache = {}


def transform_to_ekyam_standard(
    shopify_order: dict, enriched_data: dict = None
) -> dict:
    """
    Maps Shopify schema to Ekyam's Canonical Retail Model.

    Args:
        shopify_order: Raw Shopify order data
        enriched_data: Optional enriched data (e.g., customer details from API)

    Returns:
        Ekyam standardized order
    """
    ekyam_order = {
        "ext_order_id": str(shopify_order.get("id")),
        "shop_domain": shopify_order.get("shop_domain"),  # From webhook headers
        "event_timestamp": shopify_order.get("created_at"),
        "order_total_amt": float(shopify_order.get("total_price") or 0.0),
        "currency": shopify_order.get("currency"),
        "customer_email": shopify_order.get("email"),
        "customer_id": str((shopify_order.get("customer") or {}).get("id", "")),
        "order_lines": [],
        "processed_at": datetime.now(UTC).isoformat(),
        "enriched": enriched_data is not None,
    }

    for item in shopify_order.get("line_items", []):
        ekyam_order["order_lines"].append(
            {
                "ext_product_id": str(item.get("product_id")),
                "sku": item.get("sku"),
                "quantity": item.get("quantity"),
                "unit_price": float(item.get("price") or 0.0),
            }
        )

    # Add enriched data if available
    if enriched_data:
        ekyam_order["enriched_customer"] = enriched_data

    return ekyam_order


def get_access_token_for_shop(shop_domain: str) -> str | None:
    """
    Retrieve OAuth2 access token for a shop from MongoDB.

    Args:
        shop_domain: Shopify store domain

    Returns:
        Access token or None if not found
    """
    import time

    # Check local cache (valid for 5 minutes)
    cached = _token_cache.get(shop_domain)
    if cached and time.time() < cached["expires"]:
        return cached["token"]

    try:
        store = stores_collection.find_one(
            {"shop_domain": shop_domain, "is_active": True}
        )
        if store:
            logger.info(f"Retrieved access token for {shop_domain}")
            _token_cache[shop_domain] = {
                "token": store.get("access_token"),
                "expires": time.time() + 300,  # Cache TTL of 5 minutes
            }
            return store.get("access_token")
        logger.warning(f"No active access token found for {shop_domain}")
        return None
    except Exception as e:
        logger.error(f"Error retrieving access token: {e}")
        return None


def get_shopify_api(shop_domain: str) -> ShopifyAPI | None:
    """Get a cached ShopifyAPI client for a shop to reuse HTTP sessions."""
    import time

    cached = _api_cache.get(shop_domain)
    if cached and time.time() < cached["expires"]:
        return cached["api"]

    token = get_access_token_for_shop(shop_domain)
    if not token:
        return None

    api = ShopifyAPI(shop_domain, token, settings.api_version)
    _api_cache[shop_domain] = {"api": api, "expires": time.time() + 300}
    return api


def enrich_order_with_customer_data(shop_domain: str, order: dict) -> dict | None:
    """
    Optionally enrich order data by fetching customer details from Shopify API.

    Args:
        shop_domain: Shopify store domain
        order: Order data with customer_id

    Returns:
        Enriched customer data or None
    """
    try:
        customer_id = (order.get("customer") or {}).get("id")
        if not customer_id:
            return None

        # Call Shopify API to get customer details (uses cached HTTP session)
        api = get_shopify_api(shop_domain)
        if not api:
            logger.warning(f"No API client available for {shop_domain}")
            return None

        customer_data = api.get_customer(str(customer_id))

        if customer_data:
            logger.info(f"Enriched order with customer data for {customer_id}")
            return {
                "customer_id": customer_data.get("id"),
                "customer_name": customer_data.get("first_name")
                + " "
                + customer_data.get("last_name"),
                "customer_email": customer_data.get("email"),
                "customer_phone": customer_data.get("phone"),
                "customer_lifetime_value": float(
                    customer_data.get("total_spent") or 0.0
                ),
            }

        return None

    except Exception as e:
        logger.error(f"Error enriching order with customer data: {e}")
        return None


logger.info("Starting Ekyam Standardization Worker...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue

        if msg.error():
            if msg.error().code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            logger.error("Consumer error: %s", msg.error())
            continue

        try:
            raw_data = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.error("Invalid Kafka message payload: %s", exc)
            continue

        # Extract shop domain (you may need to store this in the Kafka message or as a header)
        shop_domain = raw_data.get("shop_domain", "")

        # Extract customer info directly from the payload instead of making a slow API call
        enriched_customer = None
        customer_data = raw_data.get("customer")
        if customer_data and customer_data.get("id"):
            first_name = customer_data.get("first_name", "") or ""
            last_name = customer_data.get("last_name", "") or ""
            name = f"{first_name} {last_name}".strip()
            
            enriched_customer = {
                "customer_id": str(customer_data.get("id")),
                "customer_name": name,
                "customer_email": customer_data.get("email"),
                "customer_phone": customer_data.get("phone"),
                "customer_lifetime_value": float(customer_data.get("total_spent") or 0.0),
            }

        # Transform to Ekyam standard
        standardized_data = transform_to_ekyam_standard(raw_data, enriched_customer)

        # Add a validation step to ensure we have an ID before saving.
        # If multiple messages lack an ID, they would overwrite each other in MongoDB.
        if not standardized_data.get("ext_order_id"):
            logger.error(
                "Skipping order with missing ID. Raw data: %s", raw_data
            )
            continue

        try:
            orders_collection.update_one(
                {"ext_order_id": standardized_data["ext_order_id"]},
                {"$set": standardized_data},
                upsert=True,
            )
            logger.info(
                f"Upserted order {standardized_data['ext_order_id']} in MongoDB collection '{settings.mongo_collection_orders}'"
            )
        except Exception as exc:
            logger.error("Failed to save standardized order to MongoDB: %s", exc)
            continue

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user")
finally:
    consumer.close()
    client.close()
    logger.info("Consumer closed")
