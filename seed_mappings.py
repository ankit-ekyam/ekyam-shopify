import logging
from pymongo import MongoClient
from app.config.settings import get_settings

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def seed_mappings():
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    mappings_collection = db["mappings"]

    # 1. Shopify Source Mapping (Shopify -> Ekyam)
    shopify_source = {
        "direction": "source",
        "source_system": "shopify",
        "entity": "orders",
        "mapping": {
            "fields": {
                "ext_order_id": "id",
                "event_timestamp": "created_at",
                "order_total_amt": "total_price",
                "currency": "currency",
                "customer_email": "email",
                "customer_id": "customer.id"
            },
            "lists": {
                "order_lines": {
                    "path": "line_items",
                    "fields": {
                        "ext_product_id": "product_id",
                        "sku": "sku",
                        "quantity": "quantity",
                        "unit_price": {
                            "transformer": "divide",
                            "paths": ["price", "quantity"]
                        }
                    }
                }
            }
        }
    }

    # 2. Shopify Destination Mapping (Ekyam -> Shopify)
    shopify_destination = {
        "direction": "destination",
        "target_system": "shopify",
        "entity": "orders",
        "mapping": {
            "fields": {
                "created_at": "event_timestamp",
                "total_price": "order_total_amt",
                "currency": "currency",
                "email": "customer_email",
                "customer_id": "customer_id"
            },
            "lists": {
                "line_items": {
                    "path": "order_lines",
                    "fields": {
                        "product_id": "ext_product_id",
                        "sku": "sku",
                        "quantity": "quantity",
                        "price": "unit_price"
                    }
                }
            }
        }
    }

    # Insert the rules into the DB
    mappings_collection.update_one(
        {"direction": "source", "source_system": "shopify", "entity": "orders"}, 
        {"$set": shopify_source}, 
        upsert=True
    )
    mappings_collection.update_one(
        {"direction": "destination", "target_system": "shopify", "entity": "orders"}, 
        {"$set": shopify_destination}, 
        upsert=True
    )

    logger.info("✅ Database seeded with dynamic mapping rules in 'mappings' collection.")
    client.close()

if __name__ == "__main__":
    seed_mappings()