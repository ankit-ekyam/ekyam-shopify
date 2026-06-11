import logging
from pymongo import MongoClient
from app.config.settings import get_settings

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def seed_mappings():
    """Seeds initial source mappings into the MongoDB database."""
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    mappings_collection = db[settings.mongo_collection_mappings]

    # 1. Shopify Orders Mapping
    shopify_orders_mapping = {
        "source": "shopify",
        "entity": "orders",
        "primary_key": "ext_order_id",
        "mapping": {
            "fields": {
                "ext_order_id": {"path": "id", "required": True, "type": "str"},
                "shop_domain": {"path": "shop_domain", "required": True},
                "event_timestamp": "created_at",
                "order_total_amt": {"path": "total_price", "type": "float"},
                "currency": {"path": "currency", "default": "USD", "type": "str"},
                "customer_email": "email",
                "customer_id": "customer.id"
            },
            "lists": {
                "order_lines": {
                    "path": "line_items",
                    "fields": {
                        "ext_product_id": {"path": "product_id", "required": True},
                        "sku": "sku",
                        "quantity": {"path": "quantity", "type": "int"},
                        "unit_price": {"path": "price", "type": "float"}
                    }
                }
            }
        }
    }

    # 2. Custom Orders Mapping Example
    custom_orders_mapping = {
        "source": "custom",
        "entity": "orders",
        "primary_key": "ext_order_id",
        "mapping": {
            "fields": {
                "ext_order_id": {"path": "order_id", "required": True, "type": "str"},
                "shop_domain": {"path": "source_system", "default": "custom_app"},
                "event_timestamp": "timestamp",
                "order_total_amt": {"path": "total_amount", "type": "float"},
                "currency": {"path": "currency", "default": "USD", "type": "str"},
                "customer_email": "customer_email"
            },
            "lists": {
                "order_lines": {
                    "path": "items",
                    "fields": {
                        "ext_product_id": {"path": "id", "required": True},
                        "sku": "sku",
                        "quantity": {"path": "qty", "type": "int"},
                        "unit_price": {"path": "price", "type": "float"}
                    }
                }
            }
        }
    }

    # Upsert the mappings into MongoDB
    mappings_collection.update_one({"source": "shopify", "entity": "orders"}, {"$set": shopify_orders_mapping}, upsert=True)
    mappings_collection.update_one({"source": "custom", "entity": "orders"}, {"$set": custom_orders_mapping}, upsert=True)

    logger.info("✅ Database seeded with mapping rules successfully.")
    client.close()

if __name__ == "__main__":
    seed_mappings()