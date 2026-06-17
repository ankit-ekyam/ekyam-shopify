import logging
import sys
import time
from pymongo import MongoClient
from app.config.settings import get_settings
from app.utils.shopify_pusher import ShopifyDataPusher

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

def set_nested_value(data: dict, path: str, value):
    """Helper to set nested dictionary values using dot notation (e.g., 'customer.id')."""
    if not path:
        return
    parts = path.split('.')
    current = data
    for part in parts[:-1]:
        if part not in current or not isinstance(current[part], dict):
            current[part] = {}
        current = current[part]
    current[parts[-1]] = value

def reverse_map_entity(ekyam_data: dict, mapping_config: dict) -> dict:
    """
    Converts Ekyam standardized data back to the source system format (Reverse Mapping).
    """
    raw_data = {}
    
    # 1. Reverse map flat fields
    for ekyam_field, rule in mapping_config.get("fields", {}).items():
        if ekyam_field not in ekyam_data or ekyam_data[ekyam_field] is None:
            continue
            
        val = ekyam_data[ekyam_field]
        
        # Rule can be a string (e.g., 'customer.id') or a dict (e.g., {'path': 'id'})
        path = rule if isinstance(rule, str) else rule.get("path")
        
        if path:
            set_nested_value(raw_data, path, val)

    # 2. Reverse map nested lists (e.g., order lines)
    for list_field, list_config in mapping_config.get("lists", {}).items():
        if list_field not in ekyam_data or not ekyam_data[list_field]:
            continue
            
        ekyam_list = ekyam_data[list_field]
        target_path = list_config.get("path", "")
        
        raw_list = []
        for item in ekyam_list:
            mapped_item = {}
            for item_ekyam_field, rule in list_config.get("fields", {}).items():
                if item_ekyam_field not in item or item[item_ekyam_field] is None:
                    continue
                
                val = item[item_ekyam_field]
                item_path = rule if isinstance(rule, str) else rule.get("path")
                
                if item_path:
                    set_nested_value(mapped_item, item_path, val)
            
            raw_list.append(mapped_item)
            
        if target_path:
            set_nested_value(raw_data, target_path, raw_list)

    return raw_data

def main():
    logger.info("Starting Reverse Mapping & Push Test...")
    settings = get_settings()
    client = MongoClient(settings.mongo_uri)
    db = client[settings.mongo_db]
    
    # Collections
    orders_collection = db[settings.mongo_collection_orders]
    mappings_collection = db[settings.mongo_collection_mappings]
    stores_collection = db[settings.mongo_collection_stores]

    # 1. Fetch standardized orders from MongoDB ,  If no specific ID is passed, it uses .find() to grab the 5 most recent orders.
    orders_to_process = []
    if len(sys.argv) > 1:
        target_order_id = sys.argv[1]
        logger.info(f"Looking for specific order ID: {target_order_id}")
        order = orders_collection.find_one({"ext_order_id": target_order_id})
        if order:
            orders_to_process.append(order)
    else:
        logger.info("No order ID provided. Fetching a batch of 5 recent orders...")
        # Fetch latest 5 orders to push as a batch
        orders_to_process = list(orders_collection.find(sort=[("_id", -1)]).limit(5))
        
    if not orders_to_process:
        logger.error("No Ekyam orders found in MongoDB. Please sync or create an order first.")
        return
        
    # Fetch Target Store Info once
    target_store = stores_collection.find_one({"is_active": True})
    if not target_store:
        logger.error("No active stores found to push data to.")
        return
        
    logger.info(f"Found {len(orders_to_process)} orders to process. Target store: {target_store['shop_domain']}")

    for order in orders_to_process:
        source = order.get("source_system", "shopify")
        entity = order.get("entity_type", "orders")

        # 2. Fetch mapping rules dynamically per order
        mapping_doc = mappings_collection.find_one({"source": source, "entity": entity})
        if not mapping_doc or "mapping" not in mapping_doc:
            logger.error(f"No mapping configuration found for {source} -> {entity}. Skipping.")
            continue

        # 3. Apply Reverse Mapping
        logger.info(f"Applying reverse mapping for order {order.get('ext_order_id')}...")
        raw_shopify_payload = reverse_map_entity(order, mapping_doc["mapping"])
        
        # Clean up the ID fields before pushing
        raw_shopify_payload.pop("id", None)
        raw_shopify_payload.pop("shop_domain", None)
        
        # Shopify API Validation Fix
        if "line_items" in raw_shopify_payload:
            for item in raw_shopify_payload["line_items"]:
                if "title" not in item:
                    item["title"] = item.get("sku", "Custom Product")

        logger.info(f"Reconstructed Payload ready for Push.")

        # 4. Push to Store
        entity_singular = entity[:-1] if entity.endswith("s") else entity
        result = ShopifyDataPusher.push_entity(
            shop_domain=target_store["shop_domain"],
            access_token=target_store["access_token"],
            api_version=settings.api_version,
            entity_name=entity_singular,
            entity_data=raw_shopify_payload
        )

        if result:
            logger.info(f"✅ Successfully pushed to store! Shopify Response ID: {result.get(entity_singular, {}).get('id')}")
        else:
            logger.error(f"❌ Failed to push order {order.get('ext_order_id')}. Check logs.")
            
        # Sleep slightly to respect Shopify's 2 requests/second REST API rate limit
        time.sleep(1.0)

if __name__ == "__main__":
    main()