import json
import logging
from datetime import datetime, UTC

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient, UpdateOne

from app.config.settings import get_settings

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

settings = get_settings()

# Kafka Consumer configuration
consumer_conf = {
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "group.id": f"{settings.kafka_group_id}-dev",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,
}

consumer = Consumer(consumer_conf)
# Explicitly subscribe only to the topics we know we are handling
consumer.subscribe(["shopify.raw.orders", "shopify.raw.orders.updated"])

# MongoDB connection
client = MongoClient(settings.mongo_uri)
db = client[settings.mongo_db]
orders_collection = db[settings.mongo_collection_orders]
mappings_collection = db["mappings"]

# Create primary key index for performance
orders_collection.create_index("ext_order_id", unique=True)

def get_nested_value(data: dict, path: str, default=None):
    """Helper to extract nested dictionary values using dot notation."""
    if not path: return default
    val = data
    for key in path.split('.'):
        if isinstance(val, dict):
            val = val.get(key)
        else:
            return default
    return val if val is not None else default

def map_dynamic_inbound(raw_data: dict, config: dict, source_system: str) -> dict:
    """Dynamically maps source data to Ekyam standard using MongoDB rules."""
    ekyam_order = {
        "processed_at": datetime.now(UTC).isoformat(),
        "source_system": source_system,
        "entity_type": config.get("entity", "orders"),
        "order_lines": []
    }
    #extracts the actual "mapping" dictionary from the MongoDB document
    mapping = config.get("mapping", {})
    
    
    #this loops over mappings collections of mongodb
    for ekyam_field, path in mapping.get("fields", {}).items():
    
        val = get_nested_value(raw_data, path)
        if val is not None:
            if ekyam_field == "order_total_amt": val = float(val)
            elif ekyam_field in ["ext_order_id", "customer_id"]: val = str(val)
        elif ekyam_field == "order_total_amt": val = 0.0
        
        ekyam_order[ekyam_field] = val
        
    for list_name, list_config in mapping.get("lists", {}).items():
        raw_list = get_nested_value(raw_data, list_config.get("path"), [])
        for item in raw_list:
            mapped_item = {}
            for item_ekyam_field, path in list_config.get("fields", {}).items():
                val = get_nested_value(item, path)
                if val is not None:
                    if item_ekyam_field == "unit_price": val = float(val)
                    elif item_ekyam_field == "quantity": val = int(val)
                    elif item_ekyam_field == "ext_product_id": val = str(val)
                elif item_ekyam_field == "ext_product_id": 
                    val = "custom_product"
                mapped_item[item_ekyam_field] = val
            ekyam_order[list_name].append(mapped_item)
            
    return ekyam_order

logger.info("Starting Ekyam Standardization Worker...")

# Simple in-memory cache for mapping rules to prevent DB bottlenecks
mapping_cache = {}

try:
    BATCH_SIZE = 500
    while True:
        msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
        if not msgs:
            continue

        operations = []

        for msg in msgs:
            if msg.error():
                continue

            if msg.value() is None:
                continue

            try:
                raw_data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                continue
                
            topic = msg.topic() or ""
            if "orders" not in topic:
                continue
            
            # Extract the source system from the Kafka topic (e.g. 'shopify' from 'shopify.raw.orders')
            # If it cannot be found, it defaults to shopify
            source_system = topic.split(".")[0] if "." in topic else "shopify"
            
            try:
                # 1. Fetch Mapping Config (Using Cache)
                cache_key = f"inbound_{source_system}_orders"
                
                if cache_key not in mapping_cache:
                    # Only hit MongoDB if it's NOT in the cache
                    mapping_cache[cache_key] = mappings_collection.find_one({
                        "direction": "inbound", 
                        "source_system": source_system, 
                        "entity": "orders"
                    })
                
                config = mapping_cache[cache_key]
                
                if not config:
                    logger.warning(f"No mapping config found in MongoDB for source: {source_system}")
                    continue

                # 2. Standardize dynamically
                standardized_data = map_dynamic_inbound(raw_data, config, source_system)

                # 3. Add to Database Queue
                if standardized_data.get("ext_order_id"):
                    operations.append(UpdateOne(
                        {"ext_order_id": standardized_data["ext_order_id"]},
                        {"$set": standardized_data},
                        upsert=True
                    ))
            except Exception as exc:
                logger.error(
                    "Failed to process order: %s", exc
                )
                continue
                
        if operations:
            try:
                # 4. Save to MongoDB
                result = orders_collection.bulk_write(operations, ordered=False)
                logger.info(f"Bulk processed {len(operations)} orders. Upserted: {result.upserted_count}")
                
                # 5. Tell Kafka we are done (Commit Offset)
                consumer.commit(asynchronous=False)
            except Exception as exc:
                logger.error("MongoDB write failed: %s", exc)

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user")
finally:
    consumer.close()
    client.close()
    logger.info("Consumer closed")
