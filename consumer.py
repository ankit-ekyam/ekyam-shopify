import json
import logging
import time
import re
from datetime import datetime, UTC

from confluent_kafka import Consumer, KafkaError
from pymongo import MongoClient, UpdateOne, ASCENDING, IndexModel

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
    "group.id": f"{settings.kafka_group_id}-dev",  # Suffix forces reading from the beginning
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False,  # Disable auto-commit to ensure exactly-once processing
}

consumer = Consumer(consumer_conf)
# Subscribe using a Regex pattern to match ANY source and ANY entity (e.g., shopify.raw.products)
consumer.subscribe(["^.*\\.raw\\..*$"])

# MongoDB connection
client = MongoClient(settings.mongo_uri)
db = client[settings.mongo_db]
stores_collection = db[settings.mongo_collection_stores]
mappings_collection = db[settings.mongo_collection_mappings]

logger.info(
    f"MongoDB configured -> DB: '{settings.mongo_db}', Orders Collection: '{settings.mongo_collection_orders}'"
)

# Safely log the Mongo URI to verify which server we are connected to
safe_uri = settings.mongo_uri
if "@" in safe_uri:
    parts = safe_uri.split("@")
    safe_uri = f"mongodb://[hidden_credentials]@{parts[1]}"
logger.info(f"Python App is writing to MongoDB Server: {safe_uri}")

INDEXED_COLLECTIONS = set()

def get_collection_for_entity(entity: str):
    """Dynamically get the MongoDB collection based on the entity type."""
    if entity == "orders":
        return db[settings.mongo_collection_orders]
    elif entity == "products":
        return db[settings.mongo_collection_products]
    return db[f"ekyam_{entity}"]

def ensure_collection_index(collection, pk_field: str):
    """Dynamically ensure the primary key index exists for performance."""
    col_name = collection.name
    if col_name not in INDEXED_COLLECTIONS:
        collection.create_index([(pk_field, ASCENDING)], unique=True, background=True)
        INDEXED_COLLECTIONS.add(col_name)
        logger.info("Ensured unique index on '%s' for collection '%s'", pk_field, col_name)

def get_nested_value(data: dict, path: str, default=None):
    """Helper to extract nested dictionary values using dot notation (e.g., 'customer.id')."""
    if not path:
        return default
    val = data
    for key in path.split('.'):
        if isinstance(val, dict):
            val = val.get(key)
        else:
            return default
    return val if val is not None else default


# In-memory cache to prevent querying MongoDB for every single message
SOURCE_MAPPINGS_CACHE = {}
CACHE_TTL_SECONDS = 10   # 10 seconds for development (change back to 300 for production)

def get_source_mapping(source: str, entity: str) -> dict:
    """Fetch entity-specific field mapping from cache or dynamically from MongoDB."""
    cache_key = f"{source}_{entity}"
    current_time = time.time()

    if cache_key in SOURCE_MAPPINGS_CACHE:
        cached_doc, timestamp = SOURCE_MAPPINGS_CACHE[cache_key]
        if current_time - timestamp < CACHE_TTL_SECONDS:
            return cached_doc
    # queries mongodb
    doc = mappings_collection.find_one({"source": source, "entity": entity})
    if doc and "mapping" in doc:
        SOURCE_MAPPINGS_CACHE[cache_key] = (doc, current_time)
        logger.info("Loaded mapping for %s %s from MongoDB", source, entity)
    else:
        SOURCE_MAPPINGS_CACHE[cache_key] = (None, current_time)
        logger.warning("No mapping found in MongoDB for %s %s", source, entity)
        
    return SOURCE_MAPPINGS_CACHE[cache_key][0]


def enrich_shopify_customer(raw_data: dict) -> dict | None:
    """Extracts and formats Shopify-specific customer enrichment data."""
    customer_data = raw_data.get("customer")
    if customer_data and customer_data.get("id"):
        first_name = customer_data.get("first_name", "") or ""
        last_name = customer_data.get("last_name", "") or ""
        return {
            "customer_id": str(customer_data.get("id")),
            "customer_name": f"{first_name} {last_name}".strip(),
            "customer_email": customer_data.get("email"),
            "customer_phone": customer_data.get("phone"),
            "customer_lifetime_value": float(customer_data.get("total_spent") or 0.0),
        }
    return None

# Registry for source-specific enrichment functions
ENRICHMENT_STRATEGIES = {
    ("shopify", "orders"): enrich_shopify_customer,
}

def apply_mapping_rule(data: dict, rule_config, field_name: str):
    """Extracts, validates, and casts a value based on the mapping rule."""
    # Backwards compatibility: If the rule is just a string, it's just the path
    if isinstance(rule_config, str):
        return get_nested_value(data, rule_config)
        
    path = rule_config.get("path", "")
    val = get_nested_value(data, path)
    
    # Apply default value if missing
    if val is None or val == "":
        if "default" in rule_config:
            val = rule_config["default"]
    
    # 1. Validation: Required Check
    if rule_config.get("required") and (val is None or val == ""):
        raise ValueError(f"Validation Error: Required field '{field_name}' (path: '{path}') is missing or empty.")
        
    # 2. Validation: Type Casting
    expected_type = rule_config.get("type")
    if val is not None and val != "" and expected_type:
        try:
            if expected_type == "int":
                val = int(float(val)) # Handles cases like "1.0" being cast to int
            elif expected_type == "float":
                val = float(val)
            elif expected_type == "str":
                val = str(val)
            elif expected_type == "bool":
                if not isinstance(val, bool):
                    val = str(val).lower() in ("true", "1", "t", "yes", "y")
        except (ValueError, TypeError):
            raise ValueError(f"Validation Error: Field '{field_name}' cannot be cast to '{expected_type}'. Value: {val}")
    
    # 3. Validation: Advanced Checks (Min/Max/Regex)
    if val is not None and val != "":
        if "min" in rule_config and val < rule_config["min"]:
            raise ValueError(f"Validation Error: '{field_name}' ({val}) is less than minimum {rule_config['min']}.")
        if "max" in rule_config and val > rule_config["max"]:
            raise ValueError(f"Validation Error: '{field_name}' ({val}) is greater than maximum {rule_config['max']}.")
        if expected_type == "str" and "pattern" in rule_config:
            if not re.match(rule_config["pattern"], str(val)):
                raise ValueError(f"Validation Error: '{field_name}' ({val}) does not match required pattern.")
        if "allowed_values" in rule_config and val not in rule_config["allowed_values"]:
            raise ValueError(f"Validation Error: '{field_name}' ({val}) is not in allowed values: {rule_config['allowed_values']}.")

    return val

def standardize_entity(source: str, entity: str, raw_data: dict, enriched_data: dict = None) -> tuple[dict, str]:
    """
    Completely generic router function. It builds the canonical model based entirely
    on the rules defined in the MongoDB mapping document.
    """
    doc = get_source_mapping(source, entity)
    if not doc:
        raise ValueError(f"No adapter configured for: '{source}' -> '{entity}'")
        
    mapping = doc.get("mapping", {})
    pk_field = doc.get("primary_key", f"ext_{entity[:-1]}_id") # e.g., fallback to ext_order_id

    ekyam_data = {
        "processed_at": datetime.now(UTC).isoformat(),
        "enriched": enriched_data is not None,
        "source_system": source,
        "entity_type": entity
    }

    # Map flat fields dynamically
    for ekyam_field, rule in mapping.get("fields", {}).items():
        ekyam_data[ekyam_field] = apply_mapping_rule(raw_data, rule, ekyam_field)

    # Map nested list structures (like order lines or product variants)
    for list_field, list_config in mapping.get("lists", {}).items():
        raw_list = get_nested_value(raw_data, list_config.get("path", ""), [])
        ekyam_data[list_field] = []
        for item in raw_list:
            mapped_item = {}
            for item_ekyam_field, rule in list_config.get("fields", {}).items():
                mapped_item[item_ekyam_field] = apply_mapping_rule(item, rule, item_ekyam_field)
            ekyam_data[list_field].append(mapped_item)

    if enriched_data:
        ekyam_data["enriched_data"] = enriched_data

    # Ensure primary key is a string 
    if pk_field in ekyam_data and ekyam_data[pk_field] is not None:
        ekyam_data[pk_field] = str(ekyam_data[pk_field])

    return ekyam_data, pk_field


logger.info("Starting Ekyam Standardization Worker...")

try:
    # Increased batch size for higher throughput during historical syncs
    BATCH_SIZE = 500
    while True:
        # Read up to BATCH_SIZE= 500 messages at once from Kafka
        msgs = consumer.consume(num_messages=BATCH_SIZE, timeout=1.0)
        if not msgs:
            continue

        # Group database operations by entity (e.g., {"orders": [...], "products": [...]})
        bulk_operations = {}

        for msg in msgs:
            if msg.error():
                if msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, getattr(KafkaError, "_PARTITION_EOF", None)):
                    continue
                logger.error("Consumer error: %s", msg.error())
                continue

            if msg.value() is None:
                logger.debug("Skipping message with no value (tombstone)")
                continue

            try:
                raw_data = json.loads(msg.value().decode("utf-8"))
            except (json.JSONDecodeError, UnicodeDecodeError) as exc:
                logger.error("Invalid Kafka message payload: %s", exc)
                continue

            # Determine the source and entity from the Kafka topic
            topic = msg.topic() or ""
            
            # Strict topic regex structure (e.g. 'shopify.raw.orders' or 'shopify.raw.orders.updated')
            if not re.match(r"^[a-zA-Z0-9_-]+\.raw\.[a-zA-Z0-9_-]+(?:\.updated)?$", topic):
                logger.warning("Unrecognized topic format: %s", topic)
                continue
                
            parts = topic.split(".")
            source, entity = parts[0], parts[2]

            enriched_customer = None
            
            # Apply specific enrichment strategy if one exists
            enrichment_func = ENRICHMENT_STRATEGIES.get((source, entity))
            if enrichment_func:
                enriched_customer = enrichment_func(raw_data)

            try:
                standardized_data, pk_field = standardize_entity(source, entity, raw_data, enriched_customer)

                if not standardized_data.get(pk_field):
                    logger.warning("Skipping %s with missing ID. Raw data: %s", entity, raw_data)
                    continue

                if entity not in bulk_operations:
                    bulk_operations[entity] = []

                # Queue the Upsert operation for this specific entity
                bulk_operations[entity].append(UpdateOne(
                    {pk_field: standardized_data[pk_field]},
                    {"$set": standardized_data},
                    upsert=True
                ))
            except Exception as exc:
                logger.error(
                    "Failed to process %s %s: %s. Raw Data: %s",
                    source, entity, exc, json.dumps(raw_data)
                )
                continue
                
        # Execute database updates natively for each entity involved in this batch
        for entity_name, operations in bulk_operations.items():
            collection = get_collection_for_entity(entity_name)
            # Pass the pk_field used in the first operation to guarantee index creation
            pk_field = list(operations[0]._filter.keys())[0]
            ensure_collection_index(collection, pk_field)
            
            try:
                result = collection.bulk_write(operations, ordered=False)
                logger.info(f"Bulk processed {len(operations)} {entity_name}. Upserted: {result.upserted_count}")
            except Exception as exc:
                logger.error("Bulk write for %s failed: %s", entity_name, exc)
                # We skip breaking here to ensure other valid entities still try to process
                
        # Commit offsets ONLY after successful MongoDB write (or if batch was safely skipped)
        try:
            consumer.commit(asynchronous=False)
        except Exception as exc:
            logger.error("Failed to commit Kafka offsets: %s", exc)

except KeyboardInterrupt:
    logger.info("Consumer interrupted by user")
finally:
    consumer.close()
    client.close()
    logger.info("Consumer closed")
