import json
import logging
import time

from confluent_kafka import Consumer, KafkaError
from app.config.settings import get_settings
from app.utils.shopify_pusher import ShopifyDataPusher


logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

settings = get_settings()

consumer_conf = {
    "bootstrap.servers": settings.kafka_bootstrap_servers,
    "group.id": f"{settings.kafka_group_id}-push-workers",
    "auto.offset.reset": "earliest",
    "enable.auto.commit": False, # Manual commit to ensure no data loss
}

consumer = Consumer(consumer_conf)
consumer.subscribe(["^shopify\\.push\\..*$"])

logger.info("Starting Outbound Push Consumer Worker...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            if msg.error().code() in (KafkaError.UNKNOWN_TOPIC_OR_PART, getattr(KafkaError, "_PARTITION_EOF", None)):
                continue
            logger.error("Consumer error: %s", msg.error())
            continue

        try:
            data = json.loads(msg.value().decode("utf-8"))
        except Exception as e:
            logger.error("Failed to decode message: %s", e)
            continue

        target_shop = data.get("target_shop")
        target_token = data.get("target_token")
        entity_name = data.get("entity_name")
        payload = data.get("payload")

        if not all([target_shop, target_token, entity_name, payload]):
            logger.warning("Skipping invalid push message structure: %s", data)
            continue

        logger.info("Pushing %s to %s...", entity_name, target_shop)

    
        result = ShopifyDataPusher.push_entity(
            shop_domain=target_shop,
            access_token=target_token,
            api_version=settings.api_version,
            entity_name=entity_name,
            entity_data=payload
        )

        if result:
            logger.info("✅ Successfully pushed %s!", entity_name)
            time.sleep(2.0)
            consumer.commit(asynchronous=False)
        else:
            logger.error("❌ Failed to push %s. The message will be retried after a delay.", entity_name)
            time.sleep(10.0)

except KeyboardInterrupt:
    logger.info("Push Consumer interrupted by user.")
finally:
    consumer.close()
    logger.info("Push Consumer closed.")