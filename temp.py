from confluent_kafka import Consumer, Producer
import json

# Kafka Configurations
consumer_conf = {
    "bootstrap.servers": "localhost:9092",
    "group.id": "ekyam-standardization-group",
    "auto.offset.reset": "earliest",
}
producer_conf = {"bootstrap.servers": "localhost:9092"}

consumer = Consumer(consumer_conf)
# producer = Producer(producer_conf)

consumer.subscribe(["shopify.raw.orders"])


def transform_to_ekyam_standard(shopify_order: dict) -> dict:
    """Maps Shopify schema to Ekyam's Canonical Retail Model."""
    ekyam_order = {
        "ext_order_id": str(shopify_order.get("id")),
        "event_timestamp": shopify_order.get("created_at"),
        "order_total_amt": float(shopify_order.get("total_price", 0.0)),
        "currency": shopify_order.get("currency"),
        "customer_email": shopify_order.get("email"),
        "order_lines": [],
    }

    # Map nested line items
    for item in shopify_order.get("line_items", []):
        ekyam_order["order_lines"].append(
            {
                "ext_product_id": str(item.get("product_id")),
                "sku": item.get("sku"),
                "quantity": item.get("quantity"),
                "unit_price": float(item.get("price", 0.0)),
            }
        )

    return ekyam_order


print("Starting Ekyam Standardization Worker...")

try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        if msg.value() is None:
            print("Skipping message with no value (tombstone)")
            continue

        # 1. Pull the raw data
        raw_data = json.loads(msg.value().decode("utf-8"))
        # print(f"Pulled raw order: {raw_data.get('id')}")
        print(f"Pulled raw order: {raw_data}")

        # 2. Standardize
        standardized_data = transform_to_ekyam_standard(raw_data)

        # 3. Push standardized data
        # producer.produce(
        #     "ekyam.standardized.orders",
        #     key=standardized_data["ext_order_id"].encode("utf-8"),
        #     value=json.dumps(standardized_data).encode("utf-8"),
        # )
        # producer.poll(0)
        print(f"Standardized and pushed order: {standardized_data}\n")

except KeyboardInterrupt:
    pass
finally:
    consumer.close()
