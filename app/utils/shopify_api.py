"""Shopify API utilities."""

import requests
import logging
from typing import Optional

logger = logging.getLogger(__name__)


class ShopifyAPI:
    """Shopify REST API client."""

    def __init__(
        self, shop_domain: str, access_token: str, api_version: str = "2024-01"
    ):
        """
        Initialize Shopify API client.

        Args:
            shop_domain: Shopify store domain
            access_token: OAuth2 access token
            api_version: Shopify API version
        """
        self.shop_domain = shop_domain
        self.access_token = access_token
        self.api_version = api_version
        self.base_url = f"https://{shop_domain}/admin/api/{api_version}"
        self.headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        self.session = requests.Session()
        self.session.headers.update(self.headers)

    def get_customer(self, customer_id: str) -> Optional[dict]:
        """
        Fetch customer details from Shopify.

        Args:
            customer_id: Shopify customer ID

        Returns:
            Customer data or None if failed
        """
        try:
            url = f"{self.base_url}/customers/{customer_id}.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Fetched customer {customer_id}")
            return data.get("customer")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch customer: {e}")
            return None

    def get_order(self, order_id: str) -> Optional[dict]:
        """
        Fetch order details from Shopify.

        Args:
            order_id: Shopify order ID

        Returns:
            Order data or None if failed
        """
        try:
            url = f"{self.base_url}/orders/{order_id}.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            logger.info(f"Fetched order {order_id}")
            return data.get("order")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch order: {e}")
            return None

    def list_orders(self, limit: int = 50, status: str = "any") -> list[dict]:
        """
        Fetch recent orders from Shopify.

        Args:
            limit: Maximum number of orders to fetch
            status: Shopify order status filter

        Returns:
            List of orders, or an empty list if failed
        """
        try:
            url = f"{self.base_url}/orders.json"
            response = self.session.get(
                url,
                params={"limit": limit, "status": status},
                timeout=10,
            )
            response.raise_for_status()

            data = response.json()
            logger.info(f"Fetched {len(data.get('orders', []))} orders")
            return data.get("orders", [])

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch orders: {e}")
            return []

    def list_webhooks(self) -> list[dict]:
        """Fetch existing webhooks for the shop."""
        try:
            url = f"{self.base_url}/webhooks.json"
            response = self.session.get(url, timeout=10)
            response.raise_for_status()

            data = response.json()
            return data.get("webhooks", [])

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch webhooks: {e}")
            return []

    def create_webhook(self, topic: str, address: str) -> Optional[dict]:
        """
        Create a webhook for a specific topic.

        Args:
            topic: Webhook topic (e.g., 'orders/create', 'orders/updated')
            address: Webhook endpoint URL

        Returns:
            Webhook data or None if failed
        """
        try:
            url = f"{self.base_url}/webhooks.json"
            payload = {
                "webhook": {
                    "topic": topic,
                    "address": address,
                    "format": "json",
                }
            }
            response = self.session.post(url, json=payload, timeout=10)
            if not response.ok:
                logger.error(
                    "Failed to create webhook for %s at %s: %s %s",
                    topic,
                    address,
                    response.status_code,
                    response.text,
                )
                response.raise_for_status()

            data = response.json()
            logger.info(f"Created webhook for {topic}")
            return data.get("webhook")

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to create webhook: {e}")
            return None

    def delete_webhook(self, webhook_id: int) -> bool:
        """Delete an existing webhook."""
        try:
            url = f"{self.base_url}/webhooks/{webhook_id}.json"
            response = self.session.delete(url, timeout=10)
            response.raise_for_status()
            logger.info(f"Deleted old webhook {webhook_id}")
            return True
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to delete webhook {webhook_id}: {e}")
            return False

    def ensure_webhook(self, topic: str, address: str) -> Optional[dict]:
        """
        Create a webhook only if the same topic/address is not already registered.
        If a webhook exists for the same topic but a different address, delete it first.
        """
        existing_webhook = None
        for webhook in self.list_webhooks():
            if webhook.get("topic") == topic:
                if webhook.get("address") == address:
                    logger.info(f"Webhook already exists for {topic} at {address}")
                    existing_webhook = webhook
                else:
                    logger.info(f"Found outdated webhook for {topic} ({webhook.get('address')}). Deleting...")
                    self.delete_webhook(webhook.get("id"))

        if existing_webhook:
            return existing_webhook
            
        return self.create_webhook(topic, address)
