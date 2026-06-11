"""Shopify API utilities."""

import requests
import re
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
        if not re.match(r"^[a-zA-Z0-9-]+\.myshopify\.com$", shop_domain):
            raise ValueError(f"Invalid shop domain format: {shop_domain}")
        if not access_token:
            raise ValueError("Access token cannot be empty")
            
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

    def list_orders_paginated(
        self, limit: int = 50, status: str = "any", page_info: Optional[str] = None
    ) -> tuple[list[dict], Optional[str]]:
        """
        Fetch recent orders from Shopify with pagination support.

        Args:
            limit: Maximum number of orders to fetch
            status: Shopify order status filter (ignored if page_info is provided)
            page_info: Cursor for pagination

        Returns:
            Tuple of (List of orders, next_page_info string or None)
        """
        try:
            url = f"{self.base_url}/orders.json"
            params = {"limit": limit}
            if page_info:
                params["page_info"] = page_info
            else:
                params["status"] = status

            response = self.session.get(
                url,
                params=params,
                timeout=10,
            )
            response.raise_for_status()

            data = response.json()
            orders = data.get("orders", [])
            logger.info(f"Fetched {len(orders)} orders")

            next_page_info = None
            link_header = response.headers.get("Link")
            if link_header and 'rel="next"' in link_header:
                for link in link_header.split(","):
                    if 'rel="next"' in link:
                        start_idx = link.find("<")
                        end_idx = link.find(">")
                        if start_idx != -1 and end_idx != -1:
                            import urllib.parse as urlparse
                            url_part = link[start_idx + 1 : end_idx]
                            parsed = urlparse.urlparse(url_part)
                            query = urlparse.parse_qs(parsed.query)
                            if "page_info" in query:
                                next_page_info = query["page_info"][0]

            return orders, next_page_info

        except requests.exceptions.RequestException as e:
            error_details = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_details += f" | Body: {e.response.text}"
            logger.error(f"Failed to fetch paginated orders: {error_details}")
            raise  # Raise the exception so the FastAPI endpoint can catch it

    def list_orders_graphql(
        self, limit: int = 50, page_info: Optional[str] = None
    ) -> tuple[list[dict], Optional[str]]:
        """
        Fetch orders using GraphQL to bypass REST API deprecations.
        Maps the GraphQL response back to the REST schema format.
        """
        url = f"{self.base_url}/graphql.json"
        
        query = """
        query getOrders($limit: Int!, $cursor: String) {
          orders(first: $limit, after: $cursor, sortKey: CREATED_AT, reverse: true) {
            pageInfo {
              hasNextPage
              endCursor
            }
            edges {
              node {
                id
                createdAt
                totalPriceSet { shopMoney { amount currencyCode } }
                email
                customer { 
                  id
                  firstName
                  lastName
                  email
                  phone
                  amountSpent { amount }
                }
                lineItems(first: 50) {
                  edges {
                    node {
                      product { id }
                      sku
                      quantity
                      originalUnitPriceSet { shopMoney { amount } }
                    }
                  }
                }
              }
            }
          }
        }
        """
        
        variables = {"limit": limit, "cursor": page_info}
        
        try:
            response = self.session.post(url, json={"query": query, "variables": variables}, timeout=10)
            response.raise_for_status()
            data = response.json()
            
            if "errors" in data:
                raise requests.exceptions.RequestException(f"GraphQL Error: {data['errors']}")
                
            orders_data = data.get("data", {}).get("orders", {})
            
            mapped_orders = []
            for edge in orders_data.get("edges", []):
                node = edge.get("node", {})
                
                total_price_set = node.get("totalPriceSet") or {}
                shop_money = total_price_set.get("shopMoney") or {}
                
                line_items = []
                for line in node.get("lineItems", {}).get("edges", []):
                    line_node = line.get("node", {})
                    product = line_node.get("product")
                    product_id = product.get("id", "").split("/")[-1] if product else None
                    unit_price_set = line_node.get("originalUnitPriceSet") or {}
                    unit_price_money = unit_price_set.get("shopMoney") or {}
                    
                    line_items.append({
                        "product_id": product_id,
                        "sku": line_node.get("sku"),
                        "quantity": line_node.get("quantity"),
                        "price": unit_price_money.get("amount", "0.0")
                    })
                    
                customer = node.get("customer")
                customer_id = customer.get("id", "").split("/")[-1] if customer else None
                
                mapped_order = {
                    "id": node.get("id", "").split("/")[-1],
                    "created_at": node.get("createdAt"),
                    "total_price": shop_money.get("amount", "0.0"),
                    "currency": shop_money.get("currencyCode", "USD"),
                    "email": node.get("email"),
                    "customer": {
                        "id": customer_id,
                        "first_name": customer.get("firstName") if customer else None,
                        "last_name": customer.get("lastName") if customer else None,
                        "email": customer.get("email") if customer else None,
                        "phone": customer.get("phone") if customer else None,
                        "total_spent": customer.get("amountSpent", {}).get("amount") if customer and customer.get("amountSpent") else "0.0",
                    } if customer_id else None,
                    "line_items": line_items
                }
                mapped_orders.append(mapped_order)
                
            page_info_data = orders_data.get("pageInfo", {})
            next_page_info = page_info_data.get("endCursor") if page_info_data.get("hasNextPage") else None
            
            return mapped_orders, next_page_info
            
        except requests.exceptions.RequestException as e:
            error_details = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_details += f" | Body: {e.response.text}"
            logger.error(f"Failed to fetch GraphQL orders: {error_details}")
            raise

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
