"""Shopify Pusher utility for writing data to Shopify accounts."""

import logging
import requests
from typing import Optional

logger = logging.getLogger(__name__)


class ShopifyDataPusher:
    """Static class to push data into different Shopify accounts without code repetition."""

    @staticmethod
    def _execute_request(
        shop_domain: str,
        access_token: str,
        api_version: str,
        endpoint: str,
        method: str,
        payload: Optional[dict] = None
    ) -> Optional[dict]:
        """
        Generic core method to execute HTTP requests to Shopify.
        Eliminates repetition by handling headers, URL formatting, and error tracking centrally.
        """
        url = f"https://{shop_domain}/admin/api/{api_version}/{endpoint}"
        headers = {
            "X-Shopify-Access-Token": access_token,
            "Content-Type": "application/json",
        }
        try:
            response = requests.request(method, url, headers=headers, json=payload, timeout=10)
            response.raise_for_status()
            
            # DELETE responses might be empty
            return response.json() if response.text else {}
        except requests.exceptions.RequestException as e:
            error_msg = str(e)
            if hasattr(e, "response") and e.response is not None:
                error_msg += f" | Response: {e.response.text}"
            logger.error(f"Failed to {method} {endpoint} for {shop_domain}: {error_msg}")
            return None

    @staticmethod
    def push_entity(
        shop_domain: str,
        access_token: str,
        api_version: str,
        entity_name: str,
        entity_data: dict
    ) -> Optional[dict]:
        """
        Creates a new entity (e.g., 'order', 'product', 'customer') in the target Shopify account.
        """
        # Shopify REST API typically uses plural for endpoints and singular for payload wrappers
        endpoint = f"{entity_name}s.json"
        payload = {entity_name: entity_data}
        
        logger.info(f"Pushing new {entity_name} to {shop_domain}")
        return ShopifyDataPusher._execute_request(
            shop_domain=shop_domain,
            access_token=access_token,
            api_version=api_version,
            endpoint=endpoint,
            method="POST",
            payload=payload
        )

    @staticmethod
    def update_entity(
        shop_domain: str,
        access_token: str,
        api_version: str,
        entity_name: str,
        entity_id: str,
        entity_data: dict
    ) -> Optional[dict]:
        """
        Updates an existing entity in the target Shopify account.
        """
        endpoint = f"{entity_name}s/{entity_id}.json"
        payload = {entity_name: entity_data}
        
        logger.info(f"Updating {entity_name} {entity_id} in {shop_domain}")
        return ShopifyDataPusher._execute_request(
            shop_domain=shop_domain,
            access_token=access_token,
            api_version=api_version,
            endpoint=endpoint,
            method="PUT",
            payload=payload
        )