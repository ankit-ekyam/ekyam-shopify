"""Shopify OAuth2 authentication module."""

import logging
import requests
from urllib.parse import urlencode
from datetime import datetime
from pymongo.database import Database
from app.config.settings import Settings
from app.models.shopify_store import ShopifyStoreModel

logger = logging.getLogger(__name__)


REQUIRED_SCOPES = [
    "read_orders",
    "write_orders",
    "read_customers",
    "write_customers",
    "read_webhooks",
    "write_webhooks",
]


class ShopifyOAuth2:
    """Handles Shopify OAuth2 flow."""

    def __init__(self, settings: Settings, db: Database):
        self.settings = settings
        self.db = db
        self.store_collection = db[settings.mongo_collection_stores]

    def get_authorization_url(self, shop_domain: str, state: str) -> str:
        """
        Generate Shopify OAuth2 authorization URL.

        Args:
            shop_domain: Shopify store domain (e.g., 'myshop.myshopify.com')
            state: CSRF protection token (random string)

        Returns:
            Full authorization URL to redirect the user to
        """
        params = {
            "client_id": self.settings.shopify_api_key,
            "scope": ",".join(REQUIRED_SCOPES),
            "redirect_uri": f"{self.settings.app_url}/oauth/callback",
            "state": state,
        }

        # Ensure shop_domain is correct format
        if not shop_domain.endswith(".myshopify.com"):
            shop_domain = f"{shop_domain}.myshopify.com"

        auth_url = f"https://{shop_domain}/admin/oauth/authorize?{urlencode(params)}"
        logger.info(f"Generated authorization URL for {shop_domain}")
        return auth_url

    def exchange_code_for_token(self, shop_domain: str, code: str) -> dict | None:
        """
        Exchange authorization code for access token.

        This is a server-side exchange (must be kept secret).

        Args:
            shop_domain: Shopify store domain
            code: Authorization code from Shopify callback

        Returns:
            Dictionary with access_token and scope, or None if failed
        """
        if not shop_domain.endswith(".myshopify.com"):
            shop_domain = f"{shop_domain}.myshopify.com"

        token_url = f"https://{shop_domain}/admin/oauth/access_token"

        payload = {
            "client_id": self.settings.shopify_api_key,
            "client_secret": self.settings.shopify_api_secret,
            "code": code,
        }

        try:
            response = requests.post(token_url, json=payload, timeout=10)
            response.raise_for_status()

            token_data = response.json()
            logger.info(f"Successfully exchanged code for token for {shop_domain}")
            return token_data

        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to exchange code for token: {e}")
            return None

    def get_granted_access_scopes(
        self, shop_domain: str, access_token: str
    ) -> list[str]:
        """
        Retrieve the authoritative granted scopes for an access token.

        Shopify's token exchange response can omit implied scopes. The
        AccessScope endpoint returns the scopes currently associated with the
        token and is the source of truth for what should be persisted.
        """
        if not shop_domain.endswith(".myshopify.com"):
            shop_domain = f"{shop_domain}.myshopify.com"

        scopes_url = f"https://{shop_domain}/admin/oauth/access_scopes.json"
        headers = {"X-Shopify-Access-Token": access_token}

        try:
            response = requests.get(scopes_url, headers=headers, timeout=10)
            response.raise_for_status()
            data = response.json()
            granted_scopes = [
                scope["handle"]
                for scope in data.get("access_scopes", [])
                if scope.get("handle")
            ]
            logger.info("Granted access scopes for %s: %s", shop_domain, granted_scopes)
            return granted_scopes
        except requests.exceptions.RequestException as e:
            logger.error(f"Failed to fetch granted access scopes: {e}")
            return []

    def store_access_token(self, shop_domain: str, token_data: dict) -> bool:
        """
        Store access token in MongoDB.

        Args:
            shop_domain: Shopify store domain
            token_data: Token response from Shopify (contains access_token, scope, etc.)

        Returns:
            True if successful, False otherwise
        """
        try:
            # Extract shop ID from domain
            shop_id = shop_domain.split(".")[0]
            access_token = token_data.get("access_token", "")
            granted_scopes = self.get_granted_access_scopes(shop_domain, access_token)
            if not granted_scopes:
                granted_scopes = [
                    scope
                    for scope in token_data.get("scope", "").split(",")
                    if scope
                ]

            store_model = ShopifyStoreModel(
                shop_domain=shop_domain,
                shop_id=shop_id,
                access_token=access_token,
                scopes=granted_scopes,
                installed_at=datetime.utcnow(),
                updated_at=datetime.utcnow(),
                is_active=True,
            )

            # Upsert: update if exists, insert if not
            result = self.store_collection.update_one(
                {"shop_domain": shop_domain},
                {"$set": store_model.model_dump()},
                upsert=True,
            )

            logger.info(f"Stored access token for {shop_domain}")
            return True

        except Exception as e:
            logger.error(f"Failed to store access token: {e}")
            return False

    def get_access_token(self, shop_domain: str) -> str | None:
        """
        Retrieve access token from MongoDB for a store.

        Args:
            shop_domain: Shopify store domain

        Returns:
            Access token if found and active, None otherwise
        """
        try:
            store = self.store_collection.find_one(
                {"shop_domain": shop_domain, "is_active": True}
            )
            if store:
                return store.get("access_token")
            return None

        except Exception as e:
            logger.error(f"Failed to retrieve access token: {e}")
            return None

    def verify_webhook(self, request_body: bytes, hmac_header: str) -> bool:
        """
        Verify Shopify webhook HMAC signature.

        Args:
            request_body: Raw request body bytes
            hmac_header: X-Shopify-Hmac-SHA256 header value

        Returns:
            True if signature is valid, False otherwise
        """
        import hmac
        import hashlib
        import base64

        try:
            expected_hmac = hmac.new(
                self.settings.shopify_webhook_secret.encode("utf-8"),
                request_body,
                hashlib.sha256,
            ).digest()
            expected_hmac_b64 = base64.b64encode(expected_hmac).decode("utf-8")

            is_valid = hmac.compare_digest(expected_hmac_b64, hmac_header)

            if not is_valid:
                logger.warning("Webhook HMAC verification failed")

            return is_valid

        except Exception as e:
            logger.error(f"Webhook verification error: {e}")
            return False
