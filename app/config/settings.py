"""Pydantic v2 settings for environment variables."""

from pydantic_settings import BaseSettings
from pydantic import Field


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    # Shopify OAuth2 Configuration
    shopify_api_key: str = Field(..., description="Shopify app API key")
    shopify_api_secret: str = Field(..., description="Shopify app API secret")
    shopify_webhook_secret: str = Field(..., description="Shopify webhook secret")
    app_url: str = Field(
        default="http://localhost:8000",
        description="Your app's public URL (e.g., ngrok URL)",
    )

    # Kafka Configuration
    kafka_bootstrap_servers: str = Field(
        default="localhost:9092", description="Kafka bootstrap servers"
    )
    kafka_group_id: str = Field(
        default="ekyam-standardization-group", description="Kafka consumer group ID"
    )

    # MongoDB Configuration
    mongo_uri: str = Field(
        default="mongodb://localhost:27017", description="MongoDB URI"
    )
    mongo_db: str = Field(default="ekyam", description="MongoDB database name")
    mongo_collection_orders: str = Field(
        default="shopify_orders", description="MongoDB collection for orders"
    )
    mongo_collection_stores: str = Field(
        default="shopify_stores", description="MongoDB collection for stores"
    )

    # App Configuration
    api_version: str = Field(default="unstable", description="Shopify API version")
    environment: str = Field(
        default="development", description="Environment (development/production)"
    )

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        case_sensitive = False


def get_settings() -> Settings:
    """Get the application settings."""
    return Settings()
