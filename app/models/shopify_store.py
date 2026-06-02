"""Pydantic models for Shopify data."""

from pydantic import BaseModel, Field
from datetime import datetime


class ShopifyStoreModel(BaseModel):
    """Model for storing Shopify store information and OAuth tokens."""

    shop_domain: str = Field(
        ..., description="Shop domain (e.g., 'myshop.myshopify.com')"
    )
    shop_id: str = Field(..., description="Shopify shop ID")
    access_token: str = Field(..., description="OAuth2 access token")
    scopes: list[str] = Field(default_factory=list, description="Granted OAuth2 scopes")
    installed_at: datetime = Field(
        default_factory=datetime.utcnow, description="Installation timestamp"
    )
    updated_at: datetime = Field(
        default_factory=datetime.utcnow, description="Last update timestamp"
    )
    is_active: bool = Field(
        default=True, description="Whether the app is still installed"
    )

    class Config:
        json_schema_extra = {
            "example": {
                "shop_domain": "example.myshopify.com",
                "shop_id": "12345678901234567890",
                "access_token": "shpua_xxxxxxxxxxxxxxxxxxxx",
                "scopes": ["read_orders", "read_customers"],
                "installed_at": "2026-05-10T10:30:00",
                "updated_at": "2026-05-10T10:30:00",
                "is_active": True,
            }
        }
