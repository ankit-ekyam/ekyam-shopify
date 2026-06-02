"""MongoDB connection and utilities."""

import logging
import pymongo
from pymongo import MongoClient
from pymongo.database import Database
from pymongo.errors import OperationFailure, DuplicateKeyError
from app.config.settings import Settings

logger = logging.getLogger(__name__)

_db: Database | None = None


def get_mongo_db(settings: Settings) -> Database:
    """Get MongoDB database connection."""
    global _db
    if _db is None:
        client = MongoClient(settings.mongo_uri)
        _db = client[settings.mongo_db]

        # Create indexes for optimized querying and upserts
        try:
            _db[settings.mongo_collection_stores].create_index(
                [("shop_domain", pymongo.ASCENDING)], unique=True
            )
        except DuplicateKeyError:
            logger.warning(
                "Could not create unique index on shop_domain because duplicate documents exist."
            )
        except OperationFailure as e:
            if e.code != 85:  # 85 is IndexOptionsConflict
                raise
            logger.warning(
                "Index on shop_domain already exists with different options."
            )

        try:
            _db[settings.mongo_collection_orders].create_index(
                [("ext_order_id", pymongo.ASCENDING)], unique=True
            )
        except DuplicateKeyError:
            logger.warning(
                "Could not create unique index on ext_order_id because duplicate documents exist."
            )
        except OperationFailure as e:
            if e.code != 85:
                raise
            logger.warning(
                "Index on ext_order_id already exists with different options."
            )
    return _db


def close_mongo_connection():
    """Close MongoDB connection."""
    global _db
    if _db is not None:
        _db.client.close()
        _db = None
