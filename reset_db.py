from pymongo import MongoClient

def reset():
    # Connect to local MongoDB
    client = MongoClient("mongodb://127.0.0.1:27017")
    db = client["ekyam"]
    
    # Force delete the store record
    token_result = db["shopify_stores"].delete_many({"shop_domain": "devekyam.myshopify.com"})
    print(f"✅ Deleted {token_result.deleted_count} old token records from MongoDB.")
    
    # Also clear out any previously synced orders for a clean test run
    orders_result = db["shopify_orders"].delete_many({})
    print(f"✅ Deleted {orders_result.deleted_count} old order records from MongoDB.")

if __name__ == "__main__":
    reset()
