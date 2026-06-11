import requests
import time
import sys

# Configuration
API_URL = "http://localhost:8000/sync/orders"
SHOP = "devekyam.myshopify.com"
LIMIT = 30  # Keeping it small forces multiple pages during testing

def test_historical_sync():
    print(f"Starting historical sync test for {SHOP}...")
    
    has_next_page = True
    page_info = None
    total_queued = 0
    page_count = 0

    while has_next_page:
        params = {"shop": SHOP, "limit": LIMIT}
        if page_info:
            params["page_info"] = page_info  # The cursor to the next small chunk
            
        print(f"\nFetching page {page_count + 1}...")
        
        try:
            response = requests.get(API_URL, params=params)
            
            # If we hit a rate limit (Shopify 429 wrapped as 502 by the app)
            if response.status_code == 502:
                print("⚠️ Hit rate limit or Shopify error. Waiting 5 seconds to retry...")
                time.sleep(5)
                continue
                
            response.raise_for_status()
            data = response.json()
            
            # Extract pagination data to fetch the next batch
            page_info = data.get("next_page_info")
            has_next_page = data.get("has_next_page", False)
            
            # Safely parse the "Queued X orders..." message
            msg = data.get("message", "0")
            queued_this_request = int(msg.split()[1]) if "Queued" in msg else 0
            
            total_queued += queued_this_request
            page_count += 1
            
            print(f"✅ Success: {data.get('message')}")
            
            # Small delay to prevent aggressively hammering your local FastAPI server
            time.sleep(1)
            
        except requests.exceptions.RequestException as e:
            print(f"❌ Request failed: {e}")
            if response is not None:
                print(f"Response details: {response.text}")
            sys.exit(1)

    print("\n" + "="*40)
    print("🎉 SYNC COMPLETE 🎉")
    print(f"Total pages fetched: {page_count}")
    print(f"Total orders queued to Kafka: {total_queued}")
    print("="*40)

if __name__ == "__main__":
    test_historical_sync()