# ekyam-shopify: OAuth2 Integration with Shopify

A FastAPI-based Shopify app that implements OAuth2 authentication and processes order data through Kafka and MongoDB.

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────┐
│  INSTALLATION PHASE (Store Owner)                          │
├─────────────────────────────────────────────────────────────┤
│  1. Click Install → /install?shop=myshop.myshopify.com     │
│  2. Redirected to Shopify OAuth Login                       │
│  3. Store owner approves scopes                             │
│  4. Shopify redirects to /oauth/callback with code          │
│  5. Exchange code for access token (server-side)            │
│  6. Store token in MongoDB (shopify_stores collection)      │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  WEBHOOK PHASE (Shopify → Your App)                         │
├─────────────────────────────────────────────────────────────┤
│  1. Shopify sends order webhook to /webhooks/shopify/orders │
│  2. Verify HMAC signature (webhook security)                │
│  3. Push order to Kafka topic (shopify.raw.orders)          │
│  4. Return 200 OK immediately                               │
└─────────────────────────────────────────────────────────────┘
                            ↓
┌─────────────────────────────────────────────────────────────┐
│  PROCESSING PHASE (Consumer)                                │
├─────────────────────────────────────────────────────────────┤
│  1. Consumer reads from Kafka topic                         │
│  2. Retrieve stored access token from MongoDB               │
│  3. Optional: Call Shopify API to enrich order data         │
│  4. Transform to Ekyam standard format                      │
│  5. Store in MongoDB (shopify_orders collection)            │
└─────────────────────────────────────────────────────────────┘
```

## Quick Start

### Prerequisites
- Python 3.11+
- Docker & Docker Compose (for Zookeeper, Kafka, MongoDB)
- Shopify Partner account (for development/testing)
- `uv` package manager

### Step 1: Clone & Setup Project

```bash
cd /path/to/ekyam-shopify
uv sync  # Install all dependencies from pyproject.toml
```

### Step 2: Configure Environment Variables

Copy `.env.example` to `.env` and fill in your Shopify credentials:

```bash
cp .env.example .env
```

Edit `.env` with your values:
```
SHOPIFY_API_KEY=<your_api_key>
SHOPIFY_API_SECRET=<your_api_secret>
SHOPIFY_WEBHOOK_SECRET=<your_webhook_secret>
APP_URL=https://starless-twilight-hedge.ngrok-free.dev  # Update with ngrok URL in production
```

**How to Get These Values:**
1. Go to [Shopify Partner Dashboard](https://partners.shopify.com)
2. Create a new app (or select existing)
3. Go to **Configuration** tab
4. Copy **API Key** and **API Secret**
5. Under **Webhook**, copy the **Webhook Secret**

### Step 3: Start Infrastructure (Docker)

```bash
docker compose up -d
```

This starts:
- Zookeeper (port 2181)
- Kafka (port 9092)
- MongoDB (port 27017)

Verify they're running:
```bash
docker compose ps
```

### Step 4: Start Producer (OAuth2 + Webhooks)

Open Terminal 1:
```bash
uv run uvicorn producer:app --reload --port 8000
```

This starts the FastAPI server with:
- `/install` - OAuth2 installation flow
- `/oauth/callback` - OAuth2 callback handler
- `/webhooks/shopify/orders` - Webhook receiver (with HMAC verification)
- `/health` - Health check endpoint

### Step 5: Start Consumer (Kafka → MongoDB)

Open Terminal 2:
```bash
uv run consumer.py
```

The consumer will:
- Read from Kafka topic `shopify.raw.orders`
- Retrieve OAuth2 access token for each store from MongoDB
- Optionally enrich orders with customer data (via Shopify API)
- Transform to Ekyam standard format
- Store in MongoDB collection `shopify_orders`

### Step 6: Expose to Internet (for Webhooks)

Open Terminal 3:
```bash
ngrok http 8000
```

This gives you a public URL like: `https://abc123.ngrok.io`

### Step 7: Install App on Test Store

Use the install flow with your ngrok URL:

```
https://abc123.ngrok.io/install?shop=myshop
```

**What Happens:**
1. You're redirected to Shopify login
2. Approve the requested scopes (read_orders, read_customers)
3. You're redirected to `/oauth/callback`
4. Access token is stored in MongoDB
5. Ready to receive webhooks!

### Step 8: Test Order Webhook

Create an order in your Shopify test store:
1. Go to **Orders** in admin
2. Create a test order
3. Check your consumer logs for processing

Or trigger a test webhook (after completing OAuth2):
```bash
curl -X POST http://localhost:8000/webhooks/shopify/orders \
  -H "X-Shopify-Hmac-SHA256: valid_hmac_here" \
  -H "Content-Type: application/json" \
  -d '{"id": 12345, "email": "test@example.com", "total_price": "100.00", "currency": "USD", "line_items": []}'
```

## API Endpoints

### OAuth2 Endpoints

#### `GET /install?shop=<shop_domain>`
Initiates OAuth2 authorization flow.

**Example:**
```bash
curl "http://localhost:8000/install?shop=myshop.myshopify.com"
```

**Response:** Redirects to Shopify login page.

#### `GET /oauth/callback?code=<code>&shop=<shop>&state=<state>`
OAuth2 callback endpoint (called by Shopify automatically).

**Response:**
```json
{
  "status": "success",
  "message": "App installed successfully for myshop.myshopify.com",
  "shop": "myshop.myshopify.com"
}
```

### Webhook Endpoints

#### `POST /webhooks/shopify/orders`
Receives order creation webhooks.

**Headers:**
- `X-Shopify-Hmac-SHA256`: Webhook signature
- `X-Shopify-Shop-Api-Call-Limit`: Shop information

**Response:**
```json
{
  "status": "accepted"
}
```

#### `POST /webhooks/shopify/orders/updated`
Receives order update webhooks.

**Response:** Same as above.

### Health Check

#### `GET /health`
Service health check.

**Response:**
```json
{
  "status": "ok",
  "service": "ekyam-shopify-producer"
}
```

## Database Schema

### MongoDB Collections

#### `shopify_stores`
Stores OAuth2 tokens and store information.

```json
{
  "_id": ObjectId("..."),
  "shop_domain": "myshop.myshopify.com",
  "shop_id": "myshop",
  "access_token": "shpua_xxxxxxxxxxxxxxxxxxxx",
  "scopes": ["read_orders", "read_customers"],
  "installed_at": "2026-05-10T10:30:00.000Z",
  "updated_at": "2026-05-10T10:30:00.000Z",
  "is_active": true
}
```

#### `shopify_orders`
Processed and standardized orders.

```json
{
  "_id": ObjectId("..."),
  "ext_order_id": "12345",
  "shop_domain": "myshop.myshopify.com",
  "event_timestamp": "2026-05-10T10:30:00Z",
  "order_total_amt": 100.50,
  "currency": "USD",
  "customer_email": "customer@example.com",
  "customer_id": "987654",
  "order_lines": [
    {
      "ext_product_id": "456",
      "sku": "PROD-001",
      "quantity": 2,
      "unit_price": 50.25
    }
  ],
  "processed_at": "2026-05-10T10:35:00.000Z",
  "enriched": true,
  "enriched_customer": {
    "customer_id": "987654",
    "customer_name": "John Doe",
    "customer_email": "customer@example.com",
    "customer_phone": "+1234567890",
    "customer_lifetime_value": 500.00
  }
}
```

## Project Structure

```
ekyam-shopify/
├── app/
│   ├── config/
│   │   ├── __init__.py
│   │   └── settings.py           # Pydantic v2 settings
│   ├── auth/
│   │   ├── __init__.py
│   │   └── shopify_oauth.py      # OAuth2 logic
│   ├── models/
│   │   ├── __init__.py
│   │   └── shopify_store.py      # Data models
│   ├── utils/
│   │   ├── __init__.py
│   │   ├── kafka_utils.py        # Kafka helpers
│   │   └── shopify_api.py        # Shopify API client
│   ├── database.py               # MongoDB connection
│   └── __init__.py
├── producer.py                   # FastAPI app (webhooks + OAuth2)
├── consumer.py                   # Kafka consumer + processor
├── docker-compose.yml            # Docker services
├── pyproject.toml                # Dependencies
├── .env.example                  # Env template
└── README.md                     # This file
```

## Troubleshooting

### OAuth2 Installation Fails
- Check `SHOPIFY_API_KEY` and `SHOPIFY_API_SECRET` are correct
- Ensure `APP_URL` matches your ngrok URL
- Check logs: `docker logs <container_id>`

### Webhooks Not Received
- Verify `SHOPIFY_WEBHOOK_SECRET` is correct
- Check webhook was registered in Shopify admin
- Verify HMAC signature computation

### Consumer Not Processing Orders
- Check Kafka is running: `docker compose ps`
- Verify MongoDB connection in logs
- Check access token is stored in MongoDB after OAuth2 installation

### Import Errors
- Reinstall dependencies: `uv sync`
- Verify Python version >= 3.11: `python --version`

## Security Best Practices

1. **Environment Variables**: Never commit `.env` file to git
2. **API Secret**: Keep `SHOPIFY_API_SECRET` secure
3. **Access Tokens**: Store in database (MongoDB), never in code
4. **HMAC Verification**: Always verify webhook signatures
5. **HTTPS**: Use HTTPS in production (not just http://)
6. **State Parameter**: CSRF protection in OAuth2 (already implemented)

## Testing

### Test Health Endpoint
```bash
curl http://localhost:8000/health
```

### Test OAuth2 Flow (Manual)
1. Visit: `http://localhost:8000/install?shop=your-test-shop`
2. You'll be redirected to Shopify
3. Approve scopes
4. You'll see success message

### Test Webhook Manually
```bash
# First, get a valid HMAC for testing
# Use your SHOPIFY_WEBHOOK_SECRET

python3 -c "
import hmac
import hashlib
import base64
import json

secret = 'your_webhook_secret'
body = json.dumps({'id': 123, 'email': 'test@example.com'}).encode()
hmac_value = base64.b64encode(hmac.new(secret.encode(), body, hashlib.sha256).digest()).decode()
print(hmac_value)
"

# Then use the HMAC:
curl -X POST http://localhost:8000/webhooks/shopify/orders \
  -H "X-Shopify-Hmac-SHA256: <hmac_value>" \
  -H "Content-Type: application/json" \
  -d '{"id": 123, "email": "test@example.com", "total_price": "100", "currency": "USD", "line_items": []}'
```

## Next Steps

1. **Register Public App**: List on Shopify App Store (requires Partner account)
2. **Add More Scopes**: Modify scopes in `app/auth/shopify_oauth.py`
3. **Webhook Management**: Programmatically create webhooks after installation
4. **Error Handling**: Add retry logic for Shopify API calls
5. **Monitoring**: Implement logging and metrics

## References

- [Shopify OAuth2 Documentation](https://shopify.dev/docs/admin-api/2024-01/resources/oauth)
- [Shopify Webhooks](https://shopify.dev/docs/admin-api/2024-01/resources/webhook)
- [FastAPI Documentation](https://fastapi.tiangolo.com/)
- [Pydantic v2 Documentation](https://docs.pydantic.dev/latest/)


MONGODB - DOCKER
docker compose exec mongodb mongosh
use ekyam
show collections 
db.shopify_stores.find().pretty()
db.shopify_orders.find().pretty()

# ekyam-shopify