# Grocery Store Management — RabbitMQ receiver/backend

## Components
- RabbitMQ request queue: `grocery_requests` (durable)
- PostgreSQL table: `grocery_products`
- Redis key: `grocery:<product_id>` (example: `grocery:101`)
- Docker network: `grocery-net`
- Request-response properties: request must set `reply_to` and `correlation_id`; consumer copies the exact correlation ID into the response.

## Start
Run these commands from this folder in PowerShell:

```powershell
docker compose up --build -d
docker compose ps
docker compose logs -f consumer
```

RabbitMQ Management UI: http://localhost:15672
Login: `grocery` / `grocery_dev_password`

PostgreSQL is exposed to the host at `localhost:5434`, Redis at `localhost:6380`, and RabbitMQ AMQP at `localhost:5672`. Inside Docker, use service names `postgres:5432`, `redis:6379`, and `rabbitmq:5672`.

## PostgreSQL schema
```sql
CREATE TABLE grocery_products (
    product_id INTEGER PRIMARY KEY,
    name VARCHAR(150) NOT NULL,
    category VARCHAR(100) NOT NULL,
    price NUMERIC(10, 2) NOT NULL CHECK (price >= 0),
    quantity INTEGER NOT NULL CHECK (quantity >= 0)
);
```

## Message contract
Create:
```json
{
  "request_id": "REQ101",
  "sender": "Bhakti",
  "action": "create",
  "data": {
    "product_id": 101,
    "name": "Rice",
    "category": "Grains",
    "price": 65.50,
    "quantity": 100
  }
}
```

Read:
```json
{
  "request_id": "REQ102",
  "sender": "Bhakti",
  "action": "read",
  "data": { "product_id": 101 }
}
```

The sender must publish to `grocery_requests` with `reply_to` set to its response queue and a unique `correlation_id`. Responses are JSON with `request_id`, `status`, `message`, and `data`; a `cache_status` may also be included. On response, the consumer sets the response `correlation_id` to exactly the incoming value.

## Data/cache behavior
- PostgreSQL is the source of truth.
- `create` inserts into PostgreSQL, then writes the committed record to Redis.
- `read` checks Redis first. On a cache miss or malformed cache entry, it reads PostgreSQL and refreshes Redis.
- If Redis is unavailable, reads fall back to PostgreSQL. A successful DB insert remains successful if the Redis refresh fails; response includes `cache_status: refresh_failed`, and logs identify the cache issue. This avoids pretending that a cross-database transaction is atomic. Retry/repair the cache after Redis returns.
- Duplicate `product_id` values return an error rather than overwriting an existing product.
- Invalid JSON and validation errors return an error response if `reply_to` exists, then the request is acknowledged to avoid endless poison-message retries. Database failures return an error response and are logged.

## Verify data
```powershell
# PostgreSQL
docker exec -it grocery-postgres psql -U grocery_user -d grocery_db -c "SELECT * FROM grocery_products;"

# Redis
docker exec -it grocery-redis redis-cli GET grocery:101

# Logs
docker compose logs -f consumer
```

## Important integration note for Bhakti
- Publish request messages to `grocery_requests`.
- Set `reply_to` to a queue Bhakti is consuming.
- Set `correlation_id` to a unique value and match that same value when receiving the response.
- Ensure the reply queue exists before sending the request.
- These are development credentials; replace them for any shared/production deployment.
