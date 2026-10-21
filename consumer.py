import json
import logging
import os
import time
from decimal import Decimal, InvalidOperation

import pika
import psycopg2
from psycopg2.extras import RealDictCursor
import redis

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s %(levelname)s %(message)s",
)
log = logging.getLogger("grocery-consumer")

QUEUE_NAME = os.getenv("RABBITMQ_QUEUE", "grocery_requests")
CACHE_PREFIX = os.getenv("REDIS_KEY_PREFIX", "grocery:")
MAX_RETRIES = int(os.getenv("CONNECT_RETRIES", "30"))


def wait_for_services():
    """Wait for RabbitMQ, PostgreSQL, and Redis to become reachable."""
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "postgres"),
                port=int(os.getenv("POSTGRES_PORT", "5432")),
                dbname=os.getenv("POSTGRES_DB", "grocery_db"),
                user=os.getenv("POSTGRES_USER", "grocery_user"),
                password=os.getenv("POSTGRES_PASSWORD", "grocery_password"),
                connect_timeout=3,
            )
            conn.close()
            r = redis.Redis(
                host=os.getenv("REDIS_HOST", "redis"),
                port=int(os.getenv("REDIS_PORT", "6379")),
                decode_responses=True,
                socket_connect_timeout=3,
            )
            r.ping()
            params = pika.URLParameters(os.getenv(
                "RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F"
            ))
            test_conn = pika.BlockingConnection(params)
            test_conn.close()
            log.info("All services are reachable.")
            return
        except Exception as exc:
            log.info("Waiting for services (%s/%s): %s", attempt, MAX_RETRIES, exc)
            time.sleep(2)
    raise RuntimeError("Services did not become ready in time.")


def db_connect():
    return psycopg2.connect(
        host=os.getenv("POSTGRES_HOST", "postgres"),
        port=int(os.getenv("POSTGRES_PORT", "5432")),
        dbname=os.getenv("POSTGRES_DB", "grocery_db"),
        user=os.getenv("POSTGRES_USER", "grocery_user"),
        password=os.getenv("POSTGRES_PASSWORD", "grocery_password"),
        cursor_factory=RealDictCursor,
    )


def redis_client():
    return redis.Redis(
        host=os.getenv("REDIS_HOST", "redis"),
        port=int(os.getenv("REDIS_PORT", "6379")),
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
    )


def cache_key(product_id):
    return f"{CACHE_PREFIX}{product_id}"


def as_product(row):
    if row is None:
        return None
    result = dict(row)
    result["price"] = float(result["price"])
    result["quantity"] = int(result["quantity"])
    result["product_id"] = int(result["product_id"])
    return result


def validate_request(request):
    if not isinstance(request, dict):
        raise ValueError("Request JSON must be an object.")
    action = request.get("action")
    if action not in ("create", "read"):
        raise ValueError("Unsupported action. Use 'create' or 'read'.")

    data = request.get("data")
    if not isinstance(data, dict):
        raise ValueError("'data' must be a JSON object.")

    if action == "create":
        required = ("product_id", "name", "category", "price", "quantity")
        missing = [field for field in required if field not in data]
        if missing:
            raise ValueError("Missing required field(s): " + ", ".join(missing))
        try:
            product_id = int(data["product_id"])
            price = Decimal(str(data["price"]))
            quantity = int(data["quantity"])
        except (ValueError, TypeError, InvalidOperation):
            raise ValueError("product_id and quantity must be integers; price must be numeric.")
        if product_id <= 0:
            raise ValueError("product_id must be greater than zero.")
        if not str(data["name"]).strip() or not str(data["category"]).strip():
            raise ValueError("name and category cannot be empty.")
        if not price.is_finite() or price < 0:
            raise ValueError("price must be a finite, non-negative number.")
        if quantity < 0:
            raise ValueError("quantity cannot be negative.")
        return action, {
            "product_id": product_id,
            "name": str(data["name"]).strip(),
            "category": str(data["category"]).strip(),
            "price": price,
            "quantity": quantity,
        }

    product_id = data.get("product_id", request.get("product_id"))
    if product_id is None:
        raise ValueError("Read request requires data.product_id.")
    try:
        product_id = int(product_id)
    except (ValueError, TypeError):
        raise ValueError("product_id must be an integer.")
    if product_id <= 0:
        raise ValueError("product_id must be greater than zero.")
    return action, {"product_id": product_id}


def create_product(product):
    """PostgreSQL is authoritative. Redis is refreshed after the DB commit."""
    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO grocery_products (product_id, name, category, price, quantity)
                VALUES (%s, %s, %s, %s, %s)
                RETURNING product_id, name, category, price, quantity
                """,
                (
                    product["product_id"], product["name"], product["category"],
                    product["price"], product["quantity"],
                ),
            )
            saved = as_product(cur.fetchone())
    cache_status = "updated"
    try:
        redis_client().set(cache_key(saved["product_id"]), json.dumps(saved))
    except redis.RedisError:
        cache_status = "refresh_failed"
        log.exception("PostgreSQL insert succeeded, but Redis cache update failed.")
    return saved, cache_status


def read_product(product_id):
    """Try Redis first; on a cache miss, read PostgreSQL and repopulate Redis."""
    r = redis_client()
    key = cache_key(product_id)
    try:
        cached = r.get(key)
        if cached:
            try:
                product = json.loads(cached)
                if int(product.get("product_id", -1)) == product_id:
                    return product, "hit"
            except (ValueError, TypeError, json.JSONDecodeError):
                log.warning("Invalid cached value for %s; rebuilding it.", key)
    except redis.RedisError:
        log.exception("Redis read failed; falling back to PostgreSQL.")

    with db_connect() as conn:
        with conn.cursor() as cur:
            cur.execute(
                """SELECT product_id, name, category, price, quantity
                   FROM grocery_products WHERE product_id = %s""",
                (product_id,),
            )
            row = cur.fetchone()
    product = as_product(row)
    if product is None:
        return None, "miss"
    try:
        r.set(key, json.dumps(product))
        cache_status = "refreshed"
    except redis.RedisError:
        log.exception("Database read succeeded, but Redis cache refresh failed.")
        cache_status = "refresh_failed"
    return product, cache_status


def make_response(request, status, message, data=None, cache_status=None):
    response = {
        "request_id": request.get("request_id") if isinstance(request, dict) else None,
        "status": status,
        "message": message,
        "data": data,
    }
    if cache_status is not None:
        response["cache_status"] = cache_status
    return response


def process_request(body):
    request = None
    try:
        request = json.loads(body.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return make_response({}, "error", "Invalid JSON message."), False

    try:
        action, data = validate_request(request)
        if action == "create":
            product, cache_status = create_product(data)
            return make_response(
                request, "success", "Grocery product created successfully.",
                product, cache_status,
            ), True

        product, cache_status = read_product(data["product_id"])
        if product is None:
            return make_response(
                request, "error", f"Product {data['product_id']} was not found.", None,
                cache_status,
            ), True
        return make_response(
            request, "success", "Grocery product retrieved successfully.",
            product, cache_status,
        ), True
    except ValueError as exc:
        return make_response(request, "error", str(exc)), True
    except psycopg2.IntegrityError:
        log.exception("Database integrity error.")
        return make_response(request, "error", "Product already exists or violates a database constraint."), True
    except (psycopg2.Error, redis.RedisError) as exc:
        log.exception("Database/cache operation failed.")
        return make_response(request, "error", "A database/cache operation failed; check consumer logs."), True
    except Exception:
        log.exception("Unexpected request-processing error.")
        return make_response(request, "error", "Unexpected backend error; check consumer logs."), True


def main():
    wait_for_services()
    params = pika.URLParameters(os.getenv(
        "RABBITMQ_URL", "amqp://guest:guest@rabbitmq:5672/%2F"
    ))
    connection = pika.BlockingConnection(params)
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.basic_qos(prefetch_count=1)

    def on_message(ch, method, properties, body):
        reply_to = getattr(properties, "reply_to", None)
        correlation_id = getattr(properties, "correlation_id", None)
        response, processed = process_request(body)

        # If the sender provided reply_to, return the result with the exact
        # correlation_id from the request. Use persistent JSON messages.
        try:
            if reply_to:
                ch.basic_publish(
                    exchange="",
                    routing_key=reply_to,
                    body=json.dumps(response, default=str),
                    properties=pika.BasicProperties(
                        content_type="application/json",
                        delivery_mode=2,
                        correlation_id=correlation_id,
                    ),
                )
            else:
                log.warning("Request had no reply_to property; no response can be routed.")
            # Acknowledge after processing and response publication attempt.
            # Malformed requests are acknowledged after returning an error where possible,
            # preventing a poison message from looping forever.
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception:
            log.exception("Failed while publishing response; requeueing original request.")
            ch.basic_nack(delivery_tag=method.delivery_tag, requeue=True)

    channel.basic_consume(queue=QUEUE_NAME, on_message_callback=on_message, auto_ack=False)
    log.info("Consuming queue '%s'. Press CTRL+C to stop.", QUEUE_NAME)
    try:
        channel.start_consuming()
    except KeyboardInterrupt:
        log.info("Stopping consumer.")
        channel.stop_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
