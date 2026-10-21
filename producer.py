
import json
import uuid
import pika

RABBITMQ_URL = "amqp://grocery:grocery_dev_password@localhost:5672/%2F"
QUEUE_NAME = "grocery_requests"


def main():
    connection = pika.BlockingConnection(
        pika.URLParameters(RABBITMQ_URL)
    )
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    # Temporary queue for the consumer's response
    result = channel.queue_declare(queue="", exclusive=True)
    reply_queue = result.method.queue

    request_id = str(uuid.uuid4())
    correlation_id = str(uuid.uuid4())

    print("\nGrocery RabbitMQ Test")
    print("1. Create product")
    print("2. Read product")

    choice = input("Choose 1 or 2: ").strip()

    if choice == "1":
        request = {
            "request_id": request_id,
            "action": "create",
            "data": {
                "product_id": int(input("Product ID: ")),
                "name": input("Product name: "),
                "category": input("Category: "),
                "price": float(input("Price: ")),
                "quantity": int(input("Quantity: "))
            }
        }
    elif choice == "2":
        request = {
            "request_id": request_id,
            "action": "read",
            "data": {
                "product_id": int(input("Product ID: "))
            }
        }
    else:
        print("Invalid choice.")
        connection.close()
        return

    channel.basic_publish(
        exchange="",
        routing_key=QUEUE_NAME,
        body=json.dumps(request),
        properties=pika.BasicProperties(
            reply_to=reply_queue,
            correlation_id=correlation_id,
            content_type="application/json",
            delivery_mode=2
        )
    )

    print("\nRequest sent. Waiting for consumer response...")

    def on_response(ch, method, properties, body):
        if properties.correlation_id == correlation_id:
            print("\nResponse received:")
            print(json.dumps(json.loads(body), indent=2))
            ch.stop_consuming()

    channel.basic_consume(
        queue=reply_queue,
        on_message_callback=on_response,
        auto_ack=True
    )

    try:
        channel.start_consuming()
    finally:
        connection.close()


if __name__ == "__main__":
    main()
