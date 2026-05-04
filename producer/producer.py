import json
import time
import random
import logging
from faker import Faker
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

# --- Logging Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)-8s %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- Config
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC_ORDERS = "orders"
TOPIC_CLICKS = "page_clicks"
TOPIC_SIGNUPS = "user_signups"
EVENTS_PER_SECOND = 2
RETRY_DELAY_SECONDS = 5

fake = Faker()

# --- Static Reference Data
PRODUCTS = [
    {"id": "P001", "name": "Wireless Headphones",  "category": "Electronics", "price": 79.99},
    {"id": "P002", "name": "Running Shoes",         "category": "Sports",      "price": 109.95},
    {"id": "P003", "name": "Coffee Maker",          "category": "Home",        "price": 49.99},
    {"id": "P004", "name": "Python Cookbook",       "category": "Books",       "price": 34.99},
    {"id": "P005", "name": "Yoga Mat",              "category": "Sports",      "price": 29.99},
    {"id": "P006", "name": "Mechanical Keyboard",   "category": "Electronics", "price": 129.99},
    {"id": "P007", "name": "Desk Lamp",             "category": "Home",        "price": 24.99},
    {"id": "P008", "name": "Water Bottle",          "category": "Sports",      "price": 19.99},
    {"id": "P009", "name": "Noise-Cancel Earbuds",  "category": "Electronics", "price": 59.99},
    {"id": "P010", "name": "Backpack",              "category": "Fashion",     "price": 54.99},
]

ORDER_STATUSES = ["pending", "confirmed", "shipped", "delivered", "cancelled"]
PAYMENT_METHODS = ["credit_card", "debit_card", "paypal", "gcash", "maya"]
PAGES = ["/", "/products", "/cart", "/checkout", "/account" "/deals"]
DEVICES = ["mobile", "desktop", "tablet"]

# --- Event Generators
def make_order_event() -> dict:
    product = random.choice(PRODUCTS)
    quantity = random.randint(1, 4)
    discount = round(random.uniform(0, 0.25), 2)
    subtotal = round(product["price"] * quantity, 2)
    total = round(subtotal * (1 - discount), 2)

    return {
        "event_type":      "order_placed",
        "order_id":        fake.uuid4(),
        "user_id":         fake.uuid4(),
        "product_id":      product["id"],
        "product_name":    product["name"],
        "category":        product["category"],
        "unit_price":      product["price"],
        "quantity":        quantity,
        "discount":        discount,
        "total_amount":    total,
        "currency":        "PHP",
        "status":          random.choice(ORDER_STATUSES),
        "payment_method":  random.choice(PAYMENT_METHODS),
        "customer_email":  fake.email(),
        "shipping_city":   fake.city(),
        "timestamp":       datetime.utcnow().isoformat(),
    }

def make_click_event() -> dict:
    return {
        "event_type":    "page_click",
        "session_id":    fake.uuid4(),
        "user_id":       fake.uuid4() if random.random() > 0.3 else None,  # 30 % anonymous
        "page":          random.choice(PAGES),
        "referrer":      random.choice(["google", "facebook", "direct", "instagram", None]),
        "device":        random.choice(DEVICES),
        "country":       fake.country_code(),
        "duration_secs": random.randint(3, 300),
        "timestamp":     datetime.utcnow().isoformat(),
    }

def make_signup_event() -> dict:
    return {
        "event_type":   "user_signup",
        "user_id":      fake.uuid4(),
        "email":        fake.email(),
        "full_name":    fake.name(),
        "country":      fake.country_code(),
        "signup_method": random.choice(["email", "google", "facebook"]),
        "timestamp":    datetime.utcnow().isoformat(),
    }

# --- Kafka Helpers
def build_producer() -> KafkaProducer:
    """Create a KafkaProducer, retrying until Kafka is reachable."""
    while True:
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                acks="all",            # wait for all replicas to acknowledge
                retries=3,
                linger_ms=10,          # small batching window
            )
            log.info("✅  Connected to Kafka at %s", KAFKA_BOOTSTRAP_SERVERS)
            return producer
        except NoBrokersAvailable:
            log.warning(
                "⚠️  Kafka not reachable yet. Retrying in %ss…",
                RETRY_DELAY_SECONDS,
            )
            time.sleep(RETRY_DELAY_SECONDS)


def publish(producer: KafkaProducer, topic: str, event: dict) -> None:
    """Send one event and log a confirmation."""
    future = producer.send(topic, value=event)
    future.add_errback(lambda exc: log.error("❌  Failed to send to %s: %s", topic, exc))
    log.info(
        "📨  [%-14s]  event_type=%-15s  id=%.8s…",
        topic,
        event["event_type"],
        event.get("order_id") or event.get("session_id") or event.get("user_id"),
    )

# --- Main Loop
def main() -> None:
    log.info("🚀  E-Commerce Event Producer starting…")
    producer = build_producer()

    # Weighted event mix: lots of clicks, some orders, occasional signups
    event_pool = (
        [(TOPIC_ORDERS,  make_order_event)]  * 3  +
        [(TOPIC_CLICKS,  make_click_event)]  * 6  +
        [(TOPIC_SIGNUPS, make_signup_event)] * 1
    )

    published = 0
    try:
        while True:
            topic, generator = random.choice(event_pool)
            publish(producer, topic, generator())
            published += 1

            if published % 50 == 0:
                log.info("📊  Total events published so far: %d", published)

            time.sleep(1 / EVENTS_PER_SECOND)

    except KeyboardInterrupt:
        log.info("🛑  Shutting down. Total events published: %d", published)
    finally:
        producer.flush()
        producer.close()
        log.info("👋  Producer closed cleanly.")


if __name__ == "__main__":
    main()
