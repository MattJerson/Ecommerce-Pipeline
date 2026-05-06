import os
import json
import time
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

# --- Loggin Setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- Config
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_GROUP_ID          = os.getenv("KAFKA_GROUP_ID", "ecommerce-consumer-group")
TOPICS                  = ["orders", "page_clicks", "user_signups"]
RETRY_DELAY_SECONDS     = 5

DB_CONFIG = {
    "host":     "db.vjdfcormtplscencxggn.supabase.co",
    "port":     5432,
    "dbname":   "postgres",
    "user":     "postgres",
    "password": "databasepipeline0880",
    "sslmode":  "require",
}

# --- DB Bootstrap
CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS raw_orders (
    id              SERIAL PRIMARY KEY,
    order_id        TEXT,
    user_id         TEXT,
    product_id      TEXT,
    product_name    TEXT,
    category        TEXT,
    unit_price      NUMERIC(10, 2),
    quantity        INT,
    discount        NUMERIC(5, 2),
    total_amount    NUMERIC(10, 2),
    currency        TEXT,
    status          TEXT,
    payment_method  TEXT,
    customer_email  TEXT,
    shipping_city   TEXT,
    event_timestamp TIMESTAMP,
    ingested_at     TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_page_clicks (
    id             SERIAL PRIMARY KEY,
    session_id     TEXT,
    user_id        TEXT,
    page           TEXT,
    referrer       TEXT,
    device         TEXT,
    country        TEXT,
    duration_secs  INT,
    event_timestamp TIMESTAMP,
    ingested_at    TIMESTAMP DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_user_signups (
    id             SERIAL PRIMARY KEY,
    user_id        TEXT,
    email          TEXT,
    full_name      TEXT,
    country        TEXT,
    signup_method  TEXT,
    event_timestamp TIMESTAMP,
    ingested_at    TIMESTAMP DEFAULT NOW()
);
"""

# --- Insert Statements
INSERT_ORDER = """
INSERT INTO raw_orders (
    order_id, user_id, product_id, product_name, category,
    unit_price, quantity, discount, total_amount, currency,
    status, payment_method, customer_email, shipping_city, event_timestamp
) VALUES (
    %(order_id)s, %(user_id)s, %(product_id)s, %(product_name)s, %(category)s,
    %(unit_price)s, %(quantity)s, %(discount)s, %(total_amount)s, %(currency)s,
    %(status)s, %(payment_method)s, %(customer_email)s, %(shipping_city)s,
    %(event_timestamp)s
);
"""

INSERT_CLICK = """
INSERT INTO raw_page_clicks (
    session_id, user_id, page, referrer, device,
    country, duration_secs, event_timestamp
) VALUES (
    %(session_id)s, %(user_id)s, %(page)s, %(referrer)s, %(device)s,
    %(country)s, %(duration_secs)s, %(event_timestamp)s
);
"""

INSERT_SIGNUP = """
INSERT INTO raw_user_signups (
    user_id, email, full_name, country, signup_method, event_timestamp
) VALUES (
    %(user_id)s, %(email)s, %(full_name)s, %(country)s,
    %(signup_method)s, %(event_timestamp)s
);
"""

# --- DB Connection
def build_db_connection():
    while True:
        try:
            conn = psycopg2.connect(**DB_CONFIG)
            conn.autocommit = False
            log.info("✅  Connected to PostgreSQL at %s/%s", DB_CONFIG["host"], DB_CONFIG["dbname"])
            return conn
        except psycopg2.OperationalError as e:
            log.warning("⚠️  PostgreSQL not ready yet (%s). Retrying in %ss…", e, RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)


def bootstrap_tables(conn) -> None:
    with conn.cursor() as cur:
        cur.execute(CREATE_TABLES_SQL)
    conn.commit()
    log.info("✅  Raw tables verified / created.")

# --- Kafka Consumer
def build_consumer() -> KafkaConsumer:
    while True:
        try:
            consumer = KafkaConsumer(
                *TOPICS,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                group_id=KAFKA_GROUP_ID,
                auto_offset_reset="earliest",       # replay from start if new group
                enable_auto_commit=False,           # we commit manually after DB write
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            )
            log.info("✅  Subscribed to topics: %s", TOPICS)
            return consumer
        except NoBrokersAvailable:
            log.warning("⚠️  Kafka not reachable yet. Retrying in %ss…", RETRY_DELAY_SECONDS)
            time.sleep(RETRY_DELAY_SECONDS)

# --- Per-Topic Handlers
def handle_order(cur, event: dict) -> None:
    event["event_timestamp"] = event.pop("timestamp", datetime.utcnow().isoformat())
    cur.execute(INSERT_ORDER, event)


def handle_click(cur, event: dict) -> None:
    event["event_timestamp"] = event.pop("timestamp", datetime.utcnow().isoformat())
    cur.execute(INSERT_CLICK, event)


def handle_signup(cur, event: dict) -> None:
    event["event_timestamp"] = event.pop("timestamp", datetime.utcnow().isoformat())
    cur.execute(INSERT_SIGNUP, event)


HANDLERS = {
    "orders":       handle_order,
    "page_clicks":  handle_click,
    "user_signups": handle_signup,
}

# --- Main Consume Loop
def main() -> None:
    log.info("🚀  E-Commerce Consumer starting…")

    conn     = build_db_connection()
    bootstrap_tables(conn)
    consumer = build_consumer()

    consumed   = 0
    errors     = 0

    try:
        for message in consumer:
            topic  = message.topic
            event  = message.value
            handler = HANDLERS.get(topic)

            if not handler:
                log.warning("⚠️  No handler for topic '%s'. Skipping.", topic)
                consumer.commit()
                continue

            try:
                with conn.cursor() as cur:
                    handler(cur, event)
                conn.commit()
                consumer.commit()          # only commit offset after successful DB write
                consumed += 1

                log.info(
                    "💾  [%-14s]  event_type=%-15s  partition=%d  offset=%d",
                    topic,
                    event.get("event_type", "unknown"),
                    message.partition,
                    message.offset,
                )

                if consumed % 50 == 0:
                    log.info("📊  Total events consumed & saved: %d  |  errors: %d", consumed, errors)

            except (psycopg2.Error, KeyError) as e:
                conn.rollback()
                errors += 1
                log.error("❌  Failed to process message from '%s': %s", topic, e)
                log.error("    Raw event: %s", event)
                # don't commit offset — message stays in Kafka for retry

    except KeyboardInterrupt:
        log.info("🛑  Shutting down. Consumed: %d  |  Errors: %d", consumed, errors)
    finally:
        consumer.close()
        conn.close()
        log.info("👋  Consumer and DB connection closed cleanly.")


if __name__ == "__main__":
    main()