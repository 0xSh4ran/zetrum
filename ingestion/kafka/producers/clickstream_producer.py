"""
zetrum/ingestion/kafka/producers/clickstream_producer.py

Zetrum Clickstream Producer
----------------------------
Simulates realistic e-commerce clickstream events and publishes them
to a Kafka topic using Avro serialization via Schema Registry.

Mimics events from:
  - Web browsers (desktop + mobile)
  - iOS / Android apps
  - Backend services
  - Event logs

Usage:
    # Install dependencies first:
    pip install confluent-kafka fastavro faker

    # Run with defaults (100 events, 0.5s interval):
    python clickstream_producer.py

    # Run with custom settings:
    python clickstream_producer.py --events 500 --interval 0.1 --topic zetrum.clickstream.raw

Author: Zetrum Contributors
License: Apache 2.0
"""

import argparse
import json
import logging
import random
import time
import uuid
from datetime import datetime, timezone

from faker import Faker
from confluent_kafka import Producer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import (
    SerializationContext,
    MessageField,
    StringSerializer,
)

# ──────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
SCHEMA_REGISTRY_URL = "http://localhost:8081"
DEFAULT_TOPIC = "zetrum.clickstream.raw"
DEFAULT_EVENTS = 100
DEFAULT_INTERVAL_SECONDS = 0.5

# ──────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s - %(message)s",
)
log = logging.getLogger("zetrum.producer")

# ──────────────────────────────────────────────
# SIMULATION DATA
# ──────────────────────────────────────────────
fake = Faker(["en_IN", "en_US", "en_GB"])  # India-first locale

PRODUCTS = [
    {"id": "P001", "name": "Running Shoes Pro", "category": "Footwear",    "price": 2499.00},
    {"id": "P002", "name": "Wireless Earbuds",  "category": "Electronics", "price": 1999.00},
    {"id": "P003", "name": "Yoga Mat Premium",  "category": "Sports",      "price": 899.00},
    {"id": "P004", "name": "Cotton Kurta",       "category": "Clothing",    "price": 599.00},
    {"id": "P005", "name": "Stainless Bottle",   "category": "Kitchen",     "price": 449.00},
    {"id": "P006", "name": "Mechanical Keyboard","category": "Electronics", "price": 3499.00},
    {"id": "P007", "name": "Desk Lamp LED",      "category": "Home",        "price": 799.00},
    {"id": "P008", "name": "Whey Protein 1kg",   "category": "Health",      "price": 1299.00},
]

PAGE_PATHS = [
    "/", "/products", "/products/footwear", "/products/electronics",
    "/cart", "/checkout", "/account", "/orders", "/search",
    "/products/detail", "/deals", "/wishlist",
]

BROWSERS = [
    ("Chrome", "122.0"), ("Firefox", "124.0"), ("Safari", "17.4"),
    ("Edge", "122.0"), ("Opera", "109.0"),
]

OS_LIST = [
    "Windows 11", "Windows 10", "macOS 14 Sonoma",
    "Ubuntu 22.04", "Android 14", "iOS 17.4",
]

TIMEZONES = [
    "Asia/Kolkata", "America/New_York", "Europe/London",
    "Asia/Singapore", "Australia/Sydney",
]

GEO_DATA = [
    # India
    {"country": "IN", "region": "Karnataka",   "city": "Bengaluru", "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Maharashtra", "city": "Mumbai",    "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Delhi",       "city": "New Delhi", "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Tamil Nadu",  "city": "Chennai",   "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Telangana",   "city": "Hyderabad", "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "West Bengal", "city": "Kolkata",   "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Gujarat",     "city": "Ahmedabad", "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Rajasthan",   "city": "Jaipur",    "tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Punjab",      "city": "Chandigarh","tz": "Asia/Kolkata"},
    {"country": "IN", "region": "Kerala",      "city": "Kochi",     "tz": "Asia/Kolkata"},
    # USA
    {"country": "US", "region": "California",  "city": "San Jose",      "tz": "America/Los_Angeles"},
    {"country": "US", "region": "California",  "city": "San Francisco", "tz": "America/Los_Angeles"},
    {"country": "US", "region": "New York",    "city": "New York City", "tz": "America/New_York"},
    {"country": "US", "region": "Washington",  "city": "Seattle",       "tz": "America/Los_Angeles"},
    {"country": "US", "region": "Texas",       "city": "Austin",        "tz": "America/Chicago"},
    # Europe
    {"country": "GB", "region": "England",     "city": "London",    "tz": "Europe/London"},
    {"country": "DE", "region": "Bavaria",     "city": "Munich",    "tz": "Europe/Berlin"},
    {"country": "NL", "region": "North Holland","city": "Amsterdam", "tz": "Europe/Amsterdam"},
    # Asia Pacific
    {"country": "SG", "region": "Singapore",   "city": "Singapore", "tz": "Asia/Singapore"},
    {"country": "JP", "region": "Tokyo",       "city": "Tokyo",     "tz": "Asia/Tokyo"},
    {"country": "AU", "region": "New South Wales","city": "Sydney",  "tz": "Australia/Sydney"},
    {"country": "AE", "region": "Dubai",       "city": "Dubai",     "tz": "Asia/Dubai"},
    # Canada
    {"country": "CA", "region": "Ontario",     "city": "Toronto",   "tz": "America/Toronto"},
]

# Weighted event type distribution — mirrors real-world traffic
EVENT_WEIGHTS = {
    "page_view":        35,
    "click":            25,
    "scroll":           15,
    "search":            8,
    "add_to_cart":       5,
    "remove_from_cart":  2,
    "checkout_start":    3,
    "checkout_complete": 2,
    "login":             2,
    "logout":            1,
    "app_open":          1,
    "app_close":         1,
    "error":             1,
}

EVENT_TYPES = list(EVENT_WEIGHTS.keys())
EVENT_PROBS  = [w / sum(EVENT_WEIGHTS.values()) for w in EVENT_WEIGHTS.values()]


# ──────────────────────────────────────────────
# EVENT GENERATOR
# ──────────────────────────────────────────────

class ClickstreamEventGenerator:
    """Generates realistic clickstream events."""

    def __init__(self):
        # Pool of pre-generated user & session IDs for realism
        self._users = [str(uuid.uuid4()) for _ in range(200)]
        self._sessions = {}

    def _get_session(self, anon_id: str) -> str:
        """Returns existing session or creates new one (simulates session expiry)."""
        if anon_id not in self._sessions or random.random() < 0.05:
            self._sessions[anon_id] = str(uuid.uuid4())
        return self._sessions[anon_id]

    def _pick_device(self):
        device_type = random.choices(
            ["desktop", "mobile", "tablet"],
            weights=[50, 40, 10]
        )[0]

        if device_type == "mobile":
            os = random.choice(["Android 14", "Android 13", "iOS 17.4", "iOS 16.7"])
            source = random.choice(["ios", "android"])
            browser = None
            browser_version = None
            app_version = f"{random.randint(1,3)}.{random.randint(0,9)}.{random.randint(0,9)}"
        else:
            os = random.choice(["Windows 11", "Windows 10", "macOS 14 Sonoma", "Ubuntu 22.04"])
            browser, browser_version = random.choice(BROWSERS)
            source = "web"
            app_version = None

        return {
            "device_type": device_type,
            "os": os,
            "browser": browser,
            "browser_version": browser_version,
            "app_version": app_version,
            "screen_resolution": random.choice(["1920x1080", "1366x768", "390x844", "1280x800"]),
        }, source

    def _pick_geo(self):
        g = random.choice(GEO_DATA)
        return {
            "country": g["country"],
            "region": g["region"],
            "city": g["city"],
            "timezone": g["tz"],
            "ip_address": fake.sha256()[:16],   # hashed IP, never raw
        }

    def _build_properties(self, event_type: str):
        product = random.choice(PRODUCTS)
        props = {
            "product_id": None,
            "product_name": None,
            "category": None,
            "price": None,
            "search_query": None,
            "scroll_depth_pct": None,
            "click_element": None,
            "error_message": None,
            "order_value": None,
        }

        if event_type in ("click", "add_to_cart", "remove_from_cart"):
            props.update({
                "product_id": product["id"],
                "product_name": product["name"],
                "category": product["category"],
                "price": product["price"],
                "click_element": f"#product-card-{product['id']}",
            })
        elif event_type == "checkout_complete":
            n_items = random.randint(1, 4)
            props["order_value"] = round(sum(
                random.choice(PRODUCTS)["price"] for _ in range(n_items)
            ), 2)
        elif event_type == "search":
            props["search_query"] = fake.word() + " " + random.choice(
                ["shoes", "phone", "shirt", "laptop", "bottle", "book", "watch"]
            )
        elif event_type == "scroll":
            props["scroll_depth_pct"] = random.choice([25, 50, 75, 90, 100])
        elif event_type == "error":
            props["error_message"] = random.choice([
                "Network timeout on /api/cart",
                "Failed to load product images",
                "Payment gateway unreachable",
                "Session expired",
                "500 Internal Server Error on /checkout",
            ])

        return props

    def generate(self) -> dict:
        """Generate a single realistic clickstream event."""
        now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)

        # Slight jitter to simulate client-side event lag (up to 3s)
        event_ts = now_ms - random.randint(0, 3000)

        anon_id = random.choice(self._users)
        session_id = self._get_session(anon_id)

        # 70% of events are from authenticated users
        user_id = anon_id if random.random() < 0.7 else None

        event_type = random.choices(EVENT_TYPES, weights=EVENT_PROBS)[0]
        device, source = self._pick_device()
        path = random.choice(PAGE_PATHS)

        return {
            "event_id": str(uuid.uuid4()),
            "event_type": event_type,
            "event_timestamp": event_ts,
            "ingestion_timestamp": now_ms,
            "user_id": user_id,
            "session_id": session_id,
            "anonymous_id": anon_id,
            "device": device,
            "geo": self._pick_geo(),
            "page": {
                "url": f"https://shop.zetrum.io{path}",
                "path": path,
                "referrer": f"https://shop.zetrum.io{random.choice(PAGE_PATHS)}" if random.random() > 0.3 else None,
                "title": path.replace("/", " ").strip().title() or "Home",
            },
            "properties": self._build_properties(event_type),
            "source": source,
            "schema_version": "1.0.0",
        }


# ──────────────────────────────────────────────
# KAFKA PRODUCER
# ──────────────────────────────────────────────

def load_avro_schema(schema_path: str) -> str:
    """Load Avro schema from .avsc file."""
    with open(schema_path, "r") as f:
        return json.dumps(json.load(f))


def delivery_report(err, msg):
    """Callback fired when a message is delivered or fails."""
    if err:
        log.error(f"Delivery FAILED | topic={msg.topic()} | error={err}")
    else:
        log.debug(
            f"Delivered | topic={msg.topic()} | "
            f"partition={msg.partition()} | offset={msg.offset()}"
        )


def build_producer(schema_str: str):
    """Build and return a configured Avro Kafka producer."""
    schema_registry_conf = {"url": SCHEMA_REGISTRY_URL}
    schema_registry_client = SchemaRegistryClient(schema_registry_conf)

    avro_serializer = AvroSerializer(
        schema_registry_client,
        schema_str,
        lambda obj, ctx: obj,    # Pass dict directly
    )

    string_serializer = StringSerializer("utf_8")

    producer_conf = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "acks": "all",                  # Wait for all replicas (safer)
        "retries": 3,
        "retry.backoff.ms": 500,
        "compression.type": "snappy",   # Compress for efficiency
    }

    return (
        Producer(producer_conf),
        avro_serializer,
        string_serializer,
    )


# ──────────────────────────────────────────────
# MAIN
# ──────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Zetrum Clickstream Kafka Producer")
    parser.add_argument("--topic",    default=DEFAULT_TOPIC,            help="Kafka topic name")
    parser.add_argument("--events",   default=DEFAULT_EVENTS, type=int, help="Number of events to produce (0 = infinite)")
    parser.add_argument("--interval", default=DEFAULT_INTERVAL_SECONDS, type=float, help="Seconds between events")
    parser.add_argument("--schema",   default="ingestion/kafka/schemas/clickstream.avsc", help="Path to Avro schema file")
    parser.add_argument("--verbose",  action="store_true",              help="Enable debug logging")
    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    log.info("=" * 60)
    log.info("Zetrum Clickstream Producer starting")
    log.info(f"  Topic:    {args.topic}")
    log.info(f"  Events:   {'infinite' if args.events == 0 else args.events}")
    log.info(f"  Interval: {args.interval}s")
    log.info(f"  Kafka:    {KAFKA_BOOTSTRAP_SERVERS}")
    log.info(f"  Registry: {SCHEMA_REGISTRY_URL}")
    log.info("=" * 60)

    schema_str = load_avro_schema(args.schema)
    producer, avro_serializer, string_serializer = build_producer(schema_str)
    generator = ClickstreamEventGenerator()

    produced = 0
    errors   = 0

    try:
        while args.events == 0 or produced < args.events:
            event = generator.generate()

            try:
                producer.produce(
                    topic=args.topic,
                    key=string_serializer(
                        event["session_id"],
                        SerializationContext(args.topic, MessageField.KEY),
                    ),
                    value=avro_serializer(
                        event,
                        SerializationContext(args.topic, MessageField.VALUE),
                    ),
                    on_delivery=delivery_report,
                )

                produced += 1

                # Log a summary line every 10 events
                if produced % 10 == 0:
                    log.info(
                        f"Produced {produced} events | "
                        f"last={event['event_type']} | "
                        f"user={event.get('user_id', 'anon')[:8] if event.get('user_id') else 'anon'}... | "
                        f"city={event['geo']['city']}"
                    )

                # Flush every 100 events so delivery callbacks fire
                if produced % 100 == 0:
                    producer.flush(timeout=10)

            except Exception as e:
                errors += 1
                log.error(f"Failed to produce event: {e}")

            time.sleep(args.interval)

    except KeyboardInterrupt:
        log.info("Interrupted by user — flushing remaining messages...")

    finally:
        producer.flush(timeout=30)
        log.info("=" * 60)
        log.info(f"Done. Produced={produced} | Errors={errors}")
        log.info("=" * 60)


if __name__ == "__main__":
    main()
