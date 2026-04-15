"""
Project 7 — NYC Yellow Taxi Producer
Source: NYC TLC Socrata API → Kafka topic: nyc-taxi-trips
Free, no API key required
"""

import json
import logging
from datetime import datetime, timezone

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "nyc-taxi-trips"
SOCRATA_URL = "https://data.cityofnewyork.us/resource/gkne-dk5s.json"
BATCH_SIZE = 2000


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=3,
        acks="all",
    )


def fetch_trips(limit: int = BATCH_SIZE, offset: int = 0) -> list:
    params = {
        "$limit": limit,
        "$offset": offset,
        "$order": "tpep_pickup_datetime DESC",
        "$where": "trip_distance > 0 AND fare_amount > 0",
    }
    try:
        resp = requests.get(SOCRATA_URL, params=params, timeout=30)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"Socrata API error: {e}")
        return []


def build_record(trip: dict) -> dict:
    fare = float(trip.get("fare_amount") or 0)
    tip = float(trip.get("tip_amount") or 0)
    total = float(trip.get("total_amount") or 0)
    distance = float(trip.get("trip_distance") or 0)

    return {
        "trip_id": trip.get("vendorid", "") + "_" + str(trip.get("tpep_pickup_datetime", "")),
        "vendor_id": trip.get("vendorid"),
        "pickup_datetime": trip.get("tpep_pickup_datetime"),
        "dropoff_datetime": trip.get("tpep_dropoff_datetime"),
        "passenger_count": int(trip.get("passenger_count") or 0),
        "trip_distance_miles": distance,
        "pickup_location_id": trip.get("pulocationid"),
        "dropoff_location_id": trip.get("dolocationid"),
        "payment_type": trip.get("payment_type"),
        "fare_amount_usd": fare,
        "tip_amount_usd": tip,
        "total_amount_usd": total,
        "tip_rate_pct": round(tip / fare * 100, 2) if fare > 0 else 0,
        "congestion_surcharge": float(trip.get("congestion_surcharge") or 0),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "nyc_tlc_socrata",
    }


def run():
    producer = create_producer()
    logger.info("NYC Taxi producer started...")

    trips = fetch_trips()
    if not trips:
        logger.warning("No trips fetched.")
        return

    sent = 0
    for trip in trips:
        record = build_record(trip)
        try:
            producer.send(TOPIC, key=record["trip_id"], value=record)
            sent += 1
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")

    producer.flush()
    logger.info(f"NYC Taxi: {sent} trips sent to [{TOPIC}]")


if __name__ == "__main__":
    run()
