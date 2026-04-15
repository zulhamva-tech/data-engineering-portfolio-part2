"""
Project 6 — NASA Space Data Producer
Sources: NASA APOD + NeoWs + DONKI APIs → Kafka topics
Free API: use DEMO_KEY or register at https://api.nasa.gov
"""

import json
import logging
from datetime import datetime, timezone, timedelta

import requests
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_APOD = "nasa-apod"
TOPIC_NEO = "nasa-neo-asteroids"
TOPIC_DONKI = "nasa-donki-spaceweather"

NASA_API_KEY = "DEMO_KEY"  # Replace with your key from api.nasa.gov
NASA_BASE = "https://api.nasa.gov"


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=3,
        acks="all",
    )


def fetch_apod(days_back: int = 30) -> list:
    """Fetch Astronomy Picture of the Day for last N days."""
    end = datetime.now().date()
    start = end - timedelta(days=days_back)
    url = f"{NASA_BASE}/planetary/apod"
    params = {
        "api_key": NASA_API_KEY,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json() if isinstance(resp.json(), list) else [resp.json()]
    except requests.RequestException as e:
        logger.error(f"APOD fetch error: {e}")
        return []


def fetch_neo(days_back: int = 7) -> dict:
    """Fetch Near Earth Objects for last 7 days."""
    end = datetime.now().date()
    start = end - timedelta(days=days_back)
    url = f"{NASA_BASE}/neo/rest/v1/feed"
    params = {
        "api_key": NASA_API_KEY,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
    }
    try:
        resp = requests.get(url, params=params, timeout=20)
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        logger.error(f"NeoWs fetch error: {e}")
        return {}


def fetch_donki(days_back: int = 30) -> list:
    """Fetch space weather events from DONKI."""
    end = datetime.now().date()
    start = end - timedelta(days=days_back)
    url = f"{NASA_BASE}/DONKI/CME"
    params = {
        "api_key": NASA_API_KEY,
        "startDate": start.isoformat(),
        "endDate": end.isoformat(),
    }
    try:
        resp = requests.get(url, params=params, timeout=15)
        resp.raise_for_status()
        return resp.json() or []
    except requests.RequestException as e:
        logger.error(f"DONKI fetch error: {e}")
        return []


def publish_apod(producer, items: list):
    sent = 0
    for item in items:
        record = {
            "date": item.get("date"),
            "title": item.get("title", "")[:200],
            "explanation": item.get("explanation", "")[:500],
            "media_type": item.get("media_type"),
            "url": item.get("url"),
            "hdurl": item.get("hdurl"),
            "copyright": item.get("copyright"),
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            producer.send(TOPIC_APOD, key=record["date"], value=record)
            sent += 1
        except KafkaError as e:
            logger.error(f"APOD Kafka error: {e}")
    producer.flush()
    logger.info(f"APOD: {sent} records sent")


def publish_neo(producer, data: dict):
    sent = 0
    near_objects = data.get("near_earth_objects", {})
    for date, objects in near_objects.items():
        for obj in objects:
            est_diam = obj.get("estimated_diameter", {}).get("kilometers", {})
            close_approach = obj.get("close_approach_data", [{}])[0]
            record = {
                "neo_id": obj.get("id"),
                "name": obj.get("name"),
                "close_approach_date": date,
                "is_potentially_hazardous": obj.get("is_potentially_hazardous_asteroid"),
                "diameter_min_km": est_diam.get("estimated_diameter_min"),
                "diameter_max_km": est_diam.get("estimated_diameter_max"),
                "miss_distance_km": float(close_approach.get("miss_distance", {}).get("kilometers", 0)),
                "relative_velocity_kmh": float(close_approach.get("relative_velocity", {}).get("kilometers_per_hour", 0)),
                "orbiting_body": close_approach.get("orbiting_body"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
            }
            # Classify size
            avg_diam = (record["diameter_min_km"] + record["diameter_max_km"]) / 2 if record["diameter_min_km"] else 0
            if avg_diam < 0.1:
                record["size_category"] = "small"
            elif avg_diam < 0.5:
                record["size_category"] = "medium"
            elif avg_diam < 1.0:
                record["size_category"] = "large"
            else:
                record["size_category"] = "very_large"

            try:
                producer.send(TOPIC_NEO, key=f"{record['neo_id']}_{date}", value=record)
                sent += 1
            except KafkaError as e:
                logger.error(f"NEO Kafka error: {e}")
    producer.flush()
    logger.info(f"NEO: {sent} asteroid records sent")


def publish_donki(producer, events: list):
    sent = 0
    for event in events:
        record = {
            "event_id": event.get("activityID", ""),
            "event_type": "CME",
            "start_time": event.get("startTime"),
            "source_location": event.get("sourceLocation"),
            "active_region": event.get("activeRegionNum"),
            "instruments": str(event.get("instruments", [])),
            "note": (event.get("note") or "")[:300],
            "ingested_at": datetime.now(timezone.utc).isoformat(),
        }
        try:
            producer.send(TOPIC_DONKI, key=record["event_id"] or str(sent), value=record)
            sent += 1
        except KafkaError as e:
            logger.error(f"DONKI Kafka error: {e}")
    producer.flush()
    logger.info(f"DONKI: {sent} space weather events sent")


def run():
    producer = create_producer()
    logger.info("=== NASA batch producer started ===")

    apod_data = fetch_apod()
    publish_apod(producer, apod_data)

    neo_data = fetch_neo()
    publish_neo(producer, neo_data)

    donki_data = fetch_donki()
    publish_donki(producer, donki_data)

    logger.info("=== NASA batch complete ===")


if __name__ == "__main__":
    run()
