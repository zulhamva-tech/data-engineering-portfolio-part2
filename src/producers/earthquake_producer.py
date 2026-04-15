"""
Project 8 — USGS Earthquake Monitor Producer
Source: USGS Earthquake Hazards API → Kafka topic: usgs-earthquakes
Free, no API key, GeoJSON format, updated every 5 minutes
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
TOPIC = "usgs-earthquakes"
USGS_URL = "https://earthquake.usgs.gov/fdsnws/event/1/query"
POLL_INTERVAL_MIN = 30


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=3,
        acks="all",
    )


def fetch_earthquakes(min_magnitude: float = 1.0, hours_back: int = 1) -> list:
    end_time = datetime.now(timezone.utc)
    start_time = end_time - timedelta(hours=hours_back)
    params = {
        "format": "geojson",
        "starttime": start_time.isoformat(),
        "endtime": end_time.isoformat(),
        "minmagnitude": min_magnitude,
        "orderby": "time",
        "limit": 1000,
    }
    try:
        resp = requests.get(USGS_URL, params=params, timeout=20)
        resp.raise_for_status()
        data = resp.json()
        return data.get("features", [])
    except requests.RequestException as e:
        logger.error(f"USGS API error: {e}")
        return []


def classify_depth(depth_km: float) -> str:
    if depth_km <= 70:
        return "shallow"
    elif depth_km <= 300:
        return "intermediate"
    return "deep"


def assess_tsunami_risk(magnitude: float, depth_km: float, place: str) -> bool:
    """Simple heuristic: high mag + shallow + coastal."""
    is_coastal = any(kw in (place or "").lower() for kw in ["ocean", "sea", "coast", "pacific", "atlantic", "indian"])
    return magnitude >= 6.5 and depth_km <= 70 and is_coastal


def build_record(feature: dict) -> dict:
    props = feature.get("properties", {})
    coords = feature.get("geometry", {}).get("coordinates", [None, None, None])
    lon, lat, depth = coords[0], coords[1], coords[2]

    magnitude = props.get("mag") or 0
    depth_km = depth or 0
    place = props.get("place") or ""
    event_time_ms = props.get("time") or 0

    return {
        "event_id": feature.get("id"),
        "magnitude": magnitude,
        "place": place[:200],
        "latitude": lat,
        "longitude": lon,
        "depth_km": depth_km,
        "depth_category": classify_depth(depth_km),
        "tsunami_risk": assess_tsunami_risk(magnitude, depth_km, place),
        "is_significant": magnitude >= 4.5,
        "event_time": datetime.fromtimestamp(event_time_ms / 1000, tz=timezone.utc).isoformat() if event_time_ms else None,
        "updated_time": datetime.fromtimestamp((props.get("updated") or 0) / 1000, tz=timezone.utc).isoformat(),
        "status": props.get("status"),
        "felt_reports": props.get("felt"),
        "alert_level": props.get("alert"),
        "url": props.get("url"),
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "source": "usgs_fdsnws",
    }


def run():
    producer = create_producer()
    logger.info("USGS Earthquake producer started...")

    earthquakes = fetch_earthquakes(min_magnitude=1.0, hours_back=1)
    if not earthquakes:
        logger.info("No earthquakes in last hour.")
        return

    sent = 0
    for feature in earthquakes:
        record = build_record(feature)
        if not record["event_id"]:
            continue
        try:
            producer.send(TOPIC, key=record["event_id"], value=record)
            sent += 1
        except KafkaError as e:
            logger.error(f"Kafka error: {e}")

    producer.flush()
    logger.info(f"USGS: {sent} earthquake events sent → [{TOPIC}]")


if __name__ == "__main__":
    run()
