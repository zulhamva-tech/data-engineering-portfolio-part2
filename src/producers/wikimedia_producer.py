"""
Project 10 — Wikimedia Trending Topics Producer
Sources:
  - Wikimedia Pageviews API → Kafka: wikimedia-pageviews (batch)
  - Wikimedia SSE Stream → Kafka: wikimedia-recentchanges (streaming)
Free, no API key required
"""

import json
import logging
import threading
from datetime import datetime, timezone, timedelta

import requests
import sseclient
from kafka import KafkaProducer
from kafka.errors import KafkaError

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_PAGEVIEWS = "wikimedia-pageviews"
TOPIC_CHANGES = "wikimedia-recentchanges"

WIKIMEDIA_SSE_URL = "https://stream.wikimedia.org/v2/stream/recentchange"
WIKIMEDIA_PAGEVIEWS_URL = "https://wikimedia.org/api/rest_v1/metrics/pageviews/top/{project}/all-access/{year}/{month}/{day}"

LANGUAGES = ["en", "id", "de", "fr", "es", "ja", "zh", "ar", "pt", "ru"]
TOP_N_ARTICLES = 50


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP,
        value_serializer=lambda v: json.dumps(v, default=str).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
        compression_type="gzip",
        retries=3,
        acks="all",
    )


def fetch_pageviews(lang: str, date: datetime.date) -> list:
    """Fetch top 50 articles for a given language wiki."""
    url = WIKIMEDIA_PAGEVIEWS_URL.format(
        project=f"{lang}.wikipedia",
        year=date.year,
        month=str(date.month).zfill(2),
        day=str(date.day).zfill(2),
    )
    try:
        resp = requests.get(url, timeout=15, headers={"User-Agent": "DataEngPortfolio/1.0"})
        resp.raise_for_status()
        items = resp.json().get("items", [{}])[0].get("articles", [])
        return items[:TOP_N_ARTICLES]
    except requests.RequestException as e:
        logger.error(f"Pageviews error [{lang}]: {e}")
        return []


def publish_pageviews(producer: KafkaProducer):
    """Batch: publish yesterday's top articles for all languages."""
    yesterday = (datetime.now().date() - timedelta(days=1))
    total = 0

    for lang in LANGUAGES:
        articles = fetch_pageviews(lang, yesterday)
        for rank, article in enumerate(articles, 1):
            record = {
                "language": lang,
                "article": article.get("article"),
                "views": article.get("views"),
                "rank": rank,
                "date": yesterday.isoformat(),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source": "wikimedia_pageviews_api",
            }
            try:
                key = f"{lang}_{yesterday.isoformat()}_{rank}"
                producer.send(TOPIC_PAGEVIEWS, key=key, value=record)
                total += 1
            except KafkaError as e:
                logger.error(f"Kafka error: {e}")
        logger.info(f"  {lang}.wikipedia: {len(articles)} articles queued")

    producer.flush()
    logger.info(f"Pageviews batch complete. Total: {total} records")


def stream_recent_changes(producer: KafkaProducer, max_events: int = 1000):
    """Streaming: consume SSE stream of Wikipedia edits."""
    logger.info(f"Connecting to Wikimedia SSE stream...")
    headers = {"User-Agent": "DataEngPortfolio/1.0"}

    try:
        resp = requests.get(WIKIMEDIA_SSE_URL, stream=True, headers=headers, timeout=30)
        client = sseclient.SSEClient(resp)
        sent = 0

        for event in client.events():
            if sent >= max_events:
                break
            if not event.data or event.data == "":
                continue
            try:
                data = json.loads(event.data)
            except json.JSONDecodeError:
                continue

            if data.get("type") != "edit":
                continue

            record = {
                "change_id": data.get("id"),
                "wiki": data.get("wiki"),
                "title": data.get("title", "")[:200],
                "namespace": data.get("namespace"),
                "user": data.get("user", ""),
                "is_bot": data.get("bot", False),
                "is_minor": data.get("minor", False),
                "old_length": data.get("length", {}).get("old"),
                "new_length": data.get("length", {}).get("new"),
                "comment": (data.get("comment") or "")[:200],
                "server_name": data.get("server_name"),
                "event_time": data.get("meta", {}).get("dt"),
                "ingested_at": datetime.now(timezone.utc).isoformat(),
                "source": "wikimedia_sse",
            }

            try:
                producer.send(TOPIC_CHANGES, key=str(record["change_id"] or sent), value=record)
                sent += 1
                if sent % 100 == 0:
                    producer.flush()
                    logger.info(f"SSE: {sent} edit events sent")
            except KafkaError as e:
                logger.error(f"Kafka error: {e}")

        producer.flush()
        logger.info(f"SSE stream complete. Total: {sent} events")

    except Exception as e:
        logger.error(f"SSE stream error: {e}")


def run():
    producer = create_producer()
    logger.info("=== Wikimedia producer started ===")

    # Run both in parallel
    pageviews_thread = threading.Thread(target=publish_pageviews, args=(producer,))
    sse_thread = threading.Thread(target=stream_recent_changes, args=(producer, 500))

    pageviews_thread.start()
    sse_thread.start()

    pageviews_thread.join()
    sse_thread.join()

    logger.info("=== Wikimedia producer complete ===")


if __name__ == "__main__":
    run()
