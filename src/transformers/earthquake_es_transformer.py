"""
Project 8 — USGS Earthquake Elasticsearch Transformer
Kafka: usgs-earthquakes → PySpark Streaming → Elasticsearch + Kibana
Features: geo_point mapping, geospatial queries, magnitude aggregations
"""

import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType, IntegerType
)
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC = "usgs-earthquakes"
ES_HOST = "localhost"
ES_PORT = 9200
ES_INDEX = "usgs_earthquakes"

QUAKE_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("magnitude", DoubleType()),
    StructField("place", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("depth_km", DoubleType()),
    StructField("depth_category", StringType()),
    StructField("tsunami_risk", BooleanType()),
    StructField("is_significant", BooleanType()),
    StructField("event_time", TimestampType()),
    StructField("status", StringType()),
    StructField("felt_reports", IntegerType()),
    StructField("alert_level", StringType()),
    StructField("ingested_at", TimestampType()),
])

ES_MAPPING = {
    "mappings": {
        "properties": {
            "event_id": {"type": "keyword"},
            "magnitude": {"type": "float"},
            "place": {"type": "text", "fields": {"keyword": {"type": "keyword"}}},
            "location": {"type": "geo_point"},
            "depth_km": {"type": "float"},
            "depth_category": {"type": "keyword"},
            "tsunami_risk": {"type": "boolean"},
            "is_significant": {"type": "boolean"},
            "event_time": {"type": "date"},
            "alert_level": {"type": "keyword"},
            "ingested_at": {"type": "date"},
        }
    }
}


def ensure_es_index(es: Elasticsearch):
    if not es.indices.exists(index=ES_INDEX):
        es.indices.create(index=ES_INDEX, body=ES_MAPPING)
        logger.info(f"Created Elasticsearch index: {ES_INDEX}")


def write_batch(batch_df, batch_id: int):
    if batch_df.count() == 0:
        return

    es = Elasticsearch([{"host": ES_HOST, "port": ES_PORT}])
    ensure_es_index(es)

    rows = batch_df.collect()
    actions = []
    for row in rows:
        doc = row.asDict()
        # Create geo_point from lat/lon
        if doc.get("latitude") and doc.get("longitude"):
            doc["location"] = {
                "lat": doc.pop("latitude"),
                "lon": doc.pop("longitude"),
            }
        else:
            doc.pop("latitude", None)
            doc.pop("longitude", None)

        actions.append({
            "_index": ES_INDEX,
            "_id": doc["event_id"],
            "_source": doc,
        })

    success, failed = helpers.bulk(es, actions, raise_on_error=False)
    logger.info(f"Batch {batch_id}: {success} indexed, {len(failed)} failed")


def main():
    spark = (
        SparkSession.builder
        .appName("EarthquakeElasticsearch")
        .config("spark.sql.streaming.checkpointLocation", "/tmp/checkpoints/earthquake")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    stream = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), QUAKE_SCHEMA).alias("d"))
        .select("d.*")
        .withColumn("event_time", F.to_timestamp("event_time"))
        .withWatermark("event_time", "10 minutes")
        .filter(F.col("event_id").isNotNull())
        .filter(F.col("magnitude") >= 1.0)
    )

    query = (
        stream.writeStream
        .foreachBatch(write_batch)
        .trigger(processingTime="30 seconds")
        .option("checkpointLocation", "/tmp/checkpoints/earthquake")
        .start()
    )

    logger.info("Earthquake Elasticsearch streaming started.")
    query.awaitTermination()


if __name__ == "__main__":
    main()
