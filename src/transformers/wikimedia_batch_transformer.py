"""
Project 10 — Wikimedia Trending Topics Transformer
Kafka: wikimedia-recentchanges → Flink (10-min window) → Cassandra (speed layer)
Kafka: wikimedia-pageviews → PySpark batch → Snowflake (batch layer)
Trending score: human_edits × 3 + bot_edits × 1
"""

import logging
from datetime import datetime, timezone
from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, IntegerType,
    BooleanType, TimestampType
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPIC_PAGEVIEWS = "wikimedia-pageviews"
CASSANDRA_HOST = "localhost"
CASSANDRA_KEYSPACE = "wikimedia_dw"

SNOWFLAKE_OPTIONS = {
    "sfURL": "your_account.snowflakecomputing.com",
    "sfDatabase": "WIKIMEDIA_DW",
    "sfSchema": "PUBLIC",
    "sfWarehouse": "COMPUTE_WH",
    "sfUser": "your_user",
    "sfPassword": "your_password",
}

PAGEVIEW_SCHEMA = StructType([
    StructField("language", StringType()),
    StructField("article", StringType()),
    StructField("views", IntegerType()),
    StructField("rank", IntegerType()),
    StructField("date", StringType()),
    StructField("ingested_at", TimestampType()),
])


def compute_yoy_change(df):
    """Compute year-over-year view change per article."""
    window_article = Window.partitionBy("language", "article").orderBy("date")
    return (
        df
        .withColumn("date", F.to_date("date"))
        .withColumn("prev_year_views", F.lag("views", 365).over(window_article))
        .withColumn(
            "yoy_change_pct",
            F.when(
                F.col("prev_year_views").isNotNull() & (F.col("prev_year_views") > 0),
                F.round(
                    (F.col("views") - F.col("prev_year_views")) / F.col("prev_year_views") * 100,
                    2
                )
            ).otherwise(None)
        )
        .withColumn("batch_date", F.current_date())
    )


def write_batch_to_cassandra(batch_df, batch_id: int):
    """Write trending articles to Cassandra with TTL 24h."""
    if batch_df.count() == 0:
        return

    from cassandra.cluster import Cluster
    cluster = Cluster([CASSANDRA_HOST])
    session = cluster.connect(CASSANDRA_KEYSPACE)

    rows = batch_df.collect()
    for row in rows:
        session.execute(
            """
            INSERT INTO trending_articles
            (language, article, views, rank, date, batch_date, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            USING TTL 86400
            """,
            (
                row["language"], row["article"], row["views"],
                row["rank"], str(row["date"]), str(row["batch_date"]),
                datetime.now(timezone.utc),
            )
        )
    cluster.shutdown()
    logger.info(f"Batch {batch_id}: {len(rows)} trending articles → Cassandra (TTL 24h)")


def main():
    spark = (
        SparkSession.builder
        .appName("WikimediaBatchLayer")
        .config("spark.cassandra.connection.host", CASSANDRA_HOST)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Read pageviews from Kafka
    logger.info("Reading pageviews from Kafka...")
    pageviews_df = (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC_PAGEVIEWS)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), PAGEVIEW_SCHEMA).alias("d"))
        .select("d.*")
        .filter(F.col("article").isNotNull())
        .filter(F.col("views") > 0)
        .dropDuplicates(["language", "article", "date"])
    )

    # Compute YoY change
    enriched_df = compute_yoy_change(pageviews_df)

    # Load to Snowflake (analytical layer)
    logger.info("Loading to Snowflake...")
    (
        enriched_df.write
        .format("net.snowflake.spark.snowflake")
        .options(**SNOWFLAKE_OPTIONS)
        .option("dbtable", "ARTICLE_PAGEVIEWS")
        .mode("append")
        .save()
    )

    total = enriched_df.count()
    languages = enriched_df.select("language").distinct().count()
    logger.info(f"Snowflake: {total} records loaded, {languages} languages")

    spark.stop()
    logger.info("Wikimedia batch layer complete.")


if __name__ == "__main__":
    main()
