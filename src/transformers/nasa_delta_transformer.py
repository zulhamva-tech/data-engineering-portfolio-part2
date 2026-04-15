"""
Project 6 — NASA Delta Lake PySpark Transformer
Kafka → PySpark → Delta Lake on S3 (ACID, time-travel, MERGE upsert) → BigQuery
"""

import logging
from pyspark.sql import SparkSession, functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    BooleanType, TimestampType, IntegerType
)
from delta.tables import DeltaTable

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

KAFKA_BOOTSTRAP = "localhost:9092"
TOPICS = ["nasa-apod", "nasa-neo-asteroids", "nasa-donki-spaceweather"]
DELTA_BASE = "s3a://your-bucket/delta/nasa"

NEO_SCHEMA = StructType([
    StructField("neo_id", StringType()),
    StructField("name", StringType()),
    StructField("close_approach_date", StringType()),
    StructField("is_potentially_hazardous", BooleanType()),
    StructField("diameter_min_km", DoubleType()),
    StructField("diameter_max_km", DoubleType()),
    StructField("miss_distance_km", DoubleType()),
    StructField("relative_velocity_kmh", DoubleType()),
    StructField("orbiting_body", StringType()),
    StructField("size_category", StringType()),
    StructField("ingested_at", TimestampType()),
])

APOD_SCHEMA = StructType([
    StructField("date", StringType()),
    StructField("title", StringType()),
    StructField("explanation", StringType()),
    StructField("media_type", StringType()),
    StructField("url", StringType()),
    StructField("copyright", StringType()),
    StructField("ingested_at", TimestampType()),
])


def merge_into_delta(spark, new_df, delta_path: str, merge_key: str):
    """MERGE upsert — no duplicates on daily reruns."""
    if DeltaTable.isDeltaTable(spark, delta_path):
        delta_table = DeltaTable.forPath(spark, delta_path)
        (
            delta_table.alias("existing")
            .merge(new_df.alias("new"), f"existing.{merge_key} = new.{merge_key}")
            .whenMatchedUpdateAll()
            .whenNotMatchedInsertAll()
            .execute()
        )
        logger.info(f"MERGE upsert complete → {delta_path}")
    else:
        new_df.write.format("delta").mode("overwrite").save(delta_path)
        logger.info(f"Delta table created → {delta_path}")


def read_kafka_batch(spark, topic: str, schema: StructType):
    return (
        spark.read
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("endingOffsets", "latest")
        .load()
        .select(F.from_json(F.col("value").cast("string"), schema).alias("d"))
        .select("d.*")
        .filter(F.col(schema.fields[0].name).isNotNull())
    )


def main():
    spark = (
        SparkSession.builder
        .appName("NASADeltaLake")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    # Process NEO asteroids
    logger.info("Processing NASA NEO asteroids...")
    neo_df = read_kafka_batch(spark, "nasa-neo-asteroids", NEO_SCHEMA)
    neo_enriched = (
        neo_df
        .withColumn("close_approach_date", F.to_date("close_approach_date"))
        .withColumn("avg_diameter_km", (F.col("diameter_min_km") + F.col("diameter_max_km")) / 2)
        .withColumn("batch_date", F.current_date())
    )
    merge_into_delta(spark, neo_enriched, f"{DELTA_BASE}/neo_asteroids", "neo_id")

    # Process APOD
    logger.info("Processing NASA APOD...")
    apod_df = read_kafka_batch(spark, "nasa-apod", APOD_SCHEMA)
    apod_enriched = apod_df.withColumn("apod_date", F.to_date("date")).withColumn("batch_date", F.current_date())
    merge_into_delta(spark, apod_enriched, f"{DELTA_BASE}/apod", "date")

    # Run VACUUM to clean old versions (keep 7 days)
    for path in [f"{DELTA_BASE}/neo_asteroids", f"{DELTA_BASE}/apod"]:
        if DeltaTable.isDeltaTable(spark, path):
            DeltaTable.forPath(spark, path).vacuum(168)  # 168 hours = 7 days

    # Summary stats
    hazardous_count = neo_enriched.filter(F.col("is_potentially_hazardous") == True).count()
    total_count = neo_enriched.count()
    logger.info(f"NEO: {total_count} total, {hazardous_count} potentially hazardous")
    logger.info(f"APOD: {apod_enriched.count()} records")

    spark.stop()
    logger.info("NASA Delta Lake pipeline complete.")


if __name__ == "__main__":
    main()
