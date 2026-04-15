"""
Project 9 — IMF World Economic Outlook ELT Pipeline
Source: IMF DataMapper API → S3 raw zone → DuckDB transform → BigQuery
Free, no API key required
"""

import json
import logging
from datetime import datetime, timezone

import requests
import boto3
import duckdb
import pandas as pd
from google.cloud import bigquery

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
logger = logging.getLogger(__name__)

IMF_BASE = "https://www.imf.org/external/datamapper/api/v1"
S3_BUCKET = "your-bucket"
S3_PREFIX = "imf/raw/indicators"
BQ_DATASET = "imf_economic_analytics"
BQ_TABLE = "weo_enriched"

WEO_INDICATORS = {
    "NGDP_RPCH": "gdp_growth_pct",
    "PCPIPCH": "inflation_pct",
    "LUR": "unemployment_pct",
    "GGXWDG_NGDP": "govt_debt_pct_gdp",
    "BCA_NGDPD": "current_account_pct_gdp",
    "NGDPDPC": "gdp_per_capita_usd",
    "LP": "population_millions",
    "NGSD_NGDP": "gross_savings_pct_gdp",
}

INCOME_GROUPS = {
    "US": "high", "DE": "high", "JP": "high", "GB": "high", "FR": "high",
    "CN": "upper_middle", "BR": "upper_middle", "MX": "upper_middle",
    "IN": "lower_middle", "ID": "lower_middle", "PH": "lower_middle",
    "NG": "low", "ET": "low", "CD": "low",
}


def fetch_indicator_data(indicator_code: str) -> dict:
    url = f"{IMF_BASE}/data/{indicator_code}"
    try:
        resp = requests.get(url, timeout=30)
        resp.raise_for_status()
        return resp.json().get("values", {}).get(indicator_code, {})
    except requests.RequestException as e:
        logger.error(f"IMF API error [{indicator_code}]: {e}")
        return {}


def extract_to_s3(records: list, batch_date: str) -> str:
    """Save raw data to S3 as Parquet — immutable raw zone."""
    df = pd.DataFrame(records)
    s3_key = f"{S3_PREFIX}/dt={batch_date}/weo_raw.parquet"
    local_path = f"/tmp/weo_raw_{batch_date}.parquet"
    df.to_parquet(local_path, index=False)

    s3 = boto3.client("s3")
    s3.upload_file(local_path, S3_BUCKET, s3_key)
    s3_path = f"s3://{S3_BUCKET}/{s3_key}"
    logger.info(f"Raw data saved to {s3_path}: {len(records)} records")
    return s3_path


def transform_with_duckdb(s3_path: str) -> pd.DataFrame:
    """ELT transform: pivot, compute health score, classify income group."""
    con = duckdb.connect()

    # Install and load httpfs for S3 access
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute(f"SET s3_region='ap-southeast-1';")

    # Read from S3 and pivot long→wide
    pivot_query = f"""
    SELECT
        country_code,
        year,
        MAX(CASE WHEN indicator_name = 'gdp_growth_pct' THEN value END)        AS gdp_growth_pct,
        MAX(CASE WHEN indicator_name = 'inflation_pct' THEN value END)          AS inflation_pct,
        MAX(CASE WHEN indicator_name = 'unemployment_pct' THEN value END)       AS unemployment_pct,
        MAX(CASE WHEN indicator_name = 'govt_debt_pct_gdp' THEN value END)      AS govt_debt_pct_gdp,
        MAX(CASE WHEN indicator_name = 'current_account_pct_gdp' THEN value END) AS current_account_pct_gdp,
        MAX(CASE WHEN indicator_name = 'gdp_per_capita_usd' THEN value END)     AS gdp_per_capita_usd,
        MAX(CASE WHEN indicator_name = 'population_millions' THEN value END)    AS population_millions,
        MAX(CASE WHEN indicator_name = 'gross_savings_pct_gdp' THEN value END)  AS gross_savings_pct_gdp,
    FROM read_parquet('{s3_path}')
    GROUP BY country_code, year
    HAVING gdp_growth_pct IS NOT NULL
    ORDER BY country_code, year
    """

    df = con.execute(pivot_query).df()

    # Compute economic health score
    df["economic_health_score"] = (
        df["gdp_growth_pct"].fillna(0) * 0.4
        - df["inflation_pct"].fillna(0) * 0.3
        - df["unemployment_pct"].fillna(0) * 0.3
    ).round(4)

    # Classify income group
    df["income_group"] = df["country_code"].map(INCOME_GROUPS).fillna("unknown")
    df["batch_date"] = datetime.now().date().isoformat()

    con.close()
    logger.info(f"DuckDB transform complete: {len(df)} rows")
    return df


def load_to_bigquery(df: pd.DataFrame):
    """Idempotent load: DELETE + INSERT by batch_date."""
    client = bigquery.Client()
    table_id = f"{BQ_DATASET}.{BQ_TABLE}"

    batch_date = df["batch_date"].iloc[0]
    client.query(f"DELETE FROM `{table_id}` WHERE batch_date = '{batch_date}'").result()

    job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
    client.load_table_from_dataframe(df, table_id, job_config=job_config).result()
    logger.info(f"BigQuery load complete: {len(df)} rows → {table_id}")


def run():
    logger.info("=== IMF ELT pipeline started ===")
    batch_date = datetime.now().date().isoformat()

    # Extract
    all_records = []
    for code, name in WEO_INDICATORS.items():
        data = fetch_indicator_data(code)
        for country, years in data.items():
            for year, value in years.items():
                if value is not None:
                    all_records.append({
                        "country_code": country,
                        "indicator_code": code,
                        "indicator_name": name,
                        "year": int(year),
                        "value": float(value),
                        "ingested_at": datetime.now(timezone.utc).isoformat(),
                    })
        logger.info(f"  {code} ({name}): extracted")

    logger.info(f"Total raw records: {len(all_records)}")

    # Load raw to S3
    s3_path = extract_to_s3(all_records, batch_date)

    # Transform with DuckDB
    df_enriched = transform_with_duckdb(s3_path)

    # Load to BigQuery
    load_to_bigquery(df_enriched)

    logger.info("=== IMF ELT pipeline complete ===")


if __name__ == "__main__":
    run()
