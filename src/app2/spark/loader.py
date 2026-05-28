
from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from typing import Dict

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType


def create_spark_session(app_name: str = "ETL-Validation", master_url: str = "spark://localhost:7077") -> SparkSession:
    return (
        SparkSession.builder
        .appName(app_name)
        .master(master_url)
        .config("spark.hadoop.fs.s3a.endpoint", "http://localhost:9000")
        .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
        .config("spark.hadoop.fs.s3a.path.style.access", "true")
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .getOrCreate()
    )


SCHEMA_AREA = StructType([
    StructField("id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("flag", StringType(), True),
])

SCHEMA_COMPETITION = StructType([
    StructField("id", IntegerType(), False),
    StructField("area_id", IntegerType(), False),
    StructField("name", StringType(), True),
    StructField("code", StringType(), True),
    StructField("type", StringType(), True),
    StructField("plan", StringType(), True),
])

SCHEMA_TEAM = StructType([
    StructField("id", IntegerType(), False),
    StructField("area_id", IntegerType(), True),
    StructField("name", StringType(), True),
    StructField("shortName", StringType(), True),
    StructField("tla", StringType(), True),
    StructField("crest", StringType(), True),
    StructField("venue", StringType(), True),
    StructField("address", StringType(), True),
])

SCHEMA_MATCH = StructType([
    StructField("id", IntegerType(), False),
    StructField("competition_id", IntegerType(), False),
    StructField("season_id", IntegerType(), False),
    StructField("utcDate", TimestampType(), True),
    StructField("status", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("matchday", IntegerType(), True),
    StructField("home_team_id", IntegerType(), True),
    StructField("away_team_id", IntegerType(), True),
])

SCHEMA_STANDING = StructType([
    StructField("season_id", IntegerType(), False),
    StructField("competition_id", IntegerType(), False),
    StructField("team_id", IntegerType(), False),
    StructField("type", StringType(), True),
    StructField("stage", StringType(), True),
    StructField("position", IntegerType(), True),
    StructField("playedGames", IntegerType(), True),
    StructField("won", IntegerType(), True),
    StructField("draw", IntegerType(), True),
    StructField("lost", IntegerType(), True),
    StructField("goalsFor", IntegerType(), True),
    StructField("goalsAgainst", IntegerType(), True),
    StructField("goalDifference", IntegerType(), True),
    StructField("points", IntegerType(), True),
])


def load_batches_to_spark(spark: SparkSession, input_dir: Path, database: str = "etl_kio") -> Dict[str, int]:

    batches = sorted(input_dir.glob("batch_*"))
    if not batches:
        batches = [input_dir]

    stats = {"areas": 0, "competitions": 0, "teams": 0, "matches": 0, "standings": 0}
    
    for batch_dir in batches:
        payload_dir = batch_dir / "payloads"
        if not payload_dir.exists():
            continue
        
        for payload_file in sorted(payload_dir.glob("*.json")):
            with open(payload_file, "r", encoding="utf-8") as f:
                payload = json.load(f)
            
            endpoint = payload.get("endpoint", "")
            data = payload.get("response_json", {})
            
            if endpoint == "areas":
                df = spark.createDataFrame(data.get("areas", []), schema=SCHEMA_AREA)
                df.write.mode("append").saveAsTable(f"{database}.areas")
                stats["areas"] += df.count()
            elif endpoint == "competitions":
                rows = []
                for comp in data.get("competitions", []):
                    rows.append((
                        comp["id"],
                        comp["area"]["id"],
                        comp.get("name"),
                        comp.get("code"),
                        comp.get("type"),
                        comp.get("plan", None)
                    ))
                if rows:
                    df = spark.createDataFrame(rows, schema=SCHEMA_COMPETITION)
                    df.write.mode("append").saveAsTable(f"{database}.competitions")
                    stats["competitions"] += df.count()
            elif "teams" in endpoint:
                rows = []
                for team in data.get("teams", []):
                    rows.append((
                        team["id"],
                        team.get("area", {}).get("id"),
                        team.get("name"),
                        team.get("shortName"),
                        team.get("tla"),
                        team.get("crest"),
                        team.get("venue"),
                        team.get("address")
                    ))
                if rows:
                    df = spark.createDataFrame(rows, schema=SCHEMA_TEAM)
                    df.write.mode("append").saveAsTable(f"{database}.teams")
                    stats["teams"] += df.count()
            elif "matches" in endpoint:
                rows = []
                for match in data.get("matches", []):
                    utc_str = match.get("utcDate")
                    utc_dt = None
                    if utc_str:
                        try:
                            utc_dt = datetime.fromisoformat(utc_str.replace("Z", "+00:00"))
                        except:
                            utc_dt = None
                    rows.append((
                        match["id"],
                        match.get("competition", {}).get("id"),
                        match.get("season", {}).get("id"),
                        utc_dt,
                        match.get("status"),
                        match.get("stage"),
                        match.get("matchday"),
                        match.get("homeTeam", {}).get("id"),
                        match.get("awayTeam", {}).get("id")
                    ))
                if rows:
                    df = spark.createDataFrame(rows, schema=SCHEMA_MATCH)
                    df.write.mode("append").saveAsTable(f"{database}.matches")
                    stats["matches"] += df.count()
            elif "standings" in endpoint:
                rows = []
                comp_id = data.get("competition", {}).get("id")
                season_id = data.get("season", {}).get("id")
                for st in data.get("standings", []):
                    stage = st.get("stage")
                    type_ = st.get("type")
                    for row in st.get("table", []):
                        rows.append((
                            season_id,
                            comp_id,
                            row["team"]["id"],
                            type_,
                            stage,
                            int(row.get("position", 0)),
                            int(row.get("playedGames", 0)),
                            int(row.get("won", 0)),
                            int(row.get("draw", 0)),
                            int(row.get("lost", 0)),
                            int(row.get("goalsFor", 0)),
                            int(row.get("goalsAgainst", 0)),
                            int(row.get("goalDifference", 0)),
                            int(row.get("points", 0)),
                        ))
                if rows:
                    df = spark.createDataFrame(rows, schema=SCHEMA_STANDING)
                    df.write.mode("append").saveAsTable(f"{database}.standings")
                    stats["standings"] += df.count()
    
    return stats