

from __future__ import annotations

import logging
import random
from typing import Any

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, lit, when

logger = logging.getLogger(__name__)

def _replace_table(spark: SparkSession, df: DataFrame, table_name: str):
    temp_name = f"{table_name}_temp"
    df.write.mode("overwrite").saveAsTable(temp_name)
    spark.sql(f"DROP TABLE IF EXISTS {table_name}")
    spark.sql(f"ALTER TABLE {temp_name} RENAME TO {table_name}")
    logger.debug("Table %s replaced", table_name)


def _swap_teams_df(df: DataFrame) -> DataFrame:
    matches = df.collect()
    if not matches:
        return df
    rng = random.Random(42)
    to_swap_ids = {m.id for m in rng.sample(matches, min(5, len(matches)))}
    return df.withColumn(
        "home_team_id",
        when(col("id").isin(to_swap_ids), col("away_team_id")).otherwise(col("home_team_id"))
    ).withColumn(
        "away_team_id",
        when(col("id").isin(to_swap_ids), col("home_team_id")).otherwise(col("away_team_id"))
    )


def _duplicate_first_df(df: DataFrame) -> DataFrame:
    first_row = df.limit(1)
    return df.union(first_row)


def _drop_required_df(spark: SparkSession, df: DataFrame) -> DataFrame:
    first_id = df.select("id").first()
    if first_id and first_id[0] is not None:
        return df.withColumn(
            "id",
            when(col("id") == first_id[0], lit(None).cast("int")).otherwise(col("id"))
        )
    return df


def _corrupt_id_df(spark: SparkSession, df: DataFrame) -> DataFrame:
    first_id = df.select("id").first()
    if first_id and first_id[0] is not None:
        return df.withColumn(
            "id",
            when(col("id") == first_id[0], lit(None).cast("int")).otherwise(col("id"))
        )
    return df


def _matchday_out_of_range_df(df: DataFrame) -> DataFrame:
    ids_to_change = [r.id for r in df.select("id").limit(3).collect()]
    if ids_to_change:
        return df.withColumn(
            "matchday",
            when(col("id").isin(ids_to_change), lit(999)).otherwise(col("matchday"))
        )
    return df


def apply_stg_swap_teams(spark: SparkSession):
    df = spark.table("etl_kio.matches")
    df = _swap_teams_df(df)
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("STG swap_teams: teams swapped for 5 matches")


def apply_stg_duplicate_first(spark: SparkSession):
    df = spark.table("etl_kio.matches")
    df = _duplicate_first_df(df)
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("STG duplicate_first: duplicated first match")


def apply_stg_drop_required(spark: SparkSession):
    df = spark.table("etl_kio.matches")
    df = _drop_required_df(spark, df)
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("STG drop_required: set id=NULL for first match")


def apply_stg_corrupt_id(spark: SparkSession):
    df = spark.table("etl_kio.matches")
    df = _corrupt_id_df(spark, df)
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("STG corrupt_id: corrupted id to NULL for first match")


def apply_stg_matchday_out_of_range(spark: SparkSession):
    df = spark.table("etl_kio.matches")
    df = _matchday_out_of_range_df(df)
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("STG matchday_out_of_range: set matchday=999 for 3 matches")


def apply_stg_drop_matches_key(spark: SparkSession):
    empty_df = spark.table("etl_kio.matches").filter("1=0")
    _replace_table(spark, empty_df, "etl_kio.matches")
    logger.info("STG drop_matches_key: deleted all rows from matches")


def apply_dds_fact_match(spark: SparkSession):
    comps = spark.table("etl_kio.competitions").collect()
    teams = spark.table("etl_kio.teams").collect()
    matches = spark.table("etl_kio.matches").collect()
    if not comps or not teams or not matches:
        return
    comp_id = comps[0].id
    team_id = teams[0].id
    season_id = matches[0].season_id

    new_row = spark.createDataFrame(
        [(99999901, comp_id, season_id, "2025-01-01", "MUTATED", "MUTATED", 0, team_id, team_id)],
        ["id", "competition_id", "season_id", "utcDate", "status", "stage", "matchday", "home_team_id", "away_team_id"]
    )
    df = spark.table("etl_kio.matches")
    df = df.union(new_row)
    df = df.withColumn(
        "home_team_id",
        when(col("id") == 99999901, lit(None).cast("int")).otherwise(col("home_team_id"))
    ).withColumn(
        "away_team_id",
        when(col("id") == 99999901, lit(None).cast("int")).otherwise(col("away_team_id"))
    ).withColumn(
        "matchday",
        when(col("id") == 99999901, lit(999)).otherwise(col("matchday"))
    )
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("DDS fact_match: inserted anomalous match 99999901")


def apply_dds_fact_standing(spark: SparkSession):
    comps = spark.table("etl_kio.competitions").collect()
    teams = spark.table("etl_kio.teams").collect()
    standings = spark.table("etl_kio.standings").collect()
    if not comps or not teams or not standings:
        return
    comp_id = comps[0].id
    team_id = teams[0].id
    season_id = standings[0].season_id

    new_row = spark.createDataFrame(
        [(season_id, comp_id, team_id, "MUTATED", "MUTATED", 0, 0, 0, 0, 0, 0, 0, 0, 0)],
        ["season_id", "competition_id", "team_id", "type", "stage", "position", "playedGames",
         "won", "draw", "lost", "goalsFor", "goalsAgainst", "goalDifference", "points"]
    )
    df = spark.table("etl_kio.standings")
    df = df.union(new_row)
    _replace_table(spark, df, "etl_kio.standings")
    logger.info("DDS fact_standing: inserted mutated standing with zero stats")


def apply_dds_dim_competition(spark: SparkSession):
    first_comp = spark.table("etl_kio.competitions").select("id").first()
    if first_comp:
        comp_id = first_comp[0]
        df = spark.table("etl_kio.competitions")
        df = df.withColumn(
            "name", when(col("id") == comp_id, lit("MUTATED_COMP")).otherwise(col("name"))
        ).withColumn(
            "code", when(col("id") == comp_id, lit("MUT")).otherwise(col("code"))
        ).withColumn(
            "type", when(col("id") == comp_id, lit("MUTATED")).otherwise(col("type"))
        ).withColumn(
            "plan", when(col("id") == comp_id, lit("MUTATED")).otherwise(col("plan"))
        )
        _replace_table(spark, df, "etl_kio.competitions")
        logger.info("DDS dim_competition: updated competition %s", comp_id)


def apply_dds_season_dates_missing(spark: SparkSession):
    df = spark.table("etl_kio.matches").withColumn("utcDate", lit(None).cast("timestamp"))
    _replace_table(spark, df, "etl_kio.matches")
    logger.info("DDS season_dates_missing: nullified utcDate for all matches")

MUTATION_MAP = {
    "swap_teams":          apply_stg_swap_teams,
    "duplicate_first":     apply_stg_duplicate_first,
    "drop_required":       apply_stg_drop_required,
    "corrupt_id":          apply_stg_corrupt_id,
    "matchday_out_of_range": apply_stg_matchday_out_of_range,
    "drop_matches_key":    apply_stg_drop_matches_key,
    "fact_match":          apply_dds_fact_match,
    "fact_standing":       apply_dds_fact_standing,
    "dim_competition":     apply_dds_dim_competition,
    "season_dates_missing": apply_dds_season_dates_missing,
}