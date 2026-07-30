"""Recompute feature baselines from the observation table and write them back.

Run on a Spark cluster or locally with a JDBC driver on the classpath:

    spark-submit --packages org.postgresql:postgresql:42.7.3 spark/refresh_baselines.py \
        --jdbc-url jdbc:postgresql://localhost:5432/driftwatch --bins 10

The job reads the last 30 days of observations, builds equal-width bins per feature,
computes the expected proportion in each bin, and upserts the result into the
``baselines`` table that the Java service reads when it scores drift.

The module also holds the reference PSI, KS and severity functions. The Java
service implements the same formulas; the tests in ``tests/`` pin the numbers so
the two sides cannot drift apart.
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from typing import Dict, List, Sequence

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

LOOKBACK_DAYS = 30
MIN_OBSERVATIONS = 200
EPSILON = 1e-6


def psi(expected: Sequence[float], observed: Sequence[float]) -> float:
    """Population stability index between two proportion vectors.

    Both vectors hold one proportion per bucket and must be the same length.
    Empty buckets are floored at ``EPSILON`` so a bucket that lost every value
    still contributes a finite term instead of raising.
    """
    if expected is None or observed is None:
        raise ValueError("expected and observed are required")
    if len(expected) != len(observed):
        raise ValueError("expected and observed must be the same length")
    if len(expected) < 2:
        raise ValueError("at least two buckets are required")

    total = 0.0
    for e, o in zip(expected, observed):
        e = max(float(e), EPSILON)
        o = max(float(o), EPSILON)
        total += (o - e) * math.log(o / e)
    return total


def ks(bins: Sequence[float], expected: Sequence[float], observed: Sequence[float]) -> float:
    """Kolmogorov-Smirnov distance between two binned distributions.

    ``bins`` holds the ascending bucket edges (one more than the number of
    proportions). The statistic is the largest gap between the two cumulative
    distributions, so it lands in ``[0, 1]``.
    """
    if bins is None or expected is None or observed is None:
        raise ValueError("bins, expected and observed are required")
    buckets = len(bins) - 1
    if buckets < 1:
        raise ValueError("at least two bin edges are required")
    if len(expected) != buckets or len(observed) != buckets:
        raise ValueError(f"expected and observed must hold one proportion per bucket ({buckets})")

    cum_expected = 0.0
    cum_observed = 0.0
    max_gap = 0.0
    for e, o in zip(expected, observed):
        cum_expected += float(e)
        cum_observed += float(o)
        max_gap = max(max_gap, abs(cum_expected - cum_observed))
    return min(max_gap, 1.0)


def severity(value: float) -> str:
    """Map a PSI to the label the service and the dashboard use."""
    if value < 0.1:
        return "ok"
    if value < 0.25:
        return "warn"
    return "alert"


def build_baseline(df: DataFrame, feature: str, bins: int = 10) -> Dict[str, object]:
    """Build equal-width bins and expected proportions for one feature.

    ``df`` must have the columns ``model_id``, ``feature_name`` and ``value``.
    Returns a dict with ``model_id``, ``feature_name``, ``bins`` (bin upper edges,
    the last one being the maximum) and ``proportions`` (one per bin, summing to 1).
    """
    if bins < 2:
        raise ValueError("bins must be at least 2")

    rows = df.filter(F.col("feature_name") == feature).select("model_id", "value")
    stats = rows.agg(
        F.count("value").alias("n"),
        F.min("value").alias("lo"),
        F.max("value").alias("hi"),
    ).collect()[0]

    n = int(stats["n"] or 0)
    if n < MIN_OBSERVATIONS:
        raise ValueError(f"feature {feature} has {n} observations, need {MIN_OBSERVATIONS}")

    lo = float(stats["lo"])
    hi = float(stats["hi"])
    if hi <= lo:
        hi = lo + 1.0

    width = (hi - lo) / bins
    edges: List[float] = [round(lo + width * (i + 1), 6) for i in range(bins)]
    edges[-1] = round(hi, 6)

    def bucket(value: float) -> int:
        if value <= lo:
            return 0
        for i, edge in enumerate(edges):
            if value <= edge:
                return i
        return bins - 1

    bucket_udf = F.udf(bucket, "int")
    counts = (
        rows.withColumn("bucket", bucket_udf(F.col("value")))
        .groupBy("bucket")
        .count()
        .collect()
    )
    total = sum(int(r["count"]) for r in counts) or 1
    per_bucket = {int(r["bucket"]): int(r["count"]) for r in counts}
    proportions = [round(per_bucket.get(i, 0) / total, 6) for i in range(bins)]

    model_id = rows.select("model_id").limit(1).collect()[0]["model_id"]
    return {
        "model_id": model_id,
        "feature_name": feature,
        "bins": edges,
        "proportions": proportions,
    }


def write_baselines(spark: SparkSession, jdbc_url: str, jdbc_props: dict, bins: int = 10) -> int:
    """Recompute every feature baseline and upsert it. Returns the number written."""
    observations = (
        spark.read.jdbc(url=jdbc_url, table="observations", properties=jdbc_props)
        .where(F.col("observed_at") >= F.current_timestamp() - F.expr(f"INTERVAL {LOOKBACK_DAYS} DAYS"))
        .select("model_id", "feature_name", "value")
    )

    features = [r["feature_name"] for r in observations.select("feature_name").distinct().collect()]
    written = 0
    for feature in sorted(features):
        try:
            baseline = build_baseline(observations, feature, bins=bins)
        except ValueError as exc:
            print(f"skip {feature}: {exc}", file=sys.stderr)
            continue

        row = spark.createDataFrame(
            [
                (
                    baseline["model_id"],
                    baseline["feature_name"],
                    json.dumps(baseline["bins"]),
                    json.dumps(baseline["proportions"]),
                )
            ],
            schema="model_id string, feature_name string, bins string, proportions string",
        )
        row.write.jdbc(
            url=jdbc_url,
            table="baselines_staging",
            mode="append",
            properties=jdbc_props,
        )
        written += 1

    if written:
        conn = _connect(jdbc_url, jdbc_props)
        try:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO baselines (model_id, feature_name, bins, proportions, updated_at) "
                    "SELECT model_id, feature_name, bins::jsonb, proportions::jsonb, now() "
                    "FROM baselines_staging "
                    "ON CONFLICT (model_id, feature_name) DO UPDATE SET "
                    "bins = EXCLUDED.bins, proportions = EXCLUDED.proportions, "
                    "updated_at = EXCLUDED.updated_at"
                )
                cur.execute("TRUNCATE baselines_staging")
            conn.commit()
        finally:
            conn.close()

    return written


def _connect(jdbc_url: str, jdbc_props: dict):
    """Open a plain psycopg connection from the JDBC url and properties."""
    import psycopg2

    prefix = "jdbc:postgresql://"
    if not jdbc_url.startswith(prefix):
        raise ValueError(f"unsupported jdbc url: {jdbc_url}")
    host_port, _, database = jdbc_url[len(prefix):].partition("/")
    host, _, port = host_port.partition(":")
    return psycopg2.connect(
        host=host or "localhost",
        port=int(port or 5432),
        dbname=database,
        user=jdbc_props.get("user"),
        password=jdbc_props.get("password"),
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Refresh drift baselines from observations")
    parser.add_argument("--jdbc-url", required=True, help="PostgreSQL JDBC url")
    parser.add_argument("--user", default="driftwatch")
    parser.add_argument("--password", default="driftwatch")
    parser.add_argument("--bins", type=int, default=10)
    args = parser.parse_args()

    spark = SparkSession.builder.appName("driftwatch-refresh-baselines").getOrCreate()
    props = {"user": args.user, "password": args.password}
    try:
        written = write_baselines(spark, args.jdbc_url, props, bins=args.bins)
        print(f"wrote {written} baselines")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
