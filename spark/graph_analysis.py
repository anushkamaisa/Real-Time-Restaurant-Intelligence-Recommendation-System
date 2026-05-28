import os
import subprocess
import sys
import tempfile
import warnings

repo_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, repo_root)

from spark.spark_session import get_spark

warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    message=r".*DataFrame\.sql_ctx is an internal property.*"
)

def build_graph_data(spark, data_path):
    from pyspark.sql.functions import col, expr, lit

    restaurants = spark.read.csv(data_path, header=True, inferSchema=True)

    cities = restaurants.select(col("City").alias("id")).distinct().withColumn("type", lit("city"))
    cuisines = restaurants.select(col("Cuisines").alias("id")).distinct().withColumn("type", lit("cuisine"))

    vertices = cities.union(cuisines).distinct()
    edges = restaurants.select(
        col("City").alias("src"),
        col("Cuisines").alias("dst"),
        expr("try_cast(`Aggregate rating` AS DOUBLE)").alias("rating")
    ).na.drop(subset=["src", "dst", "rating"])

    return vertices, edges


def _filter_table_lines(text):
    lines = []
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("|") or stripped.startswith("+") or stripped.startswith("only showing"):
            lines.append(line)
        elif stripped == "":
            if lines and lines[-1].strip() != "":
                lines.append(line)
    return lines


def run_graph_analysis():
    from pyspark.sql.functions import col

    data_path = os.environ.get("ZOMATO_DATA_PATH", "data/zomato.csv")
    spark = get_spark()
    _, edges = build_graph_data(spark, data_path)

    in_degrees = edges.groupBy("dst").count().withColumnRenamed("count", "inDegree")
    in_degrees.orderBy(col("inDegree").desc()).show(20, False)

    edges.groupBy("src", "dst") \
        .avg("rating") \
        .orderBy(col("avg(rating)").desc()) \
        .show(20, False)

    north_edges = edges.filter(col("dst").contains("North Indian"))
    south_edges = edges.filter(col("dst").contains("South Indian"))

    north_edges.groupBy("src").count().withColumnRenamed("count", "north_count") \
        .orderBy(col("north_count").desc()) \
        .show(20, False)

    south_edges.groupBy("src").count().withColumnRenamed("count", "south_count") \
        .orderBy(col("south_count").desc()) \
        .show(20, False)

    north_edges.groupBy("src", "dst") \
        .avg("rating") \
        .orderBy(col("avg(rating)").desc()) \
        .show(20, False)

    south_edges.groupBy("src", "dst") \
        .avg("rating") \
        .orderBy(col("avg(rating)").desc()) \
        .show(20, False)


if __name__ == "__main__":
    if "--raw" in sys.argv:
        run_graph_analysis()
    else:
        result = subprocess.run(
            [sys.executable, __file__, "--raw"],
            capture_output=True,
            text=True,
            env=os.environ,
        )
        filtered_lines = _filter_table_lines(result.stdout + result.stderr)
        print("\n".join(filtered_lines))
