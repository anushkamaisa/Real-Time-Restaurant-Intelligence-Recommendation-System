import os
import sys

repo_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, repo_root)

from pyspark.sql.functions import expr, col
from spark.spark_session import get_spark


def run_restaurant_insights():
    spark = get_spark()
    data_path = os.environ.get("ZOMATO_DATA_PATH", "data/zomato.csv")

    restaurants = spark.read.csv(data_path, header=True, inferSchema=True)
    restaurants = restaurants.withColumn(
        "avg_rating",
        expr("try_cast(regexp_replace(`Aggregate rating`, '[^0-9.]', '') AS DOUBLE)")
    ).withColumn(
        "avg_cost",
        expr("try_cast(regexp_replace(`Average Cost for two`, '[^0-9.]', '') AS DOUBLE)")
    ).filter(
        col("avg_rating").isNotNull() & col("avg_cost").isNotNull()
    )
    restaurants.createOrReplaceTempView("restaurants")

    top_cities = spark.sql("""
        SELECT City,
               AVG(avg_rating) AS avg_rating,
               COUNT(*) AS total_restaurants,
               AVG(avg_cost) AS avg_cost
        FROM restaurants
        GROUP BY City
        ORDER BY avg_rating DESC, total_restaurants DESC
        LIMIT 20
    """)

    top_cuisines = spark.sql("""
        SELECT `Cuisines` AS cuisine,
               AVG(avg_rating) AS avg_rating,
               COUNT(*) AS total_restaurants
        FROM restaurants
        GROUP BY `Cuisines`
        ORDER BY avg_rating DESC, total_restaurants DESC
        LIMIT 20
    """)

    print("=== Top Cities by Average Rating ===")
    top_cities.show(truncate=False)

    print("=== Top Cuisines by Average Rating ===")
    top_cuisines.show(truncate=False)


if __name__ == "__main__":
    run_restaurant_insights()
