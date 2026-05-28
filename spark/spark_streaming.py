import os
import sys

repo_root = os.path.dirname(os.path.dirname(__file__))

# Add the project root to sys.path first so `spark` package imports work when running this script directly.
sys.path.insert(0, repo_root)

# Load the Spark-Kafka connector when using PySpark directly via Python.
os.environ.setdefault(
    "PYSPARK_SUBMIT_ARGS",
    "--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 pyspark-shell"
)

from pyspark.sql.functions import *
from pyspark.sql.types import *

from spark.spark_session import get_spark

spark = get_spark()

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "dinevision-events") \
    .load()

schema = StructType() \
    .add("event", StringType()) \
    .add("timestamp", StringType()) \
    .add("city", StringType()) \
    .add("cuisine", StringType()) \
    .add("rating", StringType()) \
    .add("votes", StringType()) \
    .add("cost", StringType()) \
    .add("predicted_rating", StringType()) \
    .add("query", StringType()) \
    .add("response", StringType())

json_df = df.selectExpr("CAST(value AS STRING)") \
    .select(from_json(col("value"), schema).alias("data")) \
    .select("data.*")

query = json_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .start()

query.awaitTermination()