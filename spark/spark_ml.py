import os
import sys

repo_root = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, repo_root)

from pyspark.ml import Pipeline
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import StandardScaler, VectorAssembler
from pyspark.ml.regression import GBTRegressor
from pyspark.sql.functions import col, regexp_replace
from spark.spark_session import get_spark


def load_training_data(spark, data_path):
    raw = spark.read.csv(data_path, header=True, inferSchema=True)
    cleaned = raw.select(
        "Aggregate rating",
        "Votes",
        "Average Cost for two"
    ).dropna()

    prepared = cleaned.withColumnRenamed("Aggregate rating", "label") \
        .withColumnRenamed("Votes", "votes") \
        .withColumnRenamed("Average Cost for two", "avg_cost")

    prepared = prepared.withColumn("label", col("label").cast("double")) \
        .withColumn("votes", col("votes").cast("double")) \
        .withColumn("avg_cost", regexp_replace(col("avg_cost"), ",", "").cast("double"))

    return prepared.filter("label IS NOT NULL AND votes IS NOT NULL AND avg_cost IS NOT NULL")


def build_pipeline():
    assembler = VectorAssembler(
        inputCols=["votes", "avg_cost"],
        outputCol="rawFeatures"
    )

    scaler = StandardScaler(
        inputCol="rawFeatures",
        outputCol="features"
    )

    regressor = GBTRegressor(
        featuresCol="features",
        labelCol="label",
        maxIter=50,
        maxDepth=5
    )

    return Pipeline(stages=[assembler, scaler, regressor])


def train_and_save_model():
    spark = get_spark()
    data_path = os.environ.get("ZOMATO_DATA_PATH", "data/zomato.csv")
    model_path = os.environ.get("SPARK_MODEL_PATH", "models/spark_rating_model")

    dataset = load_training_data(spark, data_path)
    train, test = dataset.randomSplit([0.8, 0.2], seed=42)

    pipeline = build_pipeline()
    model = pipeline.fit(train)

    predictions = model.transform(test)
    evaluator = RegressionEvaluator(
        labelCol="label",
        predictionCol="prediction",
        metricName="rmse"
    )

    rmse = evaluator.evaluate(predictions)
    print(f"Spark ML model trained. RMSE={rmse:.4f}")

    model.write().overwrite().save(model_path)
    print(f"Saved Spark ML model to: {model_path}")
    return model


if __name__ == "__main__":
    train_and_save_model()
