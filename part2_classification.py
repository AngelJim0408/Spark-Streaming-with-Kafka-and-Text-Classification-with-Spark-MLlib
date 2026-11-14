"""
Part 2: Text Classification of Streamed Bluesky Posts with Spark MLlib.
Trains a TF-IDF + Logistic Regression pipeline on AG News, saves the PipelineModel,
runs batch classification on streamed messages from Kafka topic1, and exports CSVs.
"""

import argparse
import os
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, trim, udf
from pyspark.sql.types import DoubleType, StringType, StructField, StructType
from pyspark.ml import Pipeline, PipelineModel
from pyspark.ml.feature import RegexTokenizer, StopWordsRemover, HashingTF, IDF
from pyspark.ml.classification import LogisticRegression

LABEL_MAP = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}


def parse_args():
    parser = argparse.ArgumentParser(description="Part 2: Spark MLlib Text Classification")
    parser.add_argument("--ag-news", required=True, help="Path to AG News train parquet file")
    parser.add_argument("--model-dir", required=True, help="Directory path to save/load PipelineModel")
    parser.add_argument("--output-dir", required=True, help="Directory path to write output CSV files")
    parser.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    parser.add_argument("--topic", default="topic1", help="Kafka topic containing streamed text")
    parser.add_argument("--jsonl-file", default=None, help="Optional fallback JSONL file path instead of Kafka")
    return parser.parse_args()


def main():
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    spark = SparkSession.builder \
        .appName("CS4371-Part2-TextClassification") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    train_df = spark.read.parquet(args.ag_news)
    print("\n=== 2.1 AG News Training Set Class Counts ===")
    train_df.groupBy("label").count().orderBy("label").show()

    msg_schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
    ])

    if args.jsonl_file:
        raw_stream_df = spark.read.schema(msg_schema).json(args.jsonl_file)
    else:
        kafka_df = spark.read \
            .format("kafka") \
            .option("kafka.bootstrap.servers", args.bootstrap) \
            .option("subscribe", args.topic) \
            .option("startingOffsets", "earliest") \
            .load()
        raw_stream_df = kafka_df.select(
            from_json(col("value").cast("string"), msg_schema).alias("data")
        ).select("data.*")

    clean_df = raw_stream_df \
        .select(trim(col("text")).alias("text")) \
        .filter((col("text").isNotNull()) & (col("text") != "")) \
        .dropDuplicates(["text"])

    remaining_count = clean_df.count()
    print(f"\n=== 2.1 Remaining Clean, Deduplicated Streamed Messages: {remaining_count} ===")

    tokenizer = RegexTokenizer(inputCol="text", outputCol="words", pattern="\\W+", toLowercase=True)
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    hashing_tf = HashingTF(inputCol="filtered_words", outputCol="raw_features", numFeatures=20000)
    idf = IDF(inputCol="raw_features", outputCol="features", minDocFreq=5)
    lr = LogisticRegression(featuresCol="features", labelCol="label", maxIter=20, regParam=0.01)

    pipeline = Pipeline(stages=[tokenizer, remover, hashing_tf, idf, lr])
    fitted_model = pipeline.fit(train_df)

    fitted_model.write().overwrite().save(args.model_dir)
    print(f"\n=== 2.2 Saved PipelineModel to: {args.model_dir} ===")

    loaded_model = PipelineModel.load(args.model_dir)
    predictions = loaded_model.transform(clean_df)

    map_label_udf = udf(lambda idx: LABEL_MAP.get(int(idx), "Unknown"), StringType())
    max_prob_udf = udf(lambda prob: float(max(prob)), DoubleType())

    results_df = predictions \
        .withColumn("predicted_class", map_label_udf(col("prediction"))) \
        .withColumn("predicted_probability", max_prob_udf(col("probability"))) \
        .select("text", "predicted_class", "predicted_probability")

    print("\n=== 2.3a Predicted Class Counts Across Streamed Messages ===")
    results_df.groupBy("predicted_class").count().orderBy("count", ascending=False).show()

    print("\n=== 2.3a Five Example Messages per Class (with Predicted Probability) ===")
    for label_name in ["World", "Sports", "Business", "Sci/Tech"]:
        print(f"\n--- Class: {label_name} ---")
        results_df.filter(col("predicted_class") == label_name) \
            .orderBy(col("predicted_probability").desc()) \
            .show(5, truncate=90)

    pdf = results_df.toPandas()

    all_preds_path = os.path.join(args.output_dir, "all_batch_predictions.csv")
    pdf.to_csv(all_preds_path, index=False)

    sample_50 = pdf.sample(n=min(50, len(pdf)), random_state=42).copy()
    sample_50["human_label"] = ""
    hand_label_path = os.path.join(args.output_dir, "hand_labeled_50.csv")
    sample_50.to_csv(hand_label_path, index=False)

    print(f"\n=== 2.4 Exported Files ===")
    print(f"1. All predictions: {all_preds_path}")
    print(f"2. 50 random samples for manual labeling: {hand_label_path}")


if __name__ == "__main__":
    main()
