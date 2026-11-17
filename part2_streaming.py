"""
run
python collector.py --bootstrap localhost:9092 --topic topic1 --out messages.jsonl
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<SPARK_VERSION> \
    part2_streaming.py --model-dir saved_ag_news_model --bootstrap localhost:9092 --topic topic1
"""

import argparse

from pyspark.ml import PipelineModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, to_json, struct, trim, when, lit
from pyspark.sql.types import StringType, StructField, StructType

LABEL_MAP = {0: "World", 1: "Sports", 2: "Business", 3: "Sci/Tech"}


def parse_args():
    p = argparse.ArgumentParser(description="Part 2.3c: real-time classification with running counts")
    p.add_argument("--model-dir", required=True, help="Directory of the saved PipelineModel")
    p.add_argument("--bootstrap", default="localhost:9092", help="Kafka bootstrap servers")
    p.add_argument("--topic", default="topic1", help="Input Kafka topic")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                   help="Where to start reading topic1 (default: only new messages)")
    p.add_argument("--trigger-seconds", type=int, default=10, help="Trigger interval in seconds")
    p.add_argument("--out-topic", default=None,
                   help="Optional Kafka topic to also send the running counts to (e.g. topic2)")
    p.add_argument("--checkpoint", default="checkpoints/part2_class_counts",
                   help="Checkpoint dir (used by the Kafka sink)")
    return p.parse_args()


def main():
    args = parse_args()

    spark = SparkSession.builder.appName("CS4371-Part2-StreamingClassification").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    model = PipelineModel.load(args.model_dir)

    msg_schema = StructType([
        StructField("id", StringType(), True),
        StructField("text", StringType(), True),
        StructField("created_at", StringType(), True),
    ])

    stream_df = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)
        .load()
        .select(from_json(col("value").cast("string"), msg_schema).alias("data"))
        .select(trim(col("data.text")).alias("text"))
        .filter(col("text").isNotNull() & (col("text") != ""))
    )

    predictions = model.transform(stream_df)

    class_col = lit("Unknown")
    for idx, name in LABEL_MAP.items():
        class_col = when(col("prediction") == idx, lit(name)).otherwise(class_col)
    labeled = predictions.withColumn("predicted_class", class_col)

    counts = labeled.groupBy("predicted_class").count()

    trigger = {"processingTime": f"{args.trigger_seconds} seconds"}

    console_query = (
        counts.writeStream.outputMode("complete")
        .format("console")
        .option("truncate", False)
        .trigger(**trigger)
        .start()
    )

    queries = [console_query]

    if args.out_topic:
        kafka_query = (
            counts.select(
                col("predicted_class").alias("key"),
                to_json(struct("predicted_class", "count")).alias("value"),
            )
            .writeStream.outputMode("complete")
            .format("kafka")
            .option("kafka.bootstrap.servers", args.bootstrap)
            .option("topic", args.out_topic)
            .option("checkpointLocation", args.checkpoint)
            .trigger(**trigger)
            .start()
        )
        queries.append(kafka_query)

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
