"""

Part 1 Spark Streaming
Run with spark-submit 

"""

import argparse
import re

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import ArrayType, StringType, StructField, StructType

# Shape of the JSON messages written by collector.py
MESSAGE_SCHEMA = StructType([
    StructField("id", StringType()),
    StructField("text", StringType()),
    StructField("created_at", StringType()),
])

# spaCy entity labels we count. Numbers, dates, money, etc. are left out
# because they are not "named" entities in a useful sense for this chart.
DEFAULT_ENTITY_TYPES = "PERSON,ORG,GPE,LOC,NORP,FAC,PRODUCT,EVENT,WORK_OF_ART,LAW"

URL_RE = re.compile(r"https?://\S+|www\.\S+")
HANDLE_RE = re.compile(r"@[\w.\-]+")
SPACE_RE = re.compile(r"\s+")
EDGE_CHARS = " \"'“”‘’,:;!?()[]{}<>*_-–—…|/\\"


def parse_args():
    p = argparse.ArgumentParser(description="Kafka -> Spark NER running counts -> Kafka")
    p.add_argument("--bootstrap", required=True, help="Kafka bootstrap servers, e.g. localhost:9092")
    p.add_argument("--in-topic", default="topic1", help="Topic with raw posts (written by collector.py)")
    p.add_argument("--out-topic", default="topic2", help="Topic that receives entity counts")
    p.add_argument("--checkpoint", required=True,
                   help="Checkpoint directory for the streaming queries (any local path or URI)")
    p.add_argument("--trigger", default="1 minute", help="Micro-batch trigger interval, e.g. '30 seconds'")
    p.add_argument("--starting-offsets", default="latest", choices=["latest", "earliest"],
                   help="Where to start reading topic1 the first time the query runs")
    p.add_argument("--max-offsets", type=int, default=None,
                   help="Optional cap on messages read per trigger (maxOffsetsPerTrigger)")
    p.add_argument("--spacy-model", default="en_core_web_sm", help="spaCy model to load")
    p.add_argument("--entity-types", default=DEFAULT_ENTITY_TYPES,
                   help="Comma-separated spaCy labels to count")
    p.add_argument("--output-mode", default="update", choices=["update", "complete"],
                   help="update = send only entities whose count changed; complete = send all")
    p.add_argument("--shuffle-partitions", type=int, default=4,
                   help="spark.sql.shuffle.partitions (Spark's default of 200 is slow on one machine)")
    p.add_argument("--console", action="store_true",
                   help="Also print the current top entities to the console at each trigger")
    p.add_argument("--console-rows", type=int, default=10, help="Rows to print with --console")
    return p.parse_args()



# Text helpers 

def clean_text(text):
    """Remove URLs and @handles, turn #hashtags into words, collapse whitespace."""
    text = URL_RE.sub(" ", text)
    text = HANDLE_RE.sub(" ", text)
    text = text.replace("#", " ")
    return SPACE_RE.sub(" ", text).strip()


def normalize_entity(name):
    """Tidy an entity string so that small variants are counted together."""
    name = SPACE_RE.sub(" ", name).strip(EDGE_CHARS)
    for suffix in ("'s", "’s", "‘s", "`s"):
        if name.endswith(suffix):
            name = name[: -len(suffix)]
    if name.lower().startswith("the "):
        name = name[4:]
    name = name.strip(EDGE_CHARS)
    # Drop a sentence-ending period ("Supreme Court.") but keep abbreviations
    if name.endswith(".") and "." not in name[:-1]:
        name = name[:-1]
    return name.strip(EDGE_CHARS)


def get_nlp(model_name):
    """
    Load the spaCy model once per Python worker process and reuse it.
    Spark re-creates the UDF function for every task, so a normal global would
    be lost; a module registered in sys.modules survives because Spark reuses
    its Python worker processes between tasks.
    """
    import sys
    import types

    cache = sys.modules.get("_ner_spacy_cache")
    if cache is None:
        cache = types.ModuleType("_ner_spacy_cache")
        cache.models = {}
        sys.modules["_ner_spacy_cache"] = cache
    if model_name not in cache.models:
        import spacy
        # Only the NER component is needed; skipping the parser makes it faster.
        cache.models[model_name] = spacy.load(model_name, disable=["parser", "lemmatizer"])
    return cache.models[model_name]


def make_entity_extractor(model_name, entity_types):
    wanted = set(entity_types)

    def extract(text):
        """Return the distinct named entities in one post (each counted once per post)."""
        if not text:
            return []
        doc = get_nlp(model_name)(clean_text(text)[:2000])
        found = []
        for ent in doc.ents:
            if ent.label_ not in wanted:
                continue
            name = normalize_entity(ent.text)
            if len(name) < 2 or len(name) > 100 or not any(c.isalpha() for c in name):
                continue
            if name not in found:
                found.append(name)
        return found

    return extract


# ---------------------------------------------
# Streaming job


def main():
    args = parse_args()
    checkpoint = args.checkpoint.rstrip("/")
    entity_types = [t.strip().upper() for t in args.entity_types.split(",") if t.strip()]

    spark = SparkSession.builder.appName("BlueskyNamedEntityCounts").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    spark.conf.set("spark.sql.shuffle.partitions", str(args.shuffle_partitions))

    #  Read raw posts from topic1
    reader = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("subscribe", args.in_topic)
        .option("startingOffsets", args.starting_offsets)
        .option("failOnDataLoss", "false")
    )
    if args.max_offsets:
        reader = reader.option("maxOffsetsPerTrigger", args.max_offsets)
    raw = reader.load()

    # Kafka value bytes -> JSON -> post text
    posts = (
        raw.select(F.from_json(F.col("value").cast("string"), MESSAGE_SCHEMA).alias("m"))
        .select(F.col("m.text").alias("text"))
        .where(F.col("text").isNotNull())
    )

    # Named entities: one row per (post, entity)
    extract_entities = F.udf(make_entity_extractor(args.spacy_model, entity_types),
                             ArrayType(StringType()))
    entities = posts.select(F.explode(extract_entities(F.col("text"))).alias("entity"))

    # Running count per entity 
    counts = entities.groupBy("entity").count()

    #  At each trigger, send the counts to topic2 as JSON:
    to_kafka = counts.select(
        F.col("entity").alias("key"),
        F.to_json(F.struct(
            F.col("entity"),
            F.col("count"),
            F.current_timestamp().alias("updated_at"),
        )).alias("value"),
    )
    (
        to_kafka.writeStream.format("kafka")
        .queryName("entity_counts_to_kafka")
        .option("kafka.bootstrap.servers", args.bootstrap)
        .option("topic", args.out_topic)
        .option("checkpointLocation", f"{checkpoint}/kafka_sink")
        .outputMode(args.output_mode)
        .trigger(processingTime=args.trigger)
        .start()
    )

    #  Show the current top entities in the terminal 
    if args.console:
        (
            counts.orderBy(F.desc("count")).writeStream.format("console")
            .queryName("entity_counts_console")
            .option("numRows", args.console_rows)
            .option("truncate", "false")
            .option("checkpointLocation", f"{checkpoint}/console")
            .outputMode("complete")
            .trigger(processingTime=args.trigger)
            .start()
        )

    # The real-time classifier query will be added here as another
    # readStream on topic1 + writeStream, before waiting on the queries.

    print(f"[ner_stream] reading {args.in_topic}, writing counts to {args.out_topic} "
          f"every {args.trigger}")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
