Part 1 - Collector (collector.py streams live public posts from Bluesky Jetstream into the Kafka topic "topic1". Each post is also appended to a local JSONL file. This is the first stage of the pipeline)

Bluesky Jetstream -> collector.py -> Kafka topic1

Install the following Python packages:
  pip install kafka-python websocket-client

-----------------------------------------
Have kafka running (im using windows and set up kafka through powershell doing the following)

have "docker-compose.yml"(included in folder shared) in the directory you are using and: 
  docker compose up -d
  docker compose ps
-----------------------------------------

Create the topic1:
	docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --create --if-not-exists --topic topic1 --bootstrap-server localhost:9092

Run the collector:
 	python collector.py --bootstrap localhost:9092 --topic topic1 --out messages.jsonl
 
	Expected output:
		[producer] sent 50 messages to topic1
		[producer] sent 100 messages to topic1

Check if it works
Open http://localhost:8080 -> Topics -> topic1 -> Messages.


Part 2 - (2.1 - 2.2) - AG News Model Training & Batch Classification

Install the following Python packages:
    pip install pyspark==3.5.3 pandas pyarrow

Download the AG News training parquet file (train-00000-of-00001.parquet) from:
    https://huggingface.co/datasets/fancyzhx/ag_news

-------------------------

run
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<SPARK_VERSION> \
    part2_classification.py --ag-news <path_to_ag_news_parquet> --model-dir saved_ag_news_model --output-dir part2_outputs --bootstrap localhost:9092 --topic topic1

Expected output:
    saved_ag_news_model/ (trained Spark MLlib PipelineModel)
    part2_outputs/all_batch_predictions.csv
    part2_outputs/hand_labeled_50.csv

-------------------------


Part 2.3
run
python collector.py --bootstrap localhost:9092 --topic topic1 --out messages.jsonl
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:<SPARK_VERSION> \
    part2_streaming.py --model-dir saved_ag_news_model --bootstrap localhost:9092 --topic topic1
