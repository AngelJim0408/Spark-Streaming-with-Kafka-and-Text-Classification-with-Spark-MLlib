ReadMe

Part 1 - Collector (collector.py)

producer.py streams live public posts from Bluesky Jetstream into the Kafka topic "topic1". Each post is also appended to a local JSONL file. This is the first stage of the pipeline:

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


