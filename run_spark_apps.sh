#!/bin/bash


# Set the paths and configurations
RAW_ZONE_PATH='/path/to/raw_zone'
ARCHIVE_PATH='/path/to/archive'
RETENTION_DAYS=30
HDFS_RAW_ZONE_PATH='hdfs://localhost:9000/raw_zone'
HDFS_PROCESSED_ZONE_PATH='hdfs://localhost:9000/processed_zone'
KAFKA_BOOTSTRAP_SERVERS='localhost:9092'
KAFKA_TOPIC='sample_xml_topic'
STARTING_OFFSET='earliest'  # or 'latest' based on your requirements

# Run batch_processing.py on the Spark cluster
spark-submit --master spark://<spark-master-url>:<spark-master-port> batch_processing.py

# Run streaming_consumer.py on the Spark cluster
spark-submit --master spark://<spark-master-url>:<spark-master-port> streaming_consumer.py

# Run streaming_producer.py on the Spark cluster
spark-submit --master spark://<spark-master-url>:<spark-master-port> streaming_producer.py

# Run move_old_data.py on the Spark cluster
spark-submit --master spark://<spark-master-url>:<spark-master-port> move_old_data.py


# Wait for some time to allow streaming_producer.py to generate data
sleep 60

# Run move_old_data.py
spark-submit --master <spark-master-url> move_old_data.py

