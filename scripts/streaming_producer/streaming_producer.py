# streaming_producer.py
import logging
import time
import json
import yaml
from pyspark.sql import SparkSession

# Setup logging
log_file = 'logs/streaming_producer.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def read_streaming_config():
    with open('conf/streaming_config.yaml', 'r') as file:
        return yaml.safe_load(file)

def generate_sample_data():
    # Generate sample data with timestamp
    return """
    <Root>
        <Record>
            <ID>{}</ID>
            <Name>{}</Name>
            <Details>
                <Age>{}</Age>
                <Address>
                    <City>{}</City>
                    <State>{}</State>
                </Address>
            </Details>
        </Record>
    </Root>
    """.format(3, "Alice Johnson", 28, "Los Angeles", "CA")

def publish_data_to_kafka(spark, kafka_bootstrap_servers, kafka_topic):
    # Define the schema for the streaming DataFrame
    schema = "value STRING"

    # Create a streaming DataFrame with a single column "value"
    streaming_df = spark.readStream \
        .format("rate") \
        .option("rowsPerSecond", 1) \
        .load() \
        .withColumn("value", expr("inline(array_repeat('1', value % 10))"))

    # Define a Kafka sink for the streaming DataFrame
    kafka_sink = streaming_df \
        .selectExpr("CAST(value AS STRING)") \
        .writeStream \
        .outputMode("append") \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("topic", kafka_topic) \
        .start()

    # Start generating and writing data to Kafka
    while True:
        xml_data = generate_sample_data()
        kafka_sink \
            .send(json.dumps({"xml_data": xml_data}))
        time.sleep(5)

    # Wait for the streaming to finish
    kafka_sink.awaitTermination()

def main():
    kafka_bootstrap_servers = 'localhost:9092'
    kafka_topic = 'sample_xml_topic'

    spark = SparkSession.builder \

