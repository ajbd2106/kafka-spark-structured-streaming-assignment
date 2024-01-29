# streaming_consumer.py
import logging
import yaml
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import expr

# Setup logging
log_file = 'logs/streaming_consumer.log'
logging.basicConfig(filename=log_file, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

            

def read_streaming_config():
    with open('conf/streaming_config.yaml', 'r') as file:
        return yaml.safe_load(file)


def read_data_from_kafka(spark, kafka_bootstrap_servers, kafka_topic, starting_offset):
    try:
        # Read data from Kafka with offset maintenance and de-duplication
        df = spark.readStream.format("kafka") \
            .option("kafka.bootstrap.servers", kafka_config['bootstrap_servers']) \
            .option("subscribe", kafka_config['topic']) \
            .option("startingOffsets", kafka_config['starting_offset']) \
            .load()

        # Deserialize JSON data from Kafka
        schema = StructType().add("value", StringType())
        parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

        return parsed_df
    except Exception as e:
        print(f"Error reading data from Kafka: {e}")
        return None

def xml_parsing_and_flattening(data):
    try:
        # Use the com.databricks.spark.xml package for XML parsing and flattening
        xml_df = spark.read \
            .format("com.databricks.spark.xml") \
            .option("rowTag", "Root") \
            .option("attributePrefix", "") \
            .load(data.selectExpr("xml_data").rdd.map(lambda x: x[0]))

        # Flatten nested XML structure
        flattened_df = xml_df.select("Record.*")

        return flattened_df
    except Exception as e:
        print(f"Error parsing and flattening XML data: {e}")
        return None

def apply_data_validation(data):
    try:
        # Schema Validation
        expected_schema = StructType([
            StructField("ID", IntegerType(), True),
            StructField("Name", StringType(), True),
            StructField("Details", StructType([
                StructField("Age", IntegerType(), True),
                StructField("Address", StructType([
                    StructField("City", StringType(), True),
                    StructField("State", StringType(), True)
                ]), True)
            ]), True)
        ])

        # Validate against the expected schema
        validated_data = data.selectExpr("*").where("1 = 0")  # Empty DataFrame with the same schema
        validated_data = data.select("*").validate(expected_schema)

        # Data Type Validation
        validated_data = validated_data \
            .withColumn("ID", col("ID").cast(IntegerType())) \
            .withColumn("Age", col("Age").cast(IntegerType()))

        # Data Formatting (Trimming)
        formatted_data = validated_data \
            .withColumn("Name", trim(col("Name")))\
            .withColumn("City", trim(col("Details.Address.City")))\
            .withColumn("State", trim(col("Details.Address.State")))

        return formatted_data
    except Exception as e:
        print(f"Error applying data validation: {e}")
        return None

def write_data_to_hdfs(df, hdfs_raw_zone_path, checkpoint_location):
    try:
        # Write data to HDFS Parquet - RAW Zone
        hdfs_writer_query = df.writeStream \
            .format("parquet") \
            .outputMode("append") \
            .option("path", hdfs_raw_zone_path) \
            .option("checkpointLocation", checkpoint_location) \
            .start()

        hdfs_writer_query.awaitTermination()
    except Exception as e:
        print(f"Error writing data to HDFS: {e}")

def main():
    streaming_config = read_streaming_config()

    hdfs_raw_zone_path = 'hdfs://localhost:9000/raw_zone'
    checkpoint_location = '/path/to/checkpoint_dir'
    starting_offset = 'earliest'  # or 'latest' based on your requirements

    try:
        spark = SparkSession.builder \
            .appName("StreamingConsumer") \
            .getOrCreate()

        streaming_data = read_data_from_kafka(spark, streaming_config)

        if streaming_data:
            # Apply XML parsing and flattening
            flattened_data = xml_parsing_and_flattening(streaming_data)

            if flattened_data:
                # Apply data validation and formatting
                processed_data = apply_data_validation(flattened_data)

                if processed_data:
                    # Write data to HDFS
                    write_data_to_hdfs(processed_data, hdfs_raw_zone_path, checkpoint_location)

    except Exception as e:
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
