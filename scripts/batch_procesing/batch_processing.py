# batch_processing.py
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType
from datetime import datetime, timedelta

def read_data_from_raw_zone(spark, hdfs_raw_zone_path, watermark_duration):
    # Read data from RAW Zone with watermark
    try:
        # Read data from RAW Zone
        raw_df = spark.read.format("parquet").load(raw_zone_path)
        raw_df = raw_df.withWatermark("event_time", watermark_duration)
        return raw_df
    except Exception as e:
        logger.error(f"Error reading data from RAW Zone: {e}")
        raise

def process_and_write_to_processed_zone(raw_df, hdfs_processed_zone_path):
    try:
        # Perform any required transformations or processing
        processed_df = raw_df.withColumn("ProcessingDate", col("ProcessingDate").cast(DateType()))

        # Partition the data based on a date field (ProcessingDate in this example)
        processed_df.write \
            .partitionBy("ProcessingDate") \
            .mode("append") \
            .parquet(hdfs_processed_zone_path)

        logger.info("Batch Processing completed successfully.")
    except Exception as e:
        logger.error(f"Error in batch processing: {e}")
        raise

def main():
    hdfs_raw_zone_path = 'hdfs://localhost:9000/raw_zone'
    hdfs_processed_zone_path = 'hdfs://localhost:9000/processed_zone'
    watermark_duration = '1 hours'  # Adjust based on your data characteristics

    try:
        spark = SparkSession.builder \
            .appName("BatchProcessing") \
            .getOrCreate()

        # Read data from RAW Zone with watermark
        raw_data = read_data_from_raw_zone(spark, hdfs_raw_zone_path, watermark_duration)

        if raw_data:
            # Process and write to Processed Zone
            process_and_write_to_processed_zone(raw_data, hdfs_processed_zone_path)

    except Exception as e:
        logger.exception(f"Unexpected error: {e}")

if __name__ == "__main__":
    main()
