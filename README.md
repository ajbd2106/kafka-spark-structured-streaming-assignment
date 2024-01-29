# kafka-spark-structured-streaming-assignmen
Created a Spark Structured Streaming code streaming_producer.py in Python to publish some data to Kafka. We use Spark Structured Streaming in Python to publish data to Kafka, I have used foreach sink to write data to Kafka. I have used  'while True' loop to generate the desired XML data. Note that this example uses a rate source for demonstration purposes.
Created Spark Structured Streaming code streaming_consumer.py in Python to read the same data from Kafka and store it in HDFS Parquet - RAW Zone 
Createed a sample Project Folder Structure for Code in git. Included logs, etc. to show how it will be organized 
Create sample scripts (more than pseudo code) and placed them in the corresponding folder 
Consumer Kafka (Offset Maintenance and De-duplication) - If my application needs to maintain message ordering and prevent duplication, we can enable idempotency for your Apache Kafka producer. An idempotent producer has a unique producer ID and uses sequence IDs for each message, allowing the broker to ensure, on a per-partition basis, that it is committing ordered messages with no duplication.

            Set the ProducerConfig configuration parameters relevant to the idempotent producer:

            enable.idempotence=true
            acks=all
Applied XML Parsing and flattening
Data validation code included as well for Schema Validation , Data Type validation and Data formatting 
created a batch_processing.py code to move data from RAW to Processed zone as well as Partitioning the data based on a date field in the final Parquet file (Processed Zone) 

Apology if i have created a project and code while expectation was pseudo only but i felt like giving more
Created shell script to submit spark job
To ensure that only delta (new or changed) records are pulled from the RAW Zone to the Processed Zone, you can use a watermark and event time-based processing. Spark Structured Streaming allows you to handle late arriving data by using event time and watermarking, which is particularly useful in scenarios where data arrives out of order.
Explanation:
read_data_from_raw_zone: The function now includes withWatermark to specify the watermark duration. This setting defines how late the arriving events can be before they are considered as late.

process_and_write_to_processed_zone: The mode is changed to append to handle incremental processing and only append new data to the existing Parquet files in the Processed Zone.

To manage the size of the RAW Zone and prevent it from becoming very large, you can implement a data retention and archiving strategy. This strategy involves periodically moving or archiving older data to a different location or storage system. Below are steps and considerations for implementing such a strategy:

Define Retention Policy:

Determine the retention period for data in the RAW Zone. For example, you might decide to retain data for a specific number of days, weeks, or months.
Partition Data:

If your data has a date or timestamp column, consider partitioning your data based on this column. Partitioning makes it easier to identify and move older data.
Archive or Move Old Data:

Create a process or script that identifies and moves data older than the defined retention period. This process can be scheduled to run periodically.

The choice between running Spark programs on a cluster or in a client mode depends on your specific use case, requirements, and the resources available. Below are considerations for both cluster mode and client mode:
Development and Testing:

During the development and testing phases, consider using client mode for its ease of use and quick feedback loop.
Production and Large-scale Processing:

For production workloads or large-scale data processing, cluster mode is recommended. Deploy Spark applications to a Spark cluster managed by a cluster manager (e.g., YARN, Mesos, or Kubernetes) to fully leverage distributed computing resources.
Consideration for Clusters:

Ensure that your cluster is properly configured with sufficient resources, and the cluster manager is set up to manage the Spark applications efficiently.
Deployment Strategy:

Determine whether you want to submit Spark applications directly to the cluster from the client machine or use a deployment tool or cluster manager to manage the deployment.

the choice between cluster mode and client mode is not mutually exclusive. Depending on the use case, you may use client mode during development and testing and switch to cluster mode for production deployment.


Deciding on the number of cores and executors for Spark streaming and batch jobs involves understanding the characteristics of your data, workload, and the resources available in your Spark cluster. Below are considerations for determining the number of cores and executors for Spark streaming and batch jobs:
Dynamic Resource Allocation:

Leverage dynamic resource allocation features provided by Spark. This allows Spark to dynamically adjust the number of executors based on the workload. It can be beneficial in scenarios where workloads vary over time.
Monitor and Tune:

Continuously monitor the performance of your Spark applications and tune the configuration based on actual resource usage. Spark's web UI provides insights into the resource utilization of running applications.
Experiment and Benchmark:

Conduct experiments and benchmarks to find the optimal configuration for your specific workload. It may involve adjusting the number of cores, executors, and other configuration parameters to achieve the desired performance.
Consideration for Cluster Manager:

Consider the cluster manager (e.g., YARN, Mesos, Kubernetes) and its capabilities for resource allocation. Ensure that the cluster manager is configured to meet the requirements of your Spark applications.
Scaling Out:

Consider horizontally scaling out your cluster if needed. Adding more worker nodes to the cluster can provide additional resources for parallel processing.

Avoiding the small file issue in the RAW and Processed Zones is crucial for efficient data storage and processing in a distributed environment. Accumulation of small files can lead to increased overhead, inefficient resource utilization, and slower query performance. Here are several strategies to ensure that you do not run into small file issues:

1. File Size and Block Size:
HDFS Block Size:

Configure the HDFS block size appropriately. A smaller block size can result in more files but can be useful for handling smaller files efficiently. However, it's essential to strike a balance to avoid excessive metadata overhead.
Combine Small Files:

When writing data to HDFS, consider combining smaller files into larger ones using tools like Apache Hadoop's hadoop fs -getmerge or by using Apache Spark's coalesce or repartition operations.
2. Partitioning:
Use Meaningful Partitioning:

When writing data to HDFS, leverage meaningful partitioning based on the characteristics of your data. This can reduce the number of files in a directory and make it easier to manage.
Time-based Partitioning:

If applicable, consider partitioning data based on time (e.g., by date or hour). This is often useful for time-series data and can facilitate efficient querying for specific time ranges.
3. Compression:
Apply Compression:
Consider applying compression to the data before storing it on HDFS. Compressed files can help reduce storage space and improve read and write performance. Spark supports various compression codecs.
python
df.write.option("compression", "snappy").parquet("/path/to/output")
4. Optimal File Formats:
Choose Appropriate File Formats:
Choose file formats that are suitable for your use case. Parquet and ORC formats are commonly used for efficient storage and query performance due to their columnar storage and compression capabilities.
5. Compaction:
Periodic Compaction:
Consider implementing periodic compaction processes to merge small files into larger ones. This can be done as a scheduled job or as part of the data processing pipeline.
6. Cleanup Mechanisms:
Data Retention and Cleanup:
Implement mechanisms to clean up or archive old and unnecessary data. Regularly review the data retention policies and remove data that is no longer required.
7. Optimize Write Operations:
Batch Writing:
When writing data to HDFS, prefer batch writing over frequent small writes. This can help in optimizing the HDFS write operation and reducing the number of small files.
8. Use External Tables and Views:
External Tables or Views:
Consider using external tables or views to logically organize data without physically moving files. This can provide a structured view of the data while maintaining a more efficient storage layout.
