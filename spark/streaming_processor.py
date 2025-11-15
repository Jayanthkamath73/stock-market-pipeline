"""
PySpark Streaming Consumer for Stock Data
Reads from Kafka, processes in real-time, writes to S3 as Parquet
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, to_timestamp, current_timestamp,
    window, avg, min, max, sum as spark_sum
)
from pyspark.sql.types import (
    StructType, StructField, StringType, 
    DoubleType, IntegerType, TimestampType
)
import os
from dotenv import load_dotenv
import logging

# Load environment variables
load_dotenv()

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "stock_prices"
S3_BUCKET = os.getenv('S3_RAW_BUCKET')
S3_OUTPUT_PATH = f"s3a://{S3_BUCKET}/stock_prices/"
S3_CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoints/stock_prices/"

def create_spark_session():
    """
    Create Spark session with Kafka and S3 support
    """
    logger.info("🔧 Creating Spark Session...")
    
    spark = SparkSession.builder \
        .appName("StockMarketStreamProcessor") \
        .config("spark.jars.packages", 
                "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                "org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.hadoop.fs.s3a.access.key", os.getenv('AWS_ACCESS_KEY_ID')) \
        .config("spark.hadoop.fs.s3a.secret.key", os.getenv('AWS_SECRET_ACCESS_KEY')) \
        .config("spark.hadoop.fs.s3a.endpoint", "s3.ap-south-1.amazonaws.com") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("spark.sql.streaming.checkpointLocation", S3_CHECKPOINT_PATH) \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .getOrCreate()
    
    # Set log level
    spark.sparkContext.setLogLevel("WARN")
    
    logger.info("✅ Spark Session created successfully")
    return spark

def define_schema():
    """
    Define schema for incoming stock data
    Must match the JSON structure from Kafka producer
    """
    return StructType([
        StructField("symbol", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("volume", IntegerType(), False),
        StructField("change_percent", StringType(), True),
        StructField("timestamp", StringType(), False),
        StructField("trade_date", StringType(), True)
    ])

def read_from_kafka(spark, schema):
    """
    Read streaming data from Kafka topic
    
    Args:
        spark: SparkSession object
        schema: StructType schema definition
    
    Returns:
        DataFrame: Streaming DataFrame
    """
    logger.info(f"📖 Reading from Kafka topic: {KAFKA_TOPIC}")
    
    # Read from Kafka
    kafka_df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()
    
    # Parse JSON from Kafka value field
    stock_df = kafka_df.select(
        from_json(col("value").cast("string"), schema).alias("data"),
        col("timestamp").alias("kafka_timestamp")
    ).select("data.*", "kafka_timestamp")
    
    logger.info("✅ Kafka stream configured")
    return stock_df

def transform_data(df):
    """
    Apply transformations to streaming data
    
    Args:
        df: Input streaming DataFrame
    
    Returns:
        DataFrame: Transformed DataFrame
    """
    logger.info("🔄 Applying transformations...")
    
    # Convert string timestamp to TimestampType
    transformed_df = df \
        .withColumn("event_timestamp", to_timestamp("timestamp")) \
        .withColumn("processed_timestamp", current_timestamp()) \
        .withColumn("change_percent_numeric", 
                   col("change_percent").cast(DoubleType())) \
        .drop("timestamp", "change_percent")
    
    # Add data quality checks
    validated_df = transformed_df \
        .filter(col("price") > 0) \
        .filter(col("volume") >= 0) \
        .filter(col("symbol").isNotNull())
    
    logger.info("✅ Transformations applied")
    return validated_df

def write_to_s3_raw(df):
    """
    Write streaming data to S3 in Parquet format
    Partitioned by symbol and date for efficient querying
    
    Args:
        df: Streaming DataFrame to write
    
    Returns:
        StreamingQuery: Active streaming query
    """
    logger.info(f"💾 Writing to S3: {S3_OUTPUT_PATH}")
    
    query = df \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", S3_OUTPUT_PATH) \
        .option("checkpointLocation", S3_CHECKPOINT_PATH) \
        .partitionBy("symbol") \
        .trigger(processingTime='1 minute') \
        .start()
    
    logger.info("✅ Streaming query started")
    return query

def create_aggregated_stream(df):
    """
    Create 5-minute aggregated metrics
    Useful for monitoring and analytics
    
    Args:
        df: Input streaming DataFrame
    
    Returns:
        DataFrame: Aggregated DataFrame
    """
    logger.info("📊 Creating aggregated stream...")
    
    agg_df = df \
        .withWatermark("event_timestamp", "10 minutes") \
        .groupBy(
            window("event_timestamp", "5 minutes"),
            "symbol"
        ) \
        .agg(
            avg("price").alias("avg_price"),
            min("price").alias("min_price"),
            max("price").alias("max_price"),
            spark_sum("volume").alias("total_volume"),
            avg("change_percent_numeric").alias("avg_change_percent")
        )
    
    logger.info("✅ Aggregated stream created")
    return agg_df

def write_aggregated_to_console(agg_df):
    """
    Write aggregated metrics to console for monitoring
    
    Args:
        agg_df: Aggregated DataFrame
    
    Returns:
        StreamingQuery: Console output query
    """
    query = agg_df \
        .writeStream \
        .format("console") \
        .outputMode("update") \
        .option("truncate", "false") \
        .trigger(processingTime='1 minute') \
        .start()
    
    return query

def main():
    """
    Main execution function
    """
    logger.info("🚀 Starting PySpark Streaming Consumer...")
    
    # Validate environment variables
    if not S3_BUCKET:
        logger.error("❌ S3_RAW_BUCKET not configured in .env")
        exit(1)
    
    # Create Spark session
    spark = create_spark_session()
    
    # Define schema
    schema = define_schema()
    
    # Read from Kafka
    stock_stream = read_from_kafka(spark, schema)
    
    # Transform data
    transformed_stream = transform_data(stock_stream)
    
    # Write raw data to S3
    s3_query = write_to_s3_raw(transformed_stream)
    
    # Create aggregated metrics
    aggregated_stream = create_aggregated_stream(transformed_stream)
    
    # Write aggregated to console for monitoring
    console_query = write_aggregated_to_console(aggregated_stream)
    
    logger.info("✅ All streams started successfully")
    logger.info("📊 Monitoring console output below:")
    logger.info("🛑 Press Ctrl+C to stop\n")
    
    try:
        # Keep application running
        s3_query.awaitTermination()
    except KeyboardInterrupt:
        logger.info("\n🛑 Stopping streaming queries...")
        s3_query.stop()
        console_query.stop()
        spark.stop()
        logger.info("👋 Shutdown complete")

if __name__ == "__main__":
    main()

