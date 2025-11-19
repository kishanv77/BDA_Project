# spark_sentiment_consumer.py
# PySpark Streaming job for real-time sentiment analysis

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, udf, avg, count, when
from pyspark.sql.types import StructType, StructField, StringType, FloatType, TimestampType
from textblob import TextBlob
import re
import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

class SentimentAnalyzer:
    def __init__(self, app_name="TwitterSentimentAnalysis"):
        """Initialize Spark session with Kafka dependencies"""
        self.spark = SparkSession.builder \
            .appName(app_name) \
            .config("spark.jars.packages", 
                   "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0") \
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")

    def clean_text(self, text):
        """Clean tweet text for sentiment analysis"""
        # Remove URLs
        text = re.sub(r'https?://\S+|www\.\S+', '', text)
        # Remove mentions
        text = re.sub(r'@\w+', '', text)
        # Remove hashtags
        text = re.sub(r'#', '', text)
        # Remove RT
        text = re.sub(r'RT\s+', '', text)
        # Remove special characters
        text = re.sub(r'[^\w\s]', '', text)
        return text.strip()

    def get_sentiment(self, text):
        """Calculate sentiment polarity using TextBlob"""
        try:
            cleaned = self.clean_text(text)
            blob = TextBlob(cleaned)
            return float(blob.sentiment.polarity)
        except:
            return 0.0

    def get_subjectivity(self, text):
        """Calculate sentiment subjectivity"""
        try:
            cleaned = self.clean_text(text)
            blob = TextBlob(cleaned)
            return float(blob.sentiment.subjectivity)
        except:
            return 0.0

    def classify_sentiment(self, polarity):
        """Classify polarity score into categories"""
        if polarity > 0.1:
            return "Positive"
        elif polarity < -0.1:
            return "Negative"
        else:
            return "Neutral"

    def process_stream_kafka(self, kafka_bootstrap_servers, topic):
        """Process tweet stream from Kafka"""

        # Define schema for incoming tweets
        schema = StructType([
            StructField("id", StringType(), True),
            StructField("text", StringType(), True),
            StructField("created_at", StringType(), True),
            StructField("author_id", StringType(), True),
            StructField("lang", StringType(), True),
            StructField("timestamp", FloatType(), True)
        ])

        # Register UDFs
        sentiment_udf = udf(self.get_sentiment, FloatType())
        subjectivity_udf = udf(self.get_subjectivity, FloatType())
        classify_udf = udf(self.classify_sentiment, StringType())
        clean_text_udf = udf(self.clean_text, StringType())

        # Read from Kafka
        df = self.spark \
            .readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
            .option("subscribe", topic) \
            .option("startingOffsets", "latest") \
            .load()

        # Parse JSON and extract fields
        tweets_df = df.select(
            from_json(col("value").cast("string"), schema).alias("data")
        ).select("data.*")

        # Apply sentiment analysis
        analyzed_df = tweets_df \
            .withColumn("cleaned_text", clean_text_udf(col("text"))) \
            .withColumn("polarity", sentiment_udf(col("text"))) \
            .withColumn("subjectivity", subjectivity_udf(col("text"))) \
            .withColumn("sentiment", classify_udf(col("polarity")))

        # Write to console (for monitoring)
        query_console = analyzed_df \
            .select("text", "polarity", "subjectivity", "sentiment") \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        # Write to Parquet for persistence
        query_parquet = analyzed_df \
            .writeStream \
            .outputMode("append") \
            .format("parquet") \
            .option("path", "./sentiment_data") \
            .option("checkpointLocation", "./checkpoint") \
            .trigger(processingTime='60 seconds') \
            .start()

        # Calculate aggregated metrics
        metrics_df = analyzed_df \
            .groupBy("sentiment") \
            .agg(
                count("*").alias("count"),
                avg("polarity").alias("avg_polarity"),
                avg("subjectivity").alias("avg_subjectivity")
            )

        # Write metrics to console
        query_metrics = metrics_df \
            .writeStream \
            .outputMode("complete") \
            .format("console") \
            .start()

        # Wait for termination
        query_console.awaitTermination()

    def process_stream_socket(self, host='localhost', port=9999):
        """Process tweet stream from TCP socket"""

        schema = StructType([
            StructField("text", StringType(), True),
            StructField("created_at", StringType(), True)
        ])

        # Register UDFs
        sentiment_udf = udf(self.get_sentiment, FloatType())
        subjectivity_udf = udf(self.get_subjectivity, FloatType())
        classify_udf = udf(self.classify_sentiment, StringType())

        # Read from socket
        lines = self.spark \
            .readStream \
            .format("socket") \
            .option("host", host) \
            .option("port", port) \
            .load()

        # Parse JSON
        tweets_df = lines.select(
            from_json(col("value"), schema).alias("data")
        ).select("data.*")

        # Apply sentiment analysis
        analyzed_df = tweets_df \
            .withColumn("polarity", sentiment_udf(col("text"))) \
            .withColumn("subjectivity", subjectivity_udf(col("text"))) \
            .withColumn("sentiment", classify_udf(col("polarity")))

        # Write results
        query = analyzed_df \
            .writeStream \
            .outputMode("append") \
            .format("console") \
            .option("truncate", False) \
            .start()

        query.awaitTermination()

if __name__ == "__main__":
    analyzer = SentimentAnalyzer()

    # Read runtime mode from environment
    MODE = os.getenv('CONSUMER_MODE', 'kafka')  # 'kafka' or 'socket'
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'twitter-stream')
    TCP_HOST = os.getenv('TCP_HOST', 'localhost')
    TCP_PORT = int(os.getenv('TCP_PORT', '9999'))

    if MODE == 'kafka':
        analyzer.process_stream_kafka(kafka_bootstrap_servers=KAFKA_BOOTSTRAP, topic=KAFKA_TOPIC)
    else:
        analyzer.process_stream_socket(host=TCP_HOST, port=TCP_PORT)
