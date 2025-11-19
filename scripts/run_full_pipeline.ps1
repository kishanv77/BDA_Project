# Helper script for running the full pipeline (Kafka via Docker, producer, Spark consumer, dashboard)
# Run commands in separate PowerShell terminals as needed.

# 1) Start Kafka/Zookeeper/Spark (docker-compose must be installed)
docker-compose up -d

# 2) Create Kafka topic (replace container id lookup if necessary)
$kafkaContainer = docker ps --filter "ancestor=confluentinc/cp-kafka:latest" --format "{{.ID}}"
if (-not $kafkaContainer) {
    Write-Host "Kafka container not found. Ensure docker-compose is running and the Kafka image is in use."
} else {
    docker exec -it $kafkaContainer bash -c "kafka-topics.sh --create --topic twitter-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"
}

# 3) Start Twitter Producer (requires TWITTER_BEARER_TOKEN)
# Set environment variable in PowerShell: $env:TWITTER_BEARER_TOKEN = 'YOUR_TOKEN'
python twitter_producer.py

# 4) Start Spark consumer (Kafka mode)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_sentiment_consumer.py

# 5) Start the Dashboard API
python dashboard_api.py

# 6) Verify endpoints:
# http://localhost:5000/api/metrics
# http://localhost:5000/api/tweets
