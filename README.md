# Real-Time Twitter Sentiment Analysis

A complete production-ready system for real-time sentiment analysis of Twitter data using PySpark Streaming, Kafka, and TextBlob.

## 🎯 Features

- **Real-time Stream Processing** - Process live tweets using PySpark Structured Streaming
- **Sentiment Analysis** - Analyze polarity and subjectivity using TextBlob
- **Scalable Architecture** - Kafka-based message streaming for high throughput
- **Interactive Dashboard** - Live visualization of sentiment metrics
- **Flexible Deployment** - Docker Compose for easy setup

## 📋 Prerequisites

- Python 3.8+
- Java 11+ (for Spark)
- Apache Kafka 3.6+
- Apache Spark 3.5+
- Twitter API v2 Bearer Token

## 🚀 Quick Start

### 1. Installation

```bash
# Clone the repository
git clone <your-repo-url>
cd twitter-sentiment-analysis

# Install Python dependencies
pip install -r requirements.txt

# Download TextBlob corpora
python -m textblob.download_corpora
```

### 2. Get Twitter API Credentials

1. Visit [Twitter Developer Portal](https://developer.twitter.com/en/portal/dashboard)
2. Create a new project and app
3. Generate a Bearer Token (API v2)
4. Copy the token to `twitter_producer.py`

### 3. Start Services

#### Option A: Docker Compose (Recommended)

```bash
# Start Kafka, Zookeeper, and Spark
docker-compose up -d

# Create Kafka topic
docker exec -it <kafka-container-id> kafka-topics.sh --create \
  --topic twitter-stream \
  --bootstrap-server localhost:9092 \
  --partitions 1 \
  --replication-factor 1
```

#### Option B: Manual Setup

```bash
# Terminal 1: Start Zookeeper
bin/zookeeper-server-start.sh config/zookeeper.properties

# Terminal 2: Start Kafka
bin/kafka-server-start.sh config/server.properties

# Terminal 3: Create topic
bin/kafka-topics.sh --create --topic twitter-stream \
  --bootstrap-server localhost:9092 \
  --partitions 1 --replication-factor 1
```

### 4. Run the Application

There are two convenient ways to run the system locally:

- Full pipeline (Kafka + Spark + Producer) — recommended for production-like testing.
- Lightweight local demo (no Twitter credentials, no Kafka) — useful for quick verification.

Option A — Full pipeline (Kafka + Producer + Spark)

```powershell
# Start services (using Docker Compose)
docker-compose up -d

# Create Kafka topic (run in PowerShell)
docker exec -it (docker ps --filter "ancestor=confluentinc/cp-kafka:latest" --format "{{.ID}}") \
  bash -c "kafka-topics.sh --create --topic twitter-stream --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1"

# Terminal: Start Twitter Producer (requires TWITTER_BEARER_TOKEN in env)
# Set environment (PowerShell)
$env:TWITTER_BEARER_TOKEN = 'YOUR_TOKEN'
python twitter_producer.py

# Terminal: Start Spark Consumer (Kafka mode)
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_sentiment_consumer.py

# Terminal: Start the Dashboard API
python dashboard_api.py

# Open the dashboard in your browser (dashboard.html or the frontend you use)
```

Option B — Lightweight local demo (no Twitter, no Kafka)

This mode uses `simulate_tweets.py` to stream sample tweets to a TCP socket and the Spark consumer in socket mode.

```powershell
# Terminal 1: Start the simulator (binds to localhost:9999 by default)
python .\simulate_tweets.py --host localhost --port 9999

# Terminal 2: Start Spark Consumer in socket mode
spark-submit spark_sentiment_consumer.py

# Terminal 3: Start the Dashboard API
python dashboard_api.py

# Visit the dashboard API endpoints, e.g. http://localhost:5000/api/metrics
```

Notes:
- The socket consumer (`process_stream_socket`) reads newline-delimited JSON with `text` and `created_at` fields. `simulate_tweets.py` sends compatible messages.
- For production-style testing use Option A so Kafka decouples producer and consumer.

## 📁 Project Structure

```
twitter-sentiment-analysis/
├── twitter_producer.py          # Streams tweets to Kafka
├── spark_sentiment_consumer.py  # PySpark streaming processor
├── dashboard_api.py             # Flask API for dashboard
├── dashboard.html               # Interactive web dashboard
├── requirements.txt             # Python dependencies
├── docker-compose.yml           # Docker services configuration
├── README.md                    # This file
├── sentiment_data/              # Parquet output files (created at runtime)
└── checkpoint/                  # Spark checkpoints (created at runtime)
```

## 🔧 Configuration

### Twitter Producer

Edit `twitter_producer.py`:

```python
BEARER_TOKEN = "YOUR_TWITTER_BEARER_TOKEN"
keywords = ['#AI', 'artificial intelligence']  # Customize keywords
language = 'en'  # Change language
```

### Spark Consumer

Edit `spark_sentiment_consumer.py`:

```python
kafka_bootstrap_servers = 'localhost:9092'  # Kafka server
topic = 'twitter-stream'  # Kafka topic name
```

### Dashboard
### Create a `.env` file quickly

A helper PowerShell script is provided to copy `.env.example` to `.env` and open it for editing.

From the repository root run:

```powershell
.\scripts\create_env.ps1
```

After editing, save the file. The Python services will automatically load `.env` (we use `python-dotenv`).


Edit `dashboard_api.py`:

```python
data_path = './sentiment_data'  # Path to Parquet files
```

## 📊 Output Data

Sentiment data is stored in Parquet format with the following schema:

| Field | Type | Description |
|-------|------|-------------|
| id | string | Tweet ID |
| text | string | Tweet text |
| cleaned_text | string | Preprocessed text |
| polarity | float | Sentiment score (-1 to +1) |
| subjectivity | float | Subjectivity score (0 to 1) |
| sentiment | string | Positive/Neutral/Negative |
| created_at | timestamp | Tweet timestamp |
| timestamp | float | Processing timestamp |

## 🎨 Dashboard Features

- **Total Tweets** - Real-time count of analyzed tweets
- **Average Sentiment** - Mean polarity score
- **Dominant Sentiment** - Most common sentiment category
- **Distribution Chart** - Percentage breakdown
- **Trend Graph** - Historical sentiment trend
- **Live Stream** - Recent tweets with scores

## 🔍 Sentiment Analysis

The system uses **TextBlob** for sentiment analysis:

- **Polarity**: -1 (negative) to +1 (positive)
- **Subjectivity**: 0 (objective) to 1 (subjective)

Classification thresholds:
- Positive: polarity > 0.1
- Negative: polarity < -0.1
- Neutral: -0.1 ≤ polarity ≤ 0.1

## 🐛 Troubleshooting

### Kafka Connection Error

```bash
# Check if Kafka is running
netstat -an | grep 9092

# Verify topic exists
kafka-topics.sh --list --bootstrap-server localhost:9092
```

### Twitter API Rate Limits

The producer implements rate limiting with 15-second delays. Adjust in `twitter_producer.py`:

```python
time.sleep(15)  # Increase for stricter limits
```

### Spark Memory Issues

Increase memory allocation:

```bash
spark-submit --driver-memory 4g --executor-memory 4g \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 \
  spark_sentiment_consumer.py
```

## 📈 Performance Tips

1. **Increase Kafka partitions** for higher throughput
2. **Tune Spark batch interval** in consumer (default: 60 seconds)
3. **Use Parquet compression** for storage efficiency
4. **Scale Spark executors** for parallel processing

## 🔐 Security Notes

- **Never commit API keys** - Use environment variables
- **Secure Kafka** - Enable authentication in production
- **Rate limit API calls** - Respect Twitter API limits
- **Validate input** - Sanitize tweet text before processing

## 📝 License

MIT License - feel free to use for your projects!

## 🤝 Contributing

Contributions welcome! Please open an issue or submit a PR.

## 📧 Support

For issues or questions, please create a GitHub issue.

---

Built with ❤️ using PySpark, Kafka, and TextBlob

**Verification**

- Check the dashboard API is running and returning metrics:

```powershell
curl http://localhost:5000/api/metrics
```

- View recent tweets with sentiment:

```powershell
curl http://localhost:5000/api/tweets
```

- If running the socket demo, verify the simulator is sending messages (simulator terminal prints sent tweets) and the Spark consumer console shows analyzed rows.

- If running the Kafka pipeline, verify messages reach Kafka using the Kafka console consumer inside the container:

```powershell
docker exec -it <kafka-container-id> bash -c "kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic twitter-stream --from-beginning --timeout-ms 10000"
```

If anything is not behaving as expected, check logs for each component (`docker-compose logs`, Spark driver UI at `localhost:4040`, and Flask console) and confirm environment variables in `.env` or your shell.
