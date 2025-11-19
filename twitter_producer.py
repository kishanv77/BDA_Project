# twitter_producer.py
# Streams tweets from Twitter API to Kafka topic

import tweepy
import json
import socket
from kafka import KafkaProducer
import time
import os
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

class TwitterStreamProducer:
    def __init__(self, bearer_token, kafka_bootstrap_servers='localhost:9092'):
        """Initialize Twitter API v2 client and Kafka producer"""
        self.client = tweepy.Client(
            bearer_token=bearer_token,
            wait_on_rate_limit=True
        )

        self.producer = KafkaProducer(
            bootstrap_servers=kafka_bootstrap_servers,
            value_serializer=lambda x: json.dumps(x).encode('utf-8')
        )

    def stream_tweets(self, keywords, language='en', topic='twitter-stream'):
        """
        Stream tweets matching keywords to Kafka topic

        Args:
            keywords: List of keywords/hashtags to track
            language: Language code (default: 'en')
            topic: Kafka topic name
        """
        print(f"Starting stream for keywords: {keywords}")

        # Build search query
        query = ' OR '.join(keywords)
        if language:
            query += f' lang:{language}'

        # Stream tweets using Twitter API v2
        while True:
            try:
                tweets = self.client.search_recent_tweets(
                    query=query,
                    max_results=10,
                    tweet_fields=['created_at', 'author_id', 'lang', 'public_metrics']
                )

                if tweets.data:
                    for tweet in tweets.data:
                        tweet_data = {
                            'id': tweet.id,
                            'text': tweet.text,
                            'created_at': str(tweet.created_at),
                            'author_id': tweet.author_id,
                            'lang': tweet.lang,
                            'timestamp': time.time()
                        }

                        # Send to Kafka
                        self.producer.send(topic, value=tweet_data)
                        print(f"Sent tweet: {tweet.text[:50]}...")

                # Wait before next batch (respect rate limits)
                time.sleep(15)

            except Exception as e:
                print(f"Error: {e}")
                time.sleep(30)

# Alternative: TCP Socket approach for direct Spark streaming
class TwitterTCPStream:
    def __init__(self, bearer_token, host='localhost', port=9999):
        self.client = tweepy.Client(bearer_token=bearer_token)
        self.host = host
        self.port = port

    def stream_to_socket(self, keywords, language='en'):
        """Stream tweets directly to TCP socket for Spark consumption"""

        # Create TCP socket
        server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server_socket.bind((self.host, self.port))
        server_socket.listen(1)

        print(f"Listening on {self.host}:{self.port}")
        conn, addr = server_socket.accept()
        print(f"Connected by {addr}")

        query = ' OR '.join(keywords) + f' lang:{language}'

        while True:
            try:
                tweets = self.client.search_recent_tweets(
                    query=query,
                    max_results=10,
                    tweet_fields=['created_at']
                )

                if tweets.data:
                    for tweet in tweets.data:
                        message = json.dumps({
                            'text': tweet.text,
                            'created_at': str(tweet.created_at)
                        }) + '\n'
                        conn.send(message.encode('utf-8'))

                time.sleep(10)

            except Exception as e:
                print(f"Error: {e}")
                break

if __name__ == "__main__":
    # Read configuration from environment
    BEARER_TOKEN = os.getenv('TWITTER_BEARER_TOKEN')
    KAFKA_BOOTSTRAP = os.getenv('KAFKA_BOOTSTRAP_SERVERS', 'localhost:9092')
    KAFKA_TOPIC = os.getenv('KAFKA_TOPIC', 'twitter-stream')
    TCP_HOST = os.getenv('TCP_HOST', 'localhost')
    TCP_PORT = int(os.getenv('TCP_PORT', '9999'))
    MODE = os.getenv('PRODUCER_MODE', 'kafka')  # 'kafka', 'socket', or 'simulate'

    keywords = ['#AI', 'artificial intelligence']

    if MODE == 'kafka':
        if not BEARER_TOKEN:
            print('TWITTER_BEARER_TOKEN not set in environment. Exiting.')
        else:
            producer = TwitterStreamProducer(BEARER_TOKEN, kafka_bootstrap_servers=KAFKA_BOOTSTRAP)
            producer.stream_tweets(keywords=keywords, language='en', topic=KAFKA_TOPIC)

    elif MODE == 'socket':
        if not BEARER_TOKEN:
            print('TWITTER_BEARER_TOKEN not set; cannot stream live tweets to socket.')
        else:
            tcp_stream = TwitterTCPStream(BEARER_TOKEN, host=TCP_HOST, port=TCP_PORT)
            tcp_stream.stream_to_socket(keywords, language='en')

    elif MODE == 'simulate':
        # Use local simulator to feed a socket (good for quick local tests)
        try:
            from simulate_tweets import start_stream
            start_stream(TCP_HOST, TCP_PORT)
        except Exception as e:
            print(f'Failed to start simulator: {e}')

    else:
        print(f"Unknown PRODUCER_MODE '{MODE}'. Use 'kafka', 'socket', or 'simulate'.")
