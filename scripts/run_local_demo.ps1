# Run a quick local demo: simulator -> Spark socket consumer -> dashboard API
# Run each line in a separate PowerShell terminal so each process stays active.

# 1) Start the simulator (binds to localhost:9999)
python .\simulate_tweets.py --host localhost --port 9999

# 2) In a new terminal: Start Spark consumer in socket mode
# spark-submit should be available in your PATH
spark-submit spark_sentiment_consumer.py

# 3) In another terminal: Start the Flask dashboard API
python dashboard_api.py

# 4) Verify (use in your browser or via curl):
# http://localhost:5000/api/metrics
# http://localhost:5000/api/tweets
