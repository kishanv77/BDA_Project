# dashboard_api.py
# Flask API to serve real-time sentiment data to dashboard

from flask import Flask, jsonify
from flask_cors import CORS
import pandas as pd
import glob
import os
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables from .env if present
load_dotenv()

app = Flask(__name__)
CORS(app)

class SentimentDashboard:
    def __init__(self, data_path='./sentiment_data'):
        self.data_path = data_path

    def get_latest_data(self, limit=100):
        """Read latest sentiment data from Parquet files"""
        try:
            # Get all parquet files
            files = glob.glob(os.path.join(self.data_path, '*.parquet'))

            if not files:
                return None

            # Read all files and combine
            dfs = []
            for file in files:
                df = pd.read_parquet(file)
                dfs.append(df)

            # Combine and sort by timestamp
            combined_df = pd.concat(dfs, ignore_index=True)
            combined_df = combined_df.sort_values('timestamp', ascending=False)

            return combined_df.head(limit)

        except Exception as e:
            print(f"Error reading data: {e}")
            return None

    def get_metrics(self):
        """Calculate aggregated metrics"""
        df = self.get_latest_data(limit=1000)

        if df is None or df.empty:
            return {
                'total_tweets': 0,
                'avg_polarity': 0,
                'avg_subjectivity': 0,
                'sentiment_distribution': {
                    'Positive': 0,
                    'Neutral': 0,
                    'Negative': 0
                },
                'dominant_sentiment': 'N/A'
            }

        sentiment_counts = df['sentiment'].value_counts().to_dict()

        return {
            'total_tweets': len(df),
            'avg_polarity': float(df['polarity'].mean()),
            'avg_subjectivity': float(df['subjectivity'].mean()),
            'sentiment_distribution': {
                'Positive': sentiment_counts.get('Positive', 0),
                'Neutral': sentiment_counts.get('Neutral', 0),
                'Negative': sentiment_counts.get('Negative', 0)
            },
            'dominant_sentiment': df['sentiment'].mode()[0] if not df.empty else 'N/A'
        }

    def get_recent_tweets(self, limit=20):
        """Get recent tweets with sentiment"""
        df = self.get_latest_data(limit=limit)

        if df is None or df.empty:
            return []

        tweets = []
        for _, row in df.iterrows():
            tweets.append({
                'text': row['text'],
                'polarity': float(row['polarity']),
                'subjectivity': float(row['subjectivity']),
                'sentiment': row['sentiment'],
                'timestamp': row.get('created_at', str(datetime.now()))
            })

        return tweets

dashboard = SentimentDashboard()

@app.route('/api/metrics')
def get_metrics():
    """API endpoint for dashboard metrics"""
    return jsonify(dashboard.get_metrics())

@app.route('/api/tweets')
def get_tweets():
    """API endpoint for recent tweets"""
    return jsonify(dashboard.get_recent_tweets())

@app.route('/api/health')
def health():
    """Health check endpoint"""
    return jsonify({'status': 'ok'})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
