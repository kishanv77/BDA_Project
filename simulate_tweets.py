"""
simulate_tweets.py

Simple TCP socket tweet simulator for local testing of the Spark socket consumer.
Sends JSON lines with `text` and `created_at` fields to a listening socket.

Usage (PowerShell):
    python .\simulate_tweets.py --host localhost --port 9999

"""
import socket
import time
import json
import argparse
from datetime import datetime

SAMPLE_TWEETS = [
    "I love the new AI features! #AI #innovation",
    "Not impressed with the update. It's buggy and slow.",
    "Neutral about this — will watch how it develops.",
    "Amazing performance and great UX. Kudos to the team!",
    "This is terrible. Worst experience ever. #fail",
    "Interesting idea but needs more work. #thoughts",
    "Fantastic progress — excited for the future!",
    "I don't see the point of this feature.",
    "Could be better. Hope they fix the bugs soon.",
    "Absolutely brilliant — exceeded expectations!"
]


def start_stream(host: str, port: int, interval: float = 2.0):
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(1)
    print(f"Simulator listening on {host}:{port} — waiting for a consumer to connect...")

    conn, addr = server.accept()
    print(f"Consumer connected from {addr}")

    try:
        idx = 0
        while True:
            tweet_text = SAMPLE_TWEETS[idx % len(SAMPLE_TWEETS)]
            payload = {
                "text": tweet_text,
                "created_at": datetime.utcnow().isoformat() + "Z"
            }
            message = json.dumps(payload) + "\n"
            conn.sendall(message.encode('utf-8'))
            print(f"Sent: {tweet_text}")
            idx += 1
            time.sleep(interval)
    except BrokenPipeError:
        print("Consumer disconnected. Shutting down simulator.")
    except KeyboardInterrupt:
        print("Simulator interrupted by user.")
    finally:
        conn.close()
        server.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Simulate tweets to a TCP socket')
    parser.add_argument('--host', default='localhost', help='Host to bind')
    parser.add_argument('--port', default=9999, type=int, help='Port to bind')
    parser.add_argument('--interval', default=2.0, type=float, help='Seconds between messages')
    args = parser.parse_args()

    start_stream(args.host, args.port, args.interval)
