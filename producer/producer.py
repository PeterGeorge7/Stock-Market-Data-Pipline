import time
import json
import requests
from kafka import KafkaProducer

# use api from .env file
from dotenv import load_dotenv
import os

load_dotenv()
API_KEY = os.getenv("API_KEY")
BASE_URL = "https://finnhub.io/api/v1/quote"
STOCK_SYMBOLS = ["AAPL", "GOOGL", "MSFT", "AMZN", "TSLA"]

producer = KafkaProducer(
    # to use local kafka server
    bootstrap_servers=['localhost:29092'],

    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
)


def fetch_quote(symbol):
    url = f"{BASE_URL}?symbol={symbol}&token={API_KEY}"
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        data["symbol"] = symbol
        data["fetched_at"] = int(time.time())
        return data
    except Exception as e:
        print(f"Error fetching data for {symbol}: {e}")
        return None
    

while True:
    for symbol in STOCK_SYMBOLS:
        try:
            quote = fetch_quote(symbol)
            if quote:
                producer.send('stock-quotes', value=quote)
                producer.flush()
                print(f"Sent {symbol}: {quote}")
        except Exception as e:
            print(f"Error producing {symbol}: {e}")
        time.sleep(1)
# welcome to the stock market data producer!

