import logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

logger.info("Scraper started")

import requests, json, time
from kafka import KafkaProducer
import schedule
import os

KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
API_KEY = open("api_key.txt").read().strip()
COUNTRIES = open("country_codes.txt").read().splitlines()


def create_kafka_producer(retries=10, delay=5):
    for attempt in range(1, retries + 1):
        try:
            print(f"Attempting Kafka connection ({attempt}/{retries})...")
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BROKER,
                value_serializer=lambda v: json.dumps(v).encode('utf-8')
            )
            print("Kafka connection established.")
            return producer
        except Exception as e:
            print(f"Kafka connection failed: {e}")
            time.sleep(delay)
    raise Exception("Kafka connection failed after all retries.")

producer = create_kafka_producer()

def fetch_trending(country):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={country}&maxResults=50&key={API_KEY}"
    response = requests.get(url)
    return response.json().get("items", [])

import os

def job():
    all_data = []  # collect everything to write to file

    for country in COUNTRIES:
        print(f"Fetching for {country}...")
        items = fetch_trending(country)
        print(f"Found {len(items)} items for {country}.")
        for item in items:
            data = {"region": country, "video": item}
            all_data.append(data)
            try:
                producer.send("youtube_trending", value=data).get(timeout=10)
            except Exception as e:
                print(f"Error sending to Kafka: {e}")

    # Save to JSON file
    output_path = os.path.join(os.getcwd(), "output.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(all_data)} records to {output_path}")

schedule.every(4).hours.do(job)
job()  # run once at startup

while True:
    schedule.run_pending()
    time.sleep(60)
