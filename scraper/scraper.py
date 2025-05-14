from datetime import datetime
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

features = ["title", "publishedAt", "channelId", "channelTitle", "categoryId"]
PREV_FILE = "prev_videos.json" 

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

def load_previous_videos():
    if os.path.exists(PREV_FILE):
        with open(PREV_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {}

def save_current_videos(video_dict):
    with open(PREV_FILE, "w", encoding="utf-8") as f:
        json.dump(video_dict, f, ensure_ascii=False, indent=2)

def assign_trending_dates(current_videos):
    previous_videos = load_previous_videos()
    updated_video_dict = previous_videos.copy()
    results = []

    for video in current_videos:
        video_id = video["video_id"]
        
        if video_id not in previous_videos:
            trending_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            updated_video_dict[video_id] = trending_date
            video["trending_date"] = trending_date
        else:
            video["trending_date"] = previous_videos[video_id]

        results.append(video)

    save_current_videos(updated_video_dict)
    return results


def get_videos(items):
    videos = []
    for video in items:
        if "statistics" not in video:
            continue

        video_id = video['id']

        snippet = video['snippet']
        statistics = video['statistics']

        data = {feature: snippet.get(feature, "") for feature in features}

        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", {}).get("default", {}).get("url", "")
        tags = snippet.get("tags", ["[none]"])
        view_count = statistics.get("viewCount", 0)
        comment_count = statistics.get("commentCount", 0)


        video_data = {
            "video_id": video_id,
            "tags": tags,
            "view_count": view_count,
            "comment_count": comment_count,
            "thumbnail_link": thumbnail_link,
            "description": description
        }

        video_data.update(data)

        videos.append(video_data)

    return videos


def fetch_trending(country):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={country}&maxResults=50&key={API_KEY}"
    response = requests.get(url)
    return response.json().get("items", [])

import os

def job():
    all_data = []  
    with open("country_codes.txt") as f:
        countries = f.read().splitlines() 

    for country in countries:
        print(f"Fetching for {country}...")
        items = fetch_trending(country)
        print(f"Found {len(items)} items for {country}.")
        
        parsed_videos = get_videos(items) 
        parsed_videos = assign_trending_dates(parsed_videos)
        
        for video_data in parsed_videos:
            video_data["region"] = country  
            all_data.append(video_data)

            try:
                producer.send("youtube_trending", value=video_data).get(timeout=10)
            except Exception as e:
                print(f"Error sending to Kafka: {e}")

    # Save to JSON file
    output_path = os.path.join(os.getcwd(), "output.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(all_data, f, ensure_ascii=False, indent=2)
        print(f"Saved {len(all_data)} records to {output_path}")

schedule.every(4).hours.do(job)
job()  

while True:
    schedule.run_pending()
    time.sleep(60)
