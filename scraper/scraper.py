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

snippet_features = ["title",
                    "publishedAt",
                    "channelId",
                    "channelTitle",
                    "categoryId"]

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

def get_videos(items):
    videos = []
    for video in items:
        if "statistics" not in video:
            continue

        video_id = video['id']

        snippet = video['snippet']
        statistics = video['statistics']

        # Basic features from snippet
        snippet_data = {feature: snippet.get(feature, "") for feature in snippet_features}

        description = snippet.get("description", "")
        thumbnail_link = snippet.get("thumbnails", {}).get("default", {}).get("url", "")
        trending_date = time.strftime("%Y-%m-%d")
        tags = snippet.get("tags", ["[none]"])
        view_count = statistics.get("viewCount", 0)

        # Ratings
        if 'likeCount' in statistics and 'dislikeCount' in statistics:
            likes = statistics['likeCount']
            dislikes = statistics['dislikeCount']
            ratings_disabled = False
        else:
            likes = 0
            dislikes = 0
            ratings_disabled = True

        # Comments
        if 'commentCount' in statistics:
            comment_count = statistics['commentCount']
            comments_disabled = False
        else:
            comment_count = 0
            comments_disabled = True

        video_data = {
            "video_id": video_id,
            "trending_date": trending_date,
            "tags": tags,
            "view_count": view_count,
            "likes": likes,
            "dislikes": dislikes,
            "comment_count": comment_count,
            "thumbnail_link": thumbnail_link,
            "description": description
        }

        video_data.update(snippet_data)

        videos.append(video_data)

    return videos


def fetch_trending(country):
    url = f"https://www.googleapis.com/youtube/v3/videos?part=snippet,statistics&chart=mostPopular&regionCode={country}&maxResults=50&key={API_KEY}"
    response = requests.get(url)
    return response.json().get("items", [])

import os

def job():
    all_data = []  

    for country in COUNTRIES:
        print(f"Fetching for {country}...")
        items = fetch_trending(country)
        print(f"Found {len(items)} items for {country}.")
        
        parsed_videos = get_videos(items) 

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
