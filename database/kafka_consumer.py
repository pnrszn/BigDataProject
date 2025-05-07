from kafka import KafkaConsumer
import json
import psycopg2
from langdetect import detect

# Database connection string
DB_URL = "postgresql://admin:pass@cockroachdb:26257/videos"

# Establish connection
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

consumer = KafkaConsumer(
    "youtube_trending",
    bootstrap_servers="kafka:9092",
    group_id="youtube_trending_group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False
)

def transform_video(video):
    # Extract nested fields
    video_data = video.get("video", {})
    region = video.get("region", "unknown")

    # Handle missing fields
    video_id = video_data.get("id", None)
    title = video_data.get("snippet", {}).get("title", None)
    category_id = video_data.get("snippet", {}).get("categoryId", None)
    published_at = video_data.get("snippet", {}).get("publishedAt", None)
    views = int(video_data.get("statistics", {}).get("viewCount", 0))
    likes = int(video_data.get("statistics", {}).get("likeCount", 0))
    comments = int(video_data.get("statistics", {}).get("commentCount", 0))

    # Compute like rate
    like_rate = likes / views if views else 0

    # Detect language
    language = detect(title) if title else None

    return {
        "video_id": video_id,
        "title": title,
        "category_id": category_id,
        "region": region,
        "published_at": published_at,
        "views": views,
        "likes": likes,
        "comments": comments,
        "like_rate": like_rate,
        "language": language,
    }

upsert_sql = """
UPSERT INTO videos (
  video_id, title, category_id, region,
  published_at, trending_at, views, likes,
  comments, like_rate, language
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for msg in consumer:
    video = msg.value
    try:
        transformed_video = transform_video(video)
        cursor.execute(upsert_sql, (
            transformed_video["video_id"], transformed_video["title"], transformed_video["category_id"],
            transformed_video["region"], transformed_video["published_at"], None,  # trending_at is not provided
            transformed_video["views"], transformed_video["likes"], transformed_video["comments"],
            transformed_video["like_rate"], transformed_video["language"]
        ))
        conn.commit()
        consumer.commit()
    except Exception as e:
        conn.rollback()  # Roll back the transaction on error
        print(f"Error processing video: {e}")
        print(f"Failed video data: {video}")