from kafka import KafkaConsumer
import json
import psycopg2
from langdetect import detect

# Database connection string
DB_URL = "postgresql://m_mind:JvARh8lNgcKwExvuGxM4ew@big-data-project-6437.jxf.gcp-europe-west3.cockroachlabs.cloud:26257/trending?sslmode=verify-full"

# Connect to the Database
conn = psycopg2.connect(DB_URL)
cursor = conn.cursor()

#Consumer setup
consumer = KafkaConsumer(
    "youtube_trending",
    bootstrap_servers="kafka:9092",
    group_id="youtube_trending_group",
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="earliest",
    enable_auto_commit=False
)

def transform_video(video):
    # Extract fields
    video_id = video.get("video_id", None)
    region = video.get("region", "unknown")
    title = video.get("title", None)
    category_id = video.get("categoryId", None)
    published_at = video.get("publishedAt", None)
    trending_at = video.get("trending_date", None)
    views = int(video.get("view_count", 0))
    comments = int(video.get("comment_count", 0))

    language = detect(title) if title else None

    return {
        "video_id": video_id,
        "title": title,
        "category_id": category_id,
        "region": region,
        "published_at": published_at,
        "trending_at": trending_at,
        "views": views,
        "comments": comments,
        "language": language,
    }

upsert_sql = """
UPSERT INTO videos (
  video_id, title, category_id, region,
  published_at, trending_at, views,
  comments, language
) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

for msg in consumer:
    video = msg.value
    try:
        transformed_video = transform_video(video)
        cursor.execute(upsert_sql, (
            transformed_video["video_id"], transformed_video["title"], transformed_video["category_id"],
            transformed_video["region"], transformed_video["published_at"], transformed_video["trending_at"],
            transformed_video["views"], transformed_video["comments"], transformed_video["language"]
        ))
        conn.commit()
        consumer.commit()
    except Exception as e:
        conn.rollback()  # Rollback if there is an error 
        print(f"Error processing video: {e}")
        print(f"Failed video data: {video}")