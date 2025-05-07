import requests
import psycopg2

API_KEY = open("/app/scraper/api_key.txt").read().strip()
DB_URL = "postgresql://admin:pass@cockroachdb:26257/videos"

def fetch_categories(region="US"):
    url = f"https://www.googleapis.com/youtube/v3/videoCategories?part=snippet&regionCode={region}&key={API_KEY}"
    response = requests.get(url)
    response.raise_for_status()
    return response.json().get("items", [])

def populate_categories():
    conn = psycopg2.connect(DB_URL)
    cursor = conn.cursor()

    categories = fetch_categories()
    for category in categories:
        category_id = category["id"]
        category_name = category["snippet"]["title"]
        try:
            cursor.execute(
                "INSERT INTO categories (category_id, category_name) VALUES (%s, %s) ON CONFLICT (category_id) DO NOTHING",
                (category_id, category_name),
            )
        except Exception as e:
            print(f"Error inserting category {category_id}: {e}")
            conn.rollback()
        else:
            conn.commit()

    cursor.close()
    conn.close()
    print("Categories populated successfully.")

if __name__ == "__main__":
    populate_categories()