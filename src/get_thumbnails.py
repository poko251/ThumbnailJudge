from googleapiclient.discovery import build
import pandas as pd
import os
import requests
from PIL import Image
from io import BytesIO
from isodate import parse_duration

#access
api_key = "AIzaSyDbPYR_41A_toCjDp-fZp1ZEZwFeaaZ7Mw"
youtube = build("youtube", "v3", developerKey=api_key)

def get_video_list(channel_id, max_results=50):
    video_ids = []
    next_page_token = None

    while len(video_ids) < max_results:
        req = youtube.search().list(
            part="id",
            channelId=channel_id,
            maxResults=min(50, max_results-len(video_ids)),
            order="date",
            pageToken=next_page_token,
            type="video"
        )
        res = req.execute()
        for item in res["items"]:
            video_ids.append(item["id"]["videoId"])
        next_page_token = res.get("nextPageToken")
        if not next_page_token:
            break
    return video_ids

def get_video_details(video_ids):
    all_data = []
    for i in range(0, len(video_ids), 50):  # ApI allows to downlaod 50 at one time
        batch = video_ids[i:i+50]
        res = youtube.videos().list(
            part="snippet,statistics,contentDetails",
            id=",".join(batch)
        ).execute()
        for item in res["items"]:
            duration_iso = item["contentDetails"]["duration"]  # ISO 8601 format
            seconds = parse_duration(duration_iso).total_seconds()
            # Odfiltruj shortsy
            if seconds <= 60:
                continue
            video_id = item["id"]
            title = item["snippet"]["title"]
            published = item["snippet"]["publishedAt"]
            thumbnail_url = item["snippet"]["thumbnails"]["high"]["url"]
            stats = item["statistics"]
            views = int(stats.get("viewCount", 0))
            likes = int(stats.get("likeCount", 0))
            comments = int(stats.get("commentCount", 0))
            all_data.append({
                "video_id": video_id,
                "title": title,
                "published": published,
                "thumbnail_url": thumbnail_url,
                "views": views,
                "likes": likes,
                "comments": comments,
                "duration_seconds": seconds
            })
    return all_data

def save_metadata_and_thumbnails(data, out_csv="data/metadata.csv", img_folder="data/raw"):
    os.makedirs(img_folder, exist_ok=True)
    df = pd.DataFrame(data)
    df["thumbnail_path"] = None
    for idx, row in df.iterrows():
        url = row["thumbnail_url"]
        img_path = os.path.join(img_folder, f"{row['video_id']}.jpg")
        try:
            response = requests.get(url, timeout=10)
            img = Image.open(BytesIO(response.content))
            img.save(img_path)
            df.at[idx, "thumbnail_path"] = img_path
        except Exception as e:
            print(f"Błąd przy pobieraniu miniatury: {url} - {e}")
    df.to_csv(out_csv, index=False)

# ---- Użycie ----
if __name__ == "__main__":
    channel_id = "UCX6OQ3DkcsbYNE6H8uQQuVA"  # MrBeast 
    video_ids = get_video_list(channel_id, max_results=100)  
    print(f"Pobrano {len(video_ids)} ID filmów.")
    data = get_video_details(video_ids)
    print(f"Pobrano {len(data)} filmów (Shortsy odfiltrowane).")
    save_metadata_and_thumbnails(data)
    print("Zapisano metadane i miniatury do plików.")
