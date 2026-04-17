import json
import os
from datetime import datetime, timezone, timedelta

import requests

API_KEY = os.environ["YOUTUBE_API_KEY"]
BASE_URL = "https://www.googleapis.com/youtube/v3"

EXCLUDED_SPECIAL_CHANNELS = {"이재명", "대한민국정부", "KTV 이매진"}

with open("data/channels.json", "r", encoding="utf-8") as f:
    CHANNELS = json.load(f)

existing_data = {
    "updatedAt": "",
    "channels": [],
    "videos": [],
    "hotVideo": None
}

if os.path.exists("data/dashboard-data.json"):
    try:
        with open("data/dashboard-data.json", "r", encoding="utf-8") as f:
            existing_data = json.load(f)
    except Exception:
        pass


def get(url, params):
    r = requests.get(url, params=params, timeout=30)
    r.raise_for_status()
    return r.json()


def chunks(lst, size):
    for i in range(0, len(lst), size):
        yield lst[i:i + size]


def iso_to_date(s):
    try:
        return datetime.fromisoformat(s.replace("Z", "+00:00"))
    except Exception:
        return None


def fmt_video_url(video_id):
    return f"https://www.youtube.com/watch?v={video_id}"


def normalize_video(v):
    return {
        "group": v.get("group", ""),
        "channelName": v.get("channelName", ""),
        "handle": v.get("handle", ""),
        "channelId": v.get("channelId", ""),
        "videoId": v.get("videoId", ""),
        "videoUrl": v.get("videoUrl", ""),
        "title": v.get("title", ""),
        "publishedAt": v.get("publishedAt", ""),
        "thumbnail": v.get("thumbnail", ""),
        "viewCount": int(v.get("viewCount", 0)),
        "likeCount": int(v.get("likeCount", 0)),
        "commentCount": int(v.get("commentCount", 0))
    }


now = datetime.now(timezone.utc)
recent_cutoff = now - timedelta(days=30)
is_full_refresh_day = now.astimezone(timezone.utc).weekday() == 6

existing_video_map = {}
for v in existing_data.get("videos", []):
    if v.get("videoId"):
        existing_video_map[v["videoId"]] = normalize_video(v)

dashboard = {
    "updatedAt": datetime.now(timezone.utc).isoformat(),
    "channels": [],
    "videos": [],
    "hotVideo": None
}

updated_video_map = {}

for ch in CHANNELS:
    channel_id = ch["channelId"]

    channel_data = get(
        f"{BASE_URL}/channels",
        {
            "part": "snippet,statistics,contentDetails",
            "id": channel_id,
            "key": API_KEY
        }
    )

    items = channel_data.get("items", [])
    if not items:
        continue

    item = items[0]
    snippet = item.get("snippet", {})
    stats = item.get("statistics", {})
    uploads_playlist_id = item.get("contentDetails", {}).get("relatedPlaylists", {}).get("uploads")

    dashboard["channels"].append({
        "group": ch["group"],
        "includeInSubscriberBoard": ch.get("includeInSubscriberBoard", True),
        "name": ch["name"],
        "handle": ch["handle"],
        "channelId": channel_id,
        "channelUrl": f"https://www.youtube.com/channel/{channel_id}",
        "thumbnail": snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
        "subscriberCount": int(stats.get("subscriberCount", 0)),
        "videoCount": int(stats.get("videoCount", 0))
    })

    if not uploads_playlist_id:
        continue

    next_page_token = None
    candidate_video_ids = []

    while True:
        playlist_data = get(
            f"{BASE_URL}/playlistItems",
            {
                "part": "snippet,contentDetails",
                "playlistId": uploads_playlist_id,
                "maxResults": 50,
                "pageToken": next_page_token,
                "key": API_KEY
            }
        )

        stop_early = False

        for p_item in playlist_data.get("items", []):
            p_snippet = p_item.get("snippet", {})
            published_at = p_snippet.get("publishedAt")
            dt = iso_to_date(published_at)
            if not dt:
                continue

            if dt.year < 2026:
                stop_early = True
                break

            video_id = p_item.get("contentDetails", {}).get("videoId")
            if video_id:
                candidate_video_ids.append(video_id)

        if stop_early:
            break

        next_page_token = playlist_data.get("nextPageToken")
        if not next_page_token:
            break

    for video_id_batch in chunks(candidate_video_ids, 50):
        videos_data = get(
            f"{BASE_URL}/videos",
            {
                "part": "snippet,statistics",
                "id": ",".join(video_id_batch),
                "key": API_KEY
            }
        )

        for v in videos_data.get("items", []):
            v_snippet = v.get("snippet", {})
            v_stats = v.get("statistics", {})

            published_at = v_snippet.get("publishedAt")
            dt = iso_to_date(published_at)
            if not dt or dt.year != 2026:
                continue

            video_obj = {
                "group": ch["group"],
                "channelName": ch["name"],
                "handle": ch["handle"],
                "channelId": channel_id,
                "videoId": v["id"],
                "videoUrl": fmt_video_url(v["id"]),
                "title": v_snippet.get("title", ""),
                "publishedAt": published_at,
                "thumbnail": v_snippet.get("thumbnails", {}).get("medium", {}).get("url", "")
                             or v_snippet.get("thumbnails", {}).get("default", {}).get("url", ""),
                "viewCount": int(v_stats.get("viewCount", 0)),
                "likeCount": int(v_stats.get("likeCount", 0)),
                "commentCount": int(v_stats.get("commentCount", 0))
            }

            if is_full_refresh_day:
                updated_video_map[v["id"]] = video_obj
            else:
                if dt >= recent_cutoff:
                    updated_video_map[v["id"]] = video_obj
                else:
                    if v["id"] in existing_video_map:
                        updated_video_map[v["id"]] = existing_video_map[v["id"]]
                    else:
                        updated_video_map[v["id"]] = video_obj

dashboard["channels"].sort(key=lambda x: x["subscriberCount"], reverse=True)
dashboard["videos"] = sorted(updated_video_map.values(), key=lambda x: x["publishedAt"], reverse=True)

# 핫한 영상 계산 (전날 대비 조회수 증가) - 이재명/대한민국정부 제외
hot_video = None
max_delta = -1

for video in dashboard["videos"]:
    if video["channelName"] in EXCLUDED_SPECIAL_CHANNELS:
        continue

    prev = existing_video_map.get(video["videoId"])
    if prev:
        delta = int(video["viewCount"]) - int(prev.get("viewCount", 0))
        if delta > max_delta:
            max_delta = delta
            hot_video = {
                "group": video["group"],
                "channelName": video["channelName"],
                "title": video["title"],
                "videoUrl": video["videoUrl"],
                "thumbnail": video["thumbnail"],
                "publishedAt": video["publishedAt"],
                "viewCount": video["viewCount"],
                "deltaViews": delta
            }

dashboard["hotVideo"] = hot_video

os.makedirs("data", exist_ok=True)
with open("data/dashboard-data.json", "w", encoding="utf-8") as f:
    json.dump(dashboard, f, ensure_ascii=False, indent=2)

print("dashboard-data.json 업데이트 완료")
print(f"channels: {len(dashboard['channels'])}")
print(f"videos: {len(dashboard['videos'])}")
print(f"full refresh day: {is_full_refresh_day}")
