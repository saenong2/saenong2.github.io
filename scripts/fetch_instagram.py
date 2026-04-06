import json
import os
from datetime import datetime, timezone
from pathlib import Path

import requests

BASE_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = BASE_DIR / "data"
AGENCIES_FILE = DATA_DIR / "agencies.json"
ACCOUNTS_FILE = DATA_DIR / "accounts.json"
POSTS_FILE = DATA_DIR / "posts.json"

GRAPH_API_VERSION = "v23.0"
GRAPH_API_BASE = f"https://graph.facebook.com/{GRAPH_API_VERSION}"


def load_agencies():
    with open(AGENCIES_FILE, "r", encoding="utf-8") as f:
        raw = json.load(f)
    return raw["agencies"] if isinstance(raw, dict) and "agencies" in raw else raw


def safe_int(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def parse_timestamp(ts):
    if not ts:
        return ""
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).strftime("%Y-%m-%d")
    except Exception:
        return ts[:10]


def fetch_business_discovery(ig_user_id, access_token, username):
    fields = (
        f"business_discovery.username({username})"
        "{"
        "username,"
        "followers_count,"
        "media_count,"
        "profile_picture_url,"
        "media.limit(6)"
        "{"
        "id,"
        "caption,"
        "comments_count,"
        "like_count,"
        "media_product_type,"
        "media_type,"
        "media_url,"
        "thumbnail_url,"
        "permalink,"
        "timestamp"
        "}"
        "}"
    )

    url = f"{GRAPH_API_BASE}/{ig_user_id}"
    params = {
        "fields": fields,
        "access_token": access_token,
    }

    response = requests.get(url, params=params, timeout=30)
    response.raise_for_status()
    data = response.json()

    if "business_discovery" not in data:
        raise ValueError(f"business_discovery 응답 없음: {username}")

    return data["business_discovery"]


def detect_type(media_item):
    media_product_type = (media_item.get("media_product_type") or "").upper()
    media_type = (media_item.get("media_type") or "").upper()

    if media_product_type == "REELS":
        return "reel"
    if media_type == "CAROUSEL_ALBUM":
        return "carousel"
    return "feed"


def build_outputs(agencies, ig_user_id, access_token):
    accounts = []
    posts = []

    for agency in agencies:
        if not agency.get("active", True):
            continue

        username = agency["instagram"]

        try:
            bd = fetch_business_discovery(ig_user_id, access_token, username)
        except Exception as e:
            print(f"[WARN] {agency['agency']} 수집 실패: {e}")
            continue

        media_items = bd.get("media", {}).get("data", []) if isinstance(bd.get("media"), dict) else []

        accounts.append({
            "agency": agency["agency"],
            "display_name": agency.get("display_name", agency["agency"]),
            "instagram": agency["instagram"],
            "url": agency["url"],
            "level": agency["level"],
            "followers": safe_int(bd.get("followers_count")) or 0,
            "post_count": safe_int(bd.get("media_count")) or 0,
            "weekly_posts": None,
            "last_upload": parse_timestamp(media_items[0].get("timestamp")) if media_items else "",
            "profile_picture_url": bd.get("profile_picture_url", "")
        })

        for item in media_items:
            thumb = item.get("thumbnail_url") or item.get("media_url") or ""
            posts.append({
                "agency": agency["agency"],
                "display_name": agency.get("display_name", agency["agency"]),
                "instagram": agency["instagram"],
                "url": agency["url"],
                "level": agency["level"],
                "date": parse_timestamp(item.get("timestamp")),
                "type": detect_type(item),
                "caption": (item.get("caption") or "")[:180],
                "thumbnail": thumb,
                "link": item.get("permalink") or agency["url"],
                "views": None,
                "likes": safe_int(item.get("like_count")),
                "comments": safe_int(item.get("comments_count")),
                "timestamp_raw": item.get("timestamp") or ""
            })

    accounts.sort(key=lambda x: (x["level"], -x["followers"], x["agency"]))
    posts.sort(key=lambda x: x.get("timestamp_raw", ""), reverse=True)

    for p in posts:
        p.pop("timestamp_raw", None)

    return accounts, posts


def save_json(path, data):
    path.parent.mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main():
    ig_user_id = os.getenv("IG_USER_ID", "").strip()
    access_token = os.getenv("IG_ACCESS_TOKEN", "").strip()

    if not ig_user_id:
        raise RuntimeError("IG_USER_ID 비밀값이 없습니다.")
    if not access_token:
        raise RuntimeError("IG_ACCESS_TOKEN 비밀값이 없습니다.")

    agencies = load_agencies()
    accounts, posts = build_outputs(agencies, ig_user_id, access_token)

    save_json(ACCOUNTS_FILE, accounts)
    save_json(POSTS_FILE, posts)

    meta_info = {
        "updated_at_utc": datetime.now(timezone.utc).isoformat(),
        "accounts_count": len(accounts),
        "posts_count": len(posts)
    }
    save_json(DATA_DIR / "last_updated.json", meta_info)

    print(f"accounts.json 저장 완료: {len(accounts)}건")
    print(f"posts.json 저장 완료: {len(posts)}건")


if __name__ == "__main__":
    main()
