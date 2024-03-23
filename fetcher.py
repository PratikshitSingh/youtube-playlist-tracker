#!usr/bin/env python3

import requests
import json

def fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/playlistItems", params={
        "key": google_api_key,
        "playlistId": youtube_playlist_id,
        "part": "contentDetails",
        "pageToken": page_token,
    })

    payload = json.loads(response.text)
    return payload

def fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=None):
    payload = fetch_playlist_items_page(google_api_key, youtube_playlist_id, page_token=page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token:
        yield from fetch_playlist_items(google_api_key, youtube_playlist_id, page_token=next_page_token)

def fetch_videos_page(google_api_key, video_id, page_token=None):
    response = requests.get("https://www.googleapis.com/youtube/v3/videos", params={
        "key": google_api_key,
        "id": video_id,
        "part": "snippet, statistics",
        "pageToken": page_token,
    })

    payload = json.loads(response.text)
    return payload

def fetch_videos(google_api_key, video_id, page_token=None):
    payload = fetch_videos_page(google_api_key, video_id, page_token=page_token)

    yield from payload["items"]

    next_page_token = payload.get("nextPageToken")

    if next_page_token:
        yield from fetch_playlist_items(google_api_key, video_id, page_token=next_page_token)