#!/usr/bin/env python3

import logging
import sys

import os
from dotenv import load_dotenv

from fetcher import *
from utils import *

from producer import *

def main():
    
    load_dotenv()

    logging.info("[YOUTUBE-WATCHER] Starting youtube-watcher")

    youtube_playlist_id = "PLE73iWqgrBhg0krhQuvy53ye-1eaf70rJ"

    google_api_key = os.getenv("google_api_key")
    all_video_items = fetch_playlist_items(google_api_key, youtube_playlist_id)

    Producer = producer()

    for video_item in all_video_items:
        video_id = video_item["contentDetails"]["videoId"]
        for video in fetch_videos(google_api_key, video_id):
            logging.info("[YOUTUBE-WATCHER] Got %s", (summarize_video(video)) )

            avro_payload = {
                "TITLE": video["snippet"]["title"],
                "VIEWS": int(video["statistics"].get("viewCount", 0)),
                "LIKES": int(video["statistics"].get("likeCount", 0)),
                "COMMENTS": int(video["statistics"].get("commentCount", 0)),
            }

            Producer.produce(
                topic="youtube_videos",
                key=video_id,
                value=avro_payload,
                on_delivery=delivery_report
            )

    Producer.flush()

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    sys.exit(main())