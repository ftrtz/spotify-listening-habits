# imports and preparations
import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from airflow.models import Variable
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging

def extract_tracks_from_json(resp: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts relevant track information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (dict): JSON response from Spotify API containing recently played tracks.

    Returns:
        pd.DataFrame: DataFrame containing track information.
    """
    track_list = []

    for item in resp["items"]:
        track_spotify_id = item["track"]["id"]
        track_name = item["track"]["name"]
        artists_spotify_id = list(map(lambda a: a["id"], item["track"]["artists"]))
        artists_name = list(map(lambda a: a["name"], item["track"]["artists"]))
        album_spotify_id = item["track"]["album"]["id"]
        album_name = item["track"]["album"]["name"]
        played_at = item["played_at"]

        track_element = {
            "track_spotify_id": track_spotify_id,
            "track_name": track_name,
            "artists_spotify_id": artists_spotify_id,
            "artists_name": artists_name,
            "album_spotify_id": album_spotify_id,
            "album_name": album_name,
            "played_at": played_at
        }

        track_list.append(track_element)

    return pd.DataFrame(track_list)


def convert_time(last_played_at: datetime) -> int:
    """
    Converts a timezone-aware datetime object to a Unix timestamp in milliseconds.

    Args:
        last_played_at (datetime): Datetime object of the last played track.

    Returns:
        int: Unix timestamp in milliseconds.
    """
    # Convert the datetime object to a Unix timestamp
    unix_timestamp = int(last_played_at.timestamp() * 1000)

    logging.info(f"Last played track was at {last_played_at} - Unix Timestamp: {unix_timestamp}")
    return unix_timestamp


def extract_recently_played(last_played_at: Optional[int] = None) -> None:
    """
    Extracts recently played tracks from the Spotify API since the given timestamp,
    converts the data to a DataFrame, and saves it as a CSV file.

    Args:
        last_played_at (Optional[int]): Unix timestamp in milliseconds of the last played track. 
                                        If None, fetches the most recent tracks.
    """
    # Prepare the Spotify API client with the required scope
    scope = "user-read-recently-played"
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=Variable.get("SPOTIPY_CLIENT_ID"),
        client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
        redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
        scope=scope,
        cache_path="dags/.cache"
    ))

    # Send the request for recently played tracks
    resp = sp.current_user_recently_played(limit=50, after=last_played_at)

    # Extract relevant fields from the JSON response and store them in a DataFrame
    df = extract_tracks_from_json(resp)
    if df is not None:
        df = df.sort_values(by="played_at")
        # Save the DataFrame to a CSV file
        df.to_csv("dags/data/spotify.csv", index=False)
        logging.info(f"Retrieved {df.shape[0]} recently played tracks from Spotify.")
    else:
        logging.info(f"Retrieved no new data from Spotify.")

    
