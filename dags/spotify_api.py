import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from airflow.models import Variable
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
import os
from airflow.hooks.postgres_hook import PostgresHook

def chunks(lst: List[Any], n: int) -> List[Any]:
    """
    Yield successive n-sized chunks from lst.

    Args:
        lst (List[Any]): The list to be chunked.
        n (int): The chunk size.

    Yields:
        List[Any]: Chunks of the input list.
    """
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def extract_tracks_from_json(resp: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts relevant track information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (Dict[str, Any]): JSON response from Spotify API containing recently played tracks.

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

def extract_artists_from_json(resp: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts relevant artist information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (Dict[str, Any]): JSON response from Spotify API containing artist information.

    Returns:
        pd.DataFrame: DataFrame containing artist information.
    """
    artists = []

    for item in resp["artists"]:
        spotify_id = item["id"]
        name = item["name"]
        followers = item["followers"]["total"]
        genres = item["genres"]
        popularity = item["popularity"]
        uri = item["uri"]

        artist_element = {
            "spotify_id": spotify_id,
            "name": name,
            "followers": followers,
            "genres": genres,
            "popularity": popularity,
            "uri": uri
        }

        artists.append(artist_element)

    return pd.DataFrame(artists)

def convert_time(last_played_at: datetime) -> int:
    """
    Converts a timezone-aware datetime object to a Unix timestamp in milliseconds.

    Args:
        last_played_at (datetime): Datetime object of the last played track.

    Returns:
        int: Unix timestamp in milliseconds.
    """
    # Convert the datetime object to a Unix timestamp in milliseconds
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
    if df.shape[0] > 0:
        df = df.sort_values(by="played_at")
        # Save the DataFrame to a CSV file
        df.to_csv("dags/data/played.csv", index=False)
        logging.info(f"Retrieved {df.shape[0]} recently played tracks from Spotify.")
    else:
        logging.info(f"Retrieved no new played data from Spotify.")

def extract_artists(artist_ids: List[str]) -> None:
    """
    Extracts artist information from the Spotify API for the given list of artist IDs,
    converts the data to a DataFrame, and saves it as a CSV file.

    Args:
        artist_ids (List[str]): List of artist IDs to fetch information for.
    """
    # Prepare the Spotify API client
    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=Variable.get("SPOTIPY_CLIENT_ID"),
        client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
        redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
        cache_path="dags/.cache"
    ))

    if len(artist_ids) > 50:
        df_list = []
        for id_chunk in chunks(artist_ids, 50):
            # Send the request for artist information
            resp = sp.artists(id_chunk)
            # Extract relevant fields from the JSON response and store them in a DataFrame
            temp_df = extract_artists_from_json(resp)
            df_list.append(temp_df)
        df = pd.concat(df_list)
    elif 50 >= len(artist_ids) > 0:
        resp = sp.artists(artist_ids)
        df = extract_artists_from_json(resp)
    else:
        df = pd.DataFrame()

    if df.shape[0] > 0:
        # Save the DataFrame to a CSV file
        df.to_csv("dags/data/artist.csv", index=False)
        logging.info(f"Retrieved {df.shape[0]} artists from Spotify.")
    else:
        logging.info(f"Retrieved no new artist data from Spotify.")

def csv_to_postgresql(table_name: str, csv_path: str) -> None:
    """
    Loads the extracted Spotify data from a CSV file into the specified table in PostgreSQL.

    Args:
        table_name (str): The name of the PostgreSQL table to load data into.
        csv_path (str): The path to the CSV file containing the data.
    """
    if os.path.exists(csv_path):
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_path)
        cols = ", ".join(df.columns)
        
        # Connect to the PostgreSQL database
        postgres_hook = PostgresHook(postgres_conn_id="spotify_postgres")

        with postgres_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Create a temporary table to hold the data
                cursor.execute(f"""
                    CREATE TEMP TABLE tmp_{table_name}
                    ON COMMIT DROP
                    AS SELECT {cols}
                    FROM {table_name}
                    WITH NO DATA;
                """)
                
                # Copy data from the CSV file to the temporary table
                with open(csv_path, 'r') as f:
                    cursor.copy_expert(f"""
                        COPY tmp_{table_name} ({cols})
                        FROM stdin WITH CSV HEADER DELIMITER as ','
                    """, f)
                    
                # Insert data from the temporary table into the final table
                if table_name == "artist":
                    # We only want unique entries in this table, so we ignore entries that do no match the schema
                    cursor.execute(f"""
                        INSERT INTO {table_name} ({cols})
                        SELECT * FROM tmp_{table_name}
                        ON CONFLICT DO NOTHING;
                    """)
                else:
                    cursor.execute(f"""
                        INSERT INTO {table_name} ({cols})
                        SELECT * FROM tmp_{table_name};
                    """)
                
            conn.commit()
    else:
        logging.info(f"{csv_path} can't be found.")
