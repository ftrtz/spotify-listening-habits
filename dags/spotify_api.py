import pandas as pd
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from datetime import datetime
from typing import Optional, List, Dict, Any
import logging
import os
import json

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

def parse_datetime(datetime_string):
    formats = [
        "%Y-%m-%dT%H:%M:%S.%fZ",  # Format with milliseconds
        "%Y-%m-%dT%H:%M:%S%z"      # Format without milliseconds
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(datetime_string, fmt)
        except ValueError:
            continue
    raise ValueError(f"time data '{datetime_string}' does not match any of the formats")

def extract_played_from_json(resp: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts recently played tracks information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (Dict[str, Any]): JSON response from Spotify API containing recently played tracks.

    Returns:
        pd.DataFrame: DataFrame containing track information.
    """
    tracks = []

    for item in resp["items"]:
        played_at = item["played_at"]
        track_id = item["track"]["id"]
        track_name = item["track"]["name"]
        popularity = item["track"]["popularity"]
        duration_ms = item["track"]["duration_ms"]
        artist_ids = list(map(lambda a: a["id"], item["track"]["artists"]))
        artist_names = list(map(lambda a: a["name"], item["track"]["artists"]))
        album_id = item["track"]["album"]["id"]
        album_name = item["track"]["album"]["name"]
        album_images = json.dumps(item["track"]["album"]["images"])
        track_uri = item["track"]["uri"]

        # parse time and convert to unix timestamp
        unix_timestamp = int(parse_datetime(played_at).timestamp() * 1000)

        track_element = {
            "unix_timestamp": unix_timestamp,
            "played_at": played_at,
            "track_id": track_id,
            "track_name": track_name,
            "popularity": popularity,
            "duration_ms": duration_ms,
            "artist_ids": artist_ids,
            "artist_names": artist_names,
            "album_id": album_id,
            "album_name": album_name,
            "album_images": album_images,
            "track_uri": track_uri
        }

        tracks.append(track_element)

    return pd.DataFrame(tracks)

def extract_audio_features_from_json(resp: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts audio features information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (Dict[str, Any]): JSON response from Spotify API containing tracks audio features.

    Returns:
        pd.DataFrame: DataFrame containing audio features information.
    """
    audio_features = []

    for item in resp:
        track_id = item["id"]
        danceability = item["danceability"]
        energy = item["energy"]
        key = item["key"]
        loudness = item["loudness"]
        mode = item["mode"]
        speechiness = item["speechiness"]
        acousticness = item["acousticness"]
        instrumentalness = item["instrumentalness"]
        liveness = item["liveness"]
        valence = item["valence"]
        tempo = item["tempo"]
        time_signature = item["time_signature"]
        analysis_url = item["analysis_url"]


        audio_features_element = {
            "track_id": track_id,
            "danceability": danceability,
            "energy": energy,
            "key": key,
            "loudness": loudness,
            "mode": mode,
            "speechiness": speechiness,
            "acousticness": acousticness,
            "instrumentalness": instrumentalness,
            "liveness": liveness,
            "valence": valence,
            "tempo": tempo,
            "time_signature": time_signature,
            "analysis_url": analysis_url
        }

        audio_features.append(audio_features_element)

    return pd.DataFrame(audio_features)

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
        id = item["id"]
        name = item["name"]
        followers = item["followers"]["total"]
        genres = "{" + ",".join(f'"{genre}"' for genre in item["genres"]) + "}"
        popularity = item["popularity"]
        uri = item["uri"]
        images = json.dumps(item["images"])

        artist_element = {
            "id": id,
            "name": name,
            "followers": followers,
            "genres": genres,
            "popularity": popularity,
            "uri": uri,
            "images": images
        }

        artists.append(artist_element)

    return pd.DataFrame(artists)

def get_recently_played(sp, last_played_at: Optional[int] = None) -> pd.DataFrame:
    """
    Extracts recently played tracks from the Spotify API since the given timestamp,
    converts the data to a DataFrame, and saves it as a CSV file.

    Args:
        sp (spotipy.Spotify): The Spotify API client.
        last_played_at (Optional[int]): Unix timestamp in milliseconds of the last played track. 
                                        If None, fetches the most recent tracks.
    
    Returns:
        pd.DataFrame: DataFrame containing recently played track information.
    """
    # Send the request for recently played tracks
    resp = sp.current_user_recently_played(limit=50, after=last_played_at)

    # Extract relevant fields from the JSON response and store them in a DataFrame
    df = extract_played_from_json(resp)
    return df

def get_audio_features(sp, track_ids: List[str]) -> pd.DataFrame:
    """
    Extracts tracks audio features from the Spotify API,
    converts the data to a DataFrame, and saves it as a CSV file.

    Args:
        sp (spotipy.Spotify): The Spotify API client.
        track_ids (List[str]): List of track IDs to fetch information for.
    
    Returns:
        pd.DataFrame: DataFrame containing recently played track information.
    """
    logging.info(f"calling spotify api for {track_ids}")
    # Send the request for recently played tracks
    resp = sp.audio_features(tracks=track_ids)

    logging.info(f"parsing json {resp}")
    # Extract relevant fields from the JSON response and store them in a DataFrame
    df = extract_audio_features_from_json(resp)


    logging.info(f"got dataframe {df}")

    if df.shape[0] > 0:
        logging.info("saving csv")
        # Save the DataFrame to a CSV file
        df.to_csv("dags/data/audio_features.csv", index=False)
    
        logging.info(f"Retrieved {df.shape[0]} tracks audio features from Spotify.")
    else:
        logging.info(f"Retrieved no audio features data from Spotify.")

    return df

def get_artists(sp, artist_ids: List[str]) -> pd.DataFrame:
    """
    Extracts artist information from the Spotify API for the given list of artist IDs,
    converts the data to a DataFrame, and saves it as a CSV file.

    Args:
        sp (spotipy.Spotify): The Spotify API client.
        artist_ids (List[str]): List of artist IDs to fetch information for.

    Returns:
        pd.DataFrame: DataFrame containing artist information.
    """
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
    
    return df

def transform_played(played: pd.DataFrame) -> None:
    """
    Transforms the recently played DataFrame by sorting, exploding, and saving it into CSV files.

    Args:
        played (pd.DataFrame): DataFrame containing recently played track information.
    """
    played = played.sort_values(by="played_at")

    # Explode to create track_artist DataFrame
    track_artist = played[["track_id", "artist_ids"]]
    track_artist = track_artist.assign(
        artist_position=track_artist['artist_ids'].apply(lambda x: list(range(len(x))))
        )
    track_artist = track_artist.explode(["artist_ids", "artist_position"])
    track_artist = track_artist.rename(columns={"artist_ids": "artist_id"})
    track_artist = track_artist.drop_duplicates()

    # Create track DataFrame
    track = played[["track_id", "track_name", "popularity", "duration_ms", "album_id", "album_name", "album_images", "track_uri"]]
    track = track.rename(columns={"track_id": "id", "track_name": "name", "track_uri": "uri"})
    track = track.drop_duplicates(subset=["id"])

    # Filter final played columns
    played = played[["unix_timestamp", "played_at", "track_id"]]

    # Save the DataFrames to CSV files
    track_artist.to_csv("dags/data/track_artist.csv", index=False)
    track.to_csv("dags/data/track.csv", index=False)
    played.to_csv("dags/data/played.csv", index=False)


def csv_to_postgresql(hook, table_name: str, csv_path: str) -> None:
    """
    Loads the extracted Spotify data from a CSV file into the specified table in PostgreSQL.

    Args:
        hook (PostgresHook): The PostgresHook to interact with PostgreSQL.
        table_name (str): The name of the PostgreSQL table to load data into.
        csv_path (str): The path to the CSV file containing the data.
    """
    if os.path.exists(csv_path):
        # Read the CSV file into a DataFrame
        df = pd.read_csv(csv_path)
        cols = ", ".join(df.columns)

        with hook.get_conn() as conn:
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
                if table_name in ["artist", "track", "audio_features", "track_artist"]:
                    # We only want unique entries in these tables, so we ignore entries that do not match the schema
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
