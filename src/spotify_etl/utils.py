from sqlalchemy import MetaData, Table, Engine, text
import pandas as pd
import spotipy
from datetime import datetime
from dateutil import parser
from typing import Optional, List, Dict, Any, Tuple
import logging
import os
import json
import glob
import time
from jinja2 import Template

# Helper functions
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

# ======================== Main functions

# Extraction from JSON response
def extract_recently_played(resp: Dict[str, Any]) -> pd.DataFrame:
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
        if item is not None:

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
            unix_timestamp = int(parser.parse(played_at).timestamp() * 1000)

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

def extract_tracks(resp: Dict[str, Any]) -> pd.DataFrame:
    """
    Extracts track information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (Dict[str, Any]): JSON response from Spotify API containing track information.

    Returns:
        pd.DataFrame: DataFrame containing track information.
    """
    tracks = []

    for item in resp["tracks"]:
        if item is not None:

            id = item["id"]
            name = item["name"]
            popularity = item["popularity"]
            duration_ms = item["duration_ms"]
            artist_ids = list(map(lambda a: a["id"], item["artists"]))
            artist_names = list(map(lambda a: a["name"], item["artists"]))
            album_id = item["album"]["id"]
            album_name = item["album"]["name"]
            album_images = json.dumps(item["album"]["images"])
            uri = item["uri"]

            track_element = {
                "id": id,
                "name": name,
                "popularity": popularity,
                "duration_ms": duration_ms,
                "artist_ids": artist_ids,
                "artist_names": artist_names,
                "album_id": album_id,
                "album_name": album_name,
                "album_images": album_images,
                "uri": uri
            }

            tracks.append(track_element)

    return pd.DataFrame(tracks)

def extract_audio_features(resp: List[Dict[str, Any]]) -> pd.DataFrame:
    """
    Extracts audio features information from the Spotify API JSON response and 
    saves it into a pandas DataFrame.

    Args:
        resp (List[Dict[str, Any]]): JSON response from Spotify API containing audio features.

    Returns:
        pd.DataFrame: DataFrame containing audio features information.
    """
    audio_features = []

    for item in resp:
        if item is not None:
            
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

def extract_artists(resp: Dict[str, Any]) -> pd.DataFrame:
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
        if item is not None:

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

# Get functions
def get_played_from_history(history_path: str) -> pd.DataFrame:
    """
    Extracts played tracks from the Spotify streaming history files and saves it into
    a pandas DataFrame.

    Args:
        history_path (str): Path to the folder containing the Spotify streaming history in JSON format.

    Returns:
        pd.DataFrame: DataFrame containing played tracks information.
    """
    played_history = []
    missing_uri_count = 0

    history_files = glob.glob(history_path + "/Streaming_History_Audio_*.json")
    if not history_files:
        logging.error(f"No streaming history in {os.path.join(os.getcwd(), history_path)}")
    for file in history_files:
        with open(file, 'r') as f:
            history = json.load(f)

            for item in history:
                unix_timestamp = int(parser.parse(item["ts"]).timestamp() * 1000)
                played_at = item["ts"]
                spotify_track_uri = item["spotify_track_uri"]
                if spotify_track_uri and spotify_track_uri is not None:
                    track_id = spotify_track_uri.split("spotify:track:")[1]

                    history_element = {
                        "unix_timestamp": unix_timestamp,
                        "played_at": played_at,
                        "track_id": track_id
                    }

                    played_history.append(history_element)
                else:
                    missing_uri_count += 1

    played_history_df = pd.DataFrame(played_history)

    logging.info(f"Extracted {str(played_history_df.shape[0])} entries from streaming history")
    logging.info(f"Skipped {str(missing_uri_count)} entries due to missing URIs")

    return played_history_df

def get_recently_played(sp: spotipy.Spotify, last_played_at: Optional[int] = None) -> pd.DataFrame:
    """
    Extracts recently played tracks from the Spotify API since the given timestamp,
    converts the data to a DataFrame.

    Args:
        sp (spotipy.Spotify): Authenticated Spotify object.
        last_played_at (Optional[int]): Unix timestamp indicating the cutoff time. Defaults to None.

    Returns:
        pd.DataFrame: DataFrame containing recently played tracks.
    """
    # Send the request for recently played tracks
    resp = sp.current_user_recently_played(limit=50, after=last_played_at)

    # Extract relevant fields from the JSON response and store them in a DataFrame
    df = extract_recently_played(resp)
    return df

def get_track_from_played(played):
    # Create track information DataFrame
    track = played[["track_id", "track_name", "popularity", "duration_ms", "artist_ids", "artist_names", "album_id", "album_name", "album_images", "track_uri"]]
    track = track.rename(columns={"track_id": "id", "track_name": "name", "track_uri": "uri"})
    track = track.drop_duplicates(subset=["id"])
    return track


def get_tracks(sp: spotipy.Spotify, track_ids: List[str], chunksize: int = 50) -> pd.DataFrame:
    """
    Extracts track information from the Spotify API for a list of track IDs, 
    converts the data to a pandas DataFrame.

    Args:
        sp (spotipy.Spotify): The Spotify API client.
        track_ids (List[str]): List of track IDs to fetch information for.
        chunksize (int, optional): The number of track IDs to process per API call. Defaults to 50.
    
    Returns:
        pd.DataFrame: DataFrame containing track information.
    """
    logging.info(f"Calling Spotify API for {str(len(track_ids))} tracks.")

    df_list = []
    for id_chunk in chunks(track_ids, chunksize):
        # Send the request for track information
        resp = sp.tracks(id_chunk)
        # Extract relevant fields from the JSON response and store them in a DataFrame
        temp_df = extract_tracks(resp)
        df_list.append(temp_df)

    df = pd.concat(df_list)
    return df

def get_audio_features(sp: spotipy.Spotify, track_ids: List[str], chunksize: int = 50) -> pd.DataFrame:
    """
    Extracts audio features for a list of track IDs from the Spotify API,
    converts the data to a pandas DataFrame.

    Args:
        sp (spotipy.Spotify): The Spotify API client.
        track_ids (List[str]): List of track IDs to fetch audio features for.
        chunksize (int, optional): The number of track IDs to process per API call. Defaults to 50.
    
    Returns:
        pd.DataFrame: DataFrame containing audio features for the tracks.
    """
    logging.info(f"Calling Spotify API for {str(len(track_ids))} tracks.")

    df_list = []
    count = 0
    for id_chunk in chunks(track_ids, chunksize):
        # Send the request for audio features
        resp = sp.audio_features(id_chunk)
        # Extract relevant fields from the JSON response and store them in a DataFrame
        temp_df = extract_audio_features(resp)
        df_list.append(temp_df)

        # Delay for rate limiting
        count += 1
        if (count / 10).is_integer():
            time.sleep(5)

    df = pd.concat(df_list)
    return df

def get_artists(sp: spotipy.Spotify, artist_ids: List[str], chunksize: int = 50) -> pd.DataFrame:
    """
    Extracts artist information for a list of artist IDs from the Spotify API,
    converts the data to a pandas DataFrame.

    Args:
        sp (spotipy.Spotify): The Spotify API client.
        artist_ids (List[str]): List of artist IDs to fetch information for.
        chunksize (int, optional): The number of artist IDs to process per API call. Defaults to 50.
    
    Returns:
        pd.DataFrame: DataFrame containing artist information.
    """
    logging.info(f"Calling Spotify API for {str(len(artist_ids))} artists.")

    df_list = []
    for id_chunk in chunks(artist_ids, chunksize):
        # Send the request for artist information
        resp = sp.artists(id_chunk)
        # Extract relevant fields from the JSON response and store them in a DataFrame
        temp_df = extract_artists(resp)
        df_list.append(temp_df)

    df = pd.concat(df_list)
    return df


def clean_track_and_played(track: pd.DataFrame, played: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Cleans the track and played DataFrames by dropping rows with missing values 
    and ensuring that the played DataFrame only contains tracks present in the track DataFrame.
    Filters out duplicate tracks within a 30-second time window.

    Args:
        track (pd.DataFrame): DataFrame containing track information.
        played (pd.DataFrame): DataFrame containing played track information.

    Returns:
        Tuple[pd.DataFrame, pd.DataFrame]: 
            - Cleaned track DataFrame.
            - Cleaned played DataFrame, filtered to include only tracks present in the track DataFrame.
    """
    # Drop null values from track DataFrame
    track_len_unfiltered = track.shape[0]
    track = track.replace("", None)
    track = track.dropna(subset=["id", "name", "popularity", "duration_ms", "album_id", "album_name", "uri"], how="any", axis=0)
    logging.info(f"Removed {track_len_unfiltered - track.shape[0]} rows from track containing missing values.")

    # Ensure "played" only contains track IDs that exist in the "track" DataFrame
    played_len_unfiltered = played.shape[0]
    played = played[played["track_id"].isin(track["id"])]
    logging.info(f"Removed {played_len_unfiltered - played.shape[0]} rows from played because track info is missing.")

    # Select final played columns
    played = played[["unix_timestamp", "played_at", "track_id"]]

    # remove duplicate tracks in a 30 sec window from played
    played["timediff"] = played.sort_values(['track_id','unix_timestamp']).groupby('track_id')['unix_timestamp'].diff()
    to_delete = played[played['timediff'] < 30000]['unix_timestamp']
    played = played[~played['unix_timestamp'].isin(to_delete)].drop("timediff", axis=1)

    logging.info(f"Deleted {to_delete.shape[0]} tracks that were duplicate in a 30sec time window from played.")

    return track, played

def create_track_artist(track: pd.DataFrame) -> pd.DataFrame:
    """
    Transforms the track DataFrame by exploding the artist IDs into individual rows and 
    creating a DataFrame linking tracks with their respective artists.

    Args:
        track (pd.DataFrame): DataFrame containing track information.

    Returns:
        pd.DataFrame: DataFrame linking tracks with artists.
    """
    # Create track-artist relationship DataFrame
    track_artist = track[["id", "artist_ids"]]
    track_artist = track_artist.assign(
        artist_position=track_artist['artist_ids'].apply(lambda x: list(range(len(x))))
    )
    track_artist = track_artist.explode(["artist_ids", "artist_position"])
    track_artist = track_artist.rename(columns={"id": "track_id", "artist_ids": "artist_id"})
    track_artist = track_artist.drop_duplicates()

    # Ensure that artist_position is an integer type
    track_artist['artist_position'] = track_artist['artist_position'].astype(int)

    return track_artist

def finalize_track(track: pd.DataFrame) -> pd.DataFrame:
    """
    Finalizes the track DataFrame by renaming columns, removing duplicates, 
    and structuring the track information.

    Args:
        track (pd.DataFrame): DataFrame containing track information.

    Returns:
        pd.DataFrame: Finalized DataFrame containing unique track information.
    """
    # Finalize track DataFrame
    track = track[["id", "name", "popularity", "duration_ms", "album_id", "album_name", "album_images", "uri"]]
    track = track.drop_duplicates(subset=["id"])
    
    return track


def csv_to_staging(engine: Engine, table_name: str, csv_path: str, sql_path: str, staging_schema: str, prod_schema: str) -> None:
    """
    Loads data from a CSV file into a staging table in PostgreSQL. The function first creates a staging table
    based on the structure of the production table, then imports the data from the CSV.

    Args:
        hook (PostgresHook): The PostgresHook to interact with PostgreSQL.
        table_name (str): The name of the PostgreSQL table to load data into.
        csv_path (str): The path to the CSV file containing the data.
        staging_schema (str): The schema where the staging table is created.
        prod_schema (str): The schema of the production table to reference for creating the staging table.
    
    Returns:
        None
    """

    if os.path.exists(csv_path):
        # Read the CSV file into a DataFrame to capture column names
        df = pd.read_csv(csv_path)
        cols = ", ".join(df.columns)

        logging.info(f"Pushing {table_name} ({df.shape[0]} rows) to staging DB.")

        # get psycopg2 cursor to use copy_expert()
        con = engine.raw_connection()
        cur = con.cursor()
        with open(sql_path, "r") as sql_file:
            with open(csv_path) as csv_file:
                sql = Template(sql_file.read()).render(staging_schema=staging_schema, prod_schema=prod_schema, table_name=table_name, cols=cols)
                cur.copy_expert(sql, csv_file)
                con.commit()
    else:
        logging.info(f"{csv_path} cannot be found.")


def staging_to_prod(engine, table_name: str, sql_path: str, staging_schema: str, prod_schema: str) -> None:
    """
    Moves data from the staging table to the production table in PostgreSQL. Depending on the table, 
    it either updates existing rows based on conflict handling or inserts new rows, ensuring data integrity.

    Args:
        hook (PostgresHook): The PostgresHook to interact with PostgreSQL.
        table_name (str): The name of the PostgreSQL table to insert data into.
        staging_schema (str): The schema where the staging table is located.
        prod_schema (str): The schema of the production table.

    Returns:
        None
    """

    tbl_meta = Table(table_name, MetaData(), autoload_with=engine, schema=prod_schema)
    all_cols = [col.name for col in tbl_meta.columns]
    primary_keys = [col.name for col in tbl_meta.primary_key]
    update_cols = [col for col in all_cols if col not in primary_keys and col not in ("created", "updated")]
    with engine.begin() as con:
        with open(sql_path, "r") as sql_file:
            sql = Template(sql_file.read()).render(
                staging_schema=staging_schema,
                prod_schema=prod_schema,
                table_name=table_name,
                primary_keys=primary_keys,
                update_cols=update_cols,
                )
            con.execute(text(sql))
