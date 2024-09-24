from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotify_utils import (
    get_recently_played,
    get_track_from_played,
    clean_track_and_played,
    create_track_artist,
    finalize_track,
    get_artists,
    get_audio_features,
    csv_to_staging,
    staging_to_prod
)
import os
import glob
import logging
import pandas as pd

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

@dag(
    dag_id="spotify_etl",
    default_args=default_args,
    template_searchpath='dags/sql',
    schedule_interval="@hourly",
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def etl():
    """
    ETL pipeline for Spotify data using Airflow.
    This pipeline extracts recently played tracks and artist information from the Spotify API,
    transforms the data, and loads it into a PostgreSQL database.
    The pipeline consists of the following steps:
    
    1. Creates necessary database tables if they don't exist.
    2. Extracts recently played tracks from Spotify.
    3. Extracts artist and audio features information for these tracks.
    4. Transforms the data and stores it in CSV files.
    5. Loads the data from CSV files into PostgreSQL staging tables.
    6. Inserts data from staging into the production database.
    7. Cleans up temporary CSV files after loading.
    """

    # Task to create the required database tables if they don't exist
    create_db_tables = SQLExecuteQueryOperator(
        task_id='create_db_tables',
        conn_id='spotify_postgres',
        sql='create_tables.sql'
    )
    
    @task()
    def extract_played():
        """
        Extracts recently played tracks from the Spotify API.
        
        It fetches the last played track's timestamp from the 'played' table in the PostgreSQL database
        and uses it to extract new records from the Spotify API.
        The recently played data is then transformed into three DataFrames:
        - played: The history of played tracks.
        - track: Track information.
        - track_artist: Relationship between tracks and their artists.

        Saves these DataFrames as CSV files for further processing.
        """
        postgres_hook = PostgresHook(postgres_conn_id='spotify_postgres')
        prod_schema = Variable.get("PROD_SCHEMA")
        
        # Get the most recent played track's timestamp
        unix_timestamp = postgres_hook.get_first(f'SELECT MAX(unix_timestamp) FROM {prod_schema}.played')[0]
        
        if unix_timestamp:
            played_at = postgres_hook.get_first(f'SELECT MAX(played_at) FROM {prod_schema}.played')[0]
            logging.info(f"Last played track was at {played_at} (Unix Timestamp: {unix_timestamp})")
        else:
            unix_timestamp = None
            logging.info("No previous played data found, extracting all available records.")

        # Initialize Spotify API client
        scope = "user-read-recently-played"
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=Variable.get("SPOTIPY_CLIENT_ID"),
            client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
            scope=scope,
            cache_path="dags/.cache"
        ))

        # Extract recently played tracks
        played_raw = get_recently_played(sp, unix_timestamp)
        
        if played_raw.shape[0] > 0:
            logging.info(f"Retrieved {played_raw.shape[0]} recently played tracks from Spotify.")

            # Create data directory if it doesn't exist
            if not os.path.exists("dags/data"):
                os.makedirs("dags/data")

            # Save DataFrames to CSV
            played_raw.to_csv("dags/data/played_raw.csv", index=False)
        else:
            logging.info("No new recently played data found.")

    @task()
    def extract_track():

        data_path = "dags/data/"
        played_raw = pd.read_csv(data_path + "played_raw.csv")

        track = get_track_from_played(played_raw)

        track.to_csv(data_path + "track.csv", index=False)

    @task()
    def transform_track() -> None:
        """
        Cleans and transforms track and played data, then saves the transformed data
        to CSV files for further processing.
        """
        data_path = "dags/data/"
        track = pd.read_csv(data_path + "track.csv", converters={'artist_ids': pd.eval}) # Ensure 'artist_ids' is properly parsed as a list
        played = pd.read_csv(data_path + "played_raw.csv")

        # Clean and transform the data
        track, played = clean_track_and_played(track, played)
        track_artist = create_track_artist(track)
        track = finalize_track(track) # check ob man das braucht

        # Save the transformed data to CSV
        track_artist.to_csv(data_path + "track_artist.csv", index=False)
        track.to_csv(data_path + "track.csv", index=False)
        played.to_csv(data_path + "played.csv", index=False)


    @task()
    def extract_artist():
        """
        Extracts artist information for the recently played tracks from the Spotify API.
        
        Reads artist IDs from the 'track_artist' CSV file and fetches artist details from the API.
        Saves the artist information to a CSV file for further processing.
        """
        csv_path = "dags/data/track_artist.csv"
        
        if os.path.exists(csv_path):
            track_artist = pd.read_csv(csv_path)
            artist_ids = list(track_artist["artist_id"].unique())

            if artist_ids:
                # Initialize Spotify API client
                sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                    client_id=Variable.get("SPOTIPY_CLIENT_ID"),
                    client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
                    redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
                    cache_path="dags/.cache"
                ))
                # Extract artist details from the API
                artist = get_artists(sp, artist_ids, 50)

                if artist.shape[0] > 0:
                    artist.to_csv("dags/data/artist.csv", index=False)
                    logging.info(f"Retrieved {artist.shape[0]} artists from Spotify.")
                else:
                    logging.info("No artist data found.")
            else:
                logging.info("No artist IDs to extract.")
        else:
            logging.info("No 'track_artist' CSV file found, cannot extract artist data.")

    @task()
    def extract_audio_features():
        """
        Extracts audio features for the recently played tracks from the Spotify API.
        
        Reads track IDs from the 'track_artist' CSV file and fetches audio features for those tracks.
        Saves the audio features to a CSV file for further processing.
        """
        csv_path = "dags/data/track_artist.csv"
        
        if os.path.exists(csv_path):
            track_artist = pd.read_csv(csv_path)
            track_ids = list(track_artist["track_id"].unique())

            if track_ids:
                # Initialize Spotify API client
                sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                    client_id=Variable.get("SPOTIPY_CLIENT_ID"),
                    client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
                    redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
                    cache_path="dags/.cache"
                ))
                # Extract audio features from the API
                audio_features = get_audio_features(sp, track_ids, 50)

                if audio_features.shape[0] > 0:
                    audio_features.to_csv("dags/data/audio_features.csv", index=False)
                    logging.info(f"Retrieved {audio_features.shape[0]} audio features from Spotify.")
                else:
                    logging.info("No audio features data found.")
            else:
                logging.info("No track IDs to extract audio features.")
        else:
            logging.info("No 'track_artist' CSV file found, cannot extract audio features.")

    @task(retries=0)
    def load_tables():
        """
        Loads the extracted and transformed data from CSV files into the PostgreSQL database.
        
        The data is first loaded into staging tables and then transferred to the production schema.
        """
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        tbl_names = ["track", "audio_features", "played", "artist", "track_artist"]

        for tbl_name in tbl_names:
            csv_to_staging(pg_hook, tbl_name, f"dags/data/{tbl_name}.csv", staging_schema="staging", prod_schema=Variable.get("PROD_SCHEMA"))
            logging.info(f"Pushed {tbl_name} data to the staging database.")

    @task(retries=0)
    def insert_prod():
        """
        Inserts data from the staging tables into the production tables in PostgreSQL.
        
        Handles conflicts by upserting (inserting or updating) the data into the production schema.
        """
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        tbl_names = ["track", "audio_features", "played", "artist", "track_artist"]

        for tbl_name in tbl_names:
            staging_to_prod(pg_hook, tbl_name, staging_schema="staging", prod_schema=Variable.get("PROD_SCHEMA"))
            logging.info(f"Inserted data from staging into {tbl_name} in the production database.")

    @task()
    def cleanup():
        """
        Cleans up temporary CSV files after the data has been successfully loaded into the database.
        """
        filenames = glob.glob("dags/data/*.csv")

        for f in filenames:
            os.remove(f)
            logging.info(f"Removed temporary file: {f}")

    # Task dependencies to set the order of execution
    create_db_tables >> extract_played() >> extract_track() >> transform_track() >> extract_audio_features() >> extract_artist() >> load_tables() >> insert_prod() >> cleanup()

# Instantiate the DAG
dag_run = etl()
