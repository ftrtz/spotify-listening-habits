from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotify_utils import get_recently_played, transform_played, get_artists, get_audio_features, csv_to_staging, staging_to_prod
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
    This pipeline extracts recently played songs and artist information from the Spotify API,
    transforms the data, and loads it into a PostgreSQL database.
    """

    # Task to create the 'played' table if it doesn't exist
    create_db_tables = SQLExecuteQueryOperator(
        task_id='create_db_tables',
        conn_id='spotify_postgres',
        sql='create_tables.sql'
    )
    
    @task()
    def extract_played():
        """
        Extracts the most recently played songs from the Spotify API.
        It fetches the timestamp of the last played song from the 'played' table
        and uses it to extract new records from the Spotify API.
        """
        # Initialize PostgresHook to interact with the PostgreSQL database
        postgres_hook = PostgresHook(postgres_conn_id='spotify_postgres')
        prod_schema = Variable.get("PROD_SCHEMA")
        
        # Execute the query to get the most recent 'played_at' timestamp
        unix_timestamp = postgres_hook.get_first(f'SELECT MAX(unix_timestamp) FROM {prod_schema}.played')[0]
        
        # Convert the timestamp if available, else set to None
        if unix_timestamp:
            played_at = postgres_hook.get_first(f'SELECT MAX(played_at) FROM {prod_schema}.played')[0]
            logging.info(f"Last played track was at {played_at} - Unix Timestamp: {unix_timestamp}")
        else:
            unix_timestamp = None
            logging.info("No last played timestamp found. Using default settings.")

        # Prepare the Spotify API client with the required scope
        scope = "user-read-recently-played"
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=Variable.get("SPOTIPY_CLIENT_ID"),
            client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
            scope=scope,
            cache_path="dags/.cache"
        ))

        # Extract recently played songs from the Spotify API after the last played timestamp
        played = get_recently_played(sp, unix_timestamp)
        
        if played.shape[0] > 0:
            logging.info(f"Retrieved {played.shape[0]} recently played tracks from Spotify.")
            played, track, track_artist = transform_played(played)

            # create data dir
            if not os.path.exists("dags/data"):
                os.makedirs("dags/data")

            # Save the DataFrames to CSV files
            track_artist.to_csv("dags/data/track_artist.csv", index=False)
            track.to_csv("dags/data/track.csv", index=False)
            played.to_csv("dags/data/played.csv", index=False)

        else:
            logging.info(f"Retrieved no new played data from Spotify.")


    @task()
    def transform_recently_played():
        # placeholder for a function to separate extract and transform of played data
        pass

    @task()
    def extract_artist():
        """
        Extracts artist information for all recently played songs from the Spotify API.
        It reads the artist IDs from the 'track_artist' CSV file and fetches details for those artists.
        """
        # Path to the played songs CSV file
        csv_path = "dags/data/track_artist.csv"
        
        if os.path.exists(csv_path):
            # Get a list of unique artist IDs
            track_artist = pd.read_csv(csv_path)
            artist_ids = list(track_artist["artist_id"].unique())

            # TODO: Check if the artists are already in the database to avoid unnecessary API requests

            if artist_ids:
                # Prepare the Spotify API client
                sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                    client_id=Variable.get("SPOTIPY_CLIENT_ID"),
                    client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
                    redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
                    cache_path="dags/.cache"
                ))
                # Extract artist information from the Spotify API
                artist = get_artists(sp, artist_ids, 50)

                if artist.shape[0] > 0:
                    # Save the DataFrame to a CSV file
                    artist.to_csv("dags/data/artist.csv", index=False)
                    logging.info(f"Retrieved {artist.shape[0]} artists from Spotify.")
                else:
                    logging.info(f"Retrieved no new artist data from Spotify.")
            else:
                logging.info("No artists to extract.")
        else:
            logging.info("No 'played' CSV file found. No artists to extract.")

    @task()
    def extract_audio_features():
        """
        Extracts audio features information for all recently played songs from the Spotify API.
        It reads the track IDs from the 'track_artist' CSV file and fetches details for those tracks.
        """
        # Path to the track_artist songs CSV file
        csv_path = "dags/data/track_artist.csv"
        
        if os.path.exists(csv_path):
            # Get a list of unique artist IDs
            track_artist = pd.read_csv(csv_path)
            track_ids = list(track_artist["track_id"].unique())
            # TODO: Check if the tracks are already in the database to avoid unnecessary API requests

            if track_ids:
                # Prepare the Spotify API client
                sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                    client_id=Variable.get("SPOTIPY_CLIENT_ID"),
                    client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
                    redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
                    cache_path="dags/.cache"
                ))
                # Extract artist information from the Spotify API
                audio_features = get_audio_features(sp, track_ids, 50)

                if audio_features.shape[0] > 0:
                    # Save the DataFrame to a CSV file
                    audio_features.to_csv("dags/data/audio_features.csv", index=False)
                
                    logging.info(f"Retrieved {audio_features.shape[0]} tracks audio features from Spotify.")
                else:
                    logging.info(f"Retrieved no audio features data from Spotify.")
            else:
                logging.info("No audio features to extract.")
        else:
            logging.info("No 'track_artist' CSV file found. No audio features to extract.")
    
    @task(retries=0)
    def load_tables():
        """
        Loads all csv tables data from the CSV files into the corresponding PostgreSQL tables.
        """
        # Connect to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        tbl_names = ["track", "audio_features", "played", "artist", "track_artist"]
        for tbl_name in tbl_names:
            csv_to_staging(pg_hook, tbl_name, f"dags/data/{tbl_name}.csv", staging_schema="staging", prod_schema=Variable.get("PROD_SCHEMA"))
            logging.info(f"Pushed {tbl_name} data to staging database")

    @task(retries=0)
    def insert_prod():
        """
        Insert data from staging tables to prod.
        """
        # Connect to the PostgreSQL database
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        tbl_names = ["track", "audio_features", "played", "artist", "track_artist"]
        for tbl_name in tbl_names:
            staging_to_prod(pg_hook, tbl_name, staging_schema="staging", prod_schema = Variable.get("PROD_SCHEMA"))


    @task()
    def cleanup():
        """
        Cleans up the temporary CSV files after loading the data into the PostgreSQL database.
        """
        filenames = glob.glob("dags/data/*.csv")

        for f in filenames:
            os.remove(f)
            logging.info(f"Removed {f}")
        
    # Define task dependencies to set the order of execution
    create_db_tables >> extract_played() >> extract_audio_features() >> extract_artist() >> load_tables() >> insert_prod() >> cleanup()

# Instantiate the DAG
dag_run = etl()
