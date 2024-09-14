from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotify_api import get_played_from_history, get_tracks, transform_track, get_artists, get_audio_features, csv_to_postgresql
import os
import logging
import pandas as pd

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'retries': 1,
    'retry_delay': timedelta(seconds=5)
}

@dag(
    dag_id="history_upload",
    default_args=default_args,
    template_searchpath='dags/sql',
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False
)


def history_etl():

    @task()
    def extract_history():

        history_folder = "dags/SpotifyExtendedStreamingHistory"
        played_history_df = get_played_from_history(history_folder)
        played_history_df.to_csv("dags/data/played_history.csv", index=False)
        # todo: move the other file saving operations to the dag, too


    @task()
    def extract_track():

        # Prepare the Spotify API client with the required scope
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=Variable.get("SPOTIPY_CLIENT_ID"),
            client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
            cache_path="dags/.cache"
        ))

        track_ids = pd.read_csv("dags/data/played_history.csv")["track_id"]

        track = get_tracks(sp, track_ids)

        transform_track(track)


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
                get_artists(sp, artist_ids)
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
                get_audio_features(sp, track_ids)
            else:
                logging.info("No audio features to extract.")
        else:
            logging.info("No 'track_artist' CSV file found. No audio features to extract.")
 

    @task()
    def cleanup():
        """
        Cleans up the temporary CSV files after loading the data into the PostgreSQL database.
        """
        filenames = os.listdir("dags/data/")

        for f in filenames:
            f_path = os.path.join("dags/data/", f)
            os.remove(f_path)
            logging.info(f"Removed {f_path}")

    # Define task dependencies to set the order of execution
    extract_history() >> extract_track()  >> extract_artist()  >> extract_audio_features() # >> cleanup()

# Instantiate the DAG
dag_run = history_etl()