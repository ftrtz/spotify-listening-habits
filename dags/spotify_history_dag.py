from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotify_api import get_played_from_history, get_tracks, transform_track, get_artists, get_audio_features, csv_to_staging, get_db_ids
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


    @task()
    def extract_track():

        # Prepare the Spotify API client with the required scope
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=Variable.get("SPOTIPY_CLIENT_ID"),
            client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
            cache_path="dags/.cache"
        ))

        track_ids_new = list(pd.read_csv("dags/data/played_history.csv")["track_id"].unique())

        # Get track_ids from db to filter out already present ids
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        track_ids_db = get_db_ids(pg_hook, "track")

        track_ids = list(set(track_ids_new) - set(track_ids_db))

        # call spotify api
        track = get_tracks(sp, track_ids, 100)

        if track.shape[0] > 0:
            logging.info(f"Retrieved {track.shape[0]} tracks from Spotify.")
            track, track_artist = transform_track(track)
            
            # Save the DataFrames to CSV files
            track_artist.to_csv("dags/data/track_artist_history.csv", index=False)
            track.to_csv("dags/data/track_history.csv", index=False)
        else:
            logging.info(f"Retrieved no new track data from Spotify.")

    @task()
    def transform_track_history():
        # placeholder for a function to separate exrtact and transform of track data
        pass


    @task()
    def extract_artist():
        """
        Extracts artist information for all recently played songs from the Spotify API.
        It reads the artist IDs from the 'track_artist' CSV file and fetches details for those artists.
        """
        # Path to the played songs CSV file
        csv_path = "dags/data/track_artist_history.csv"
        
        if os.path.exists(csv_path):
            # Get a list of unique artist IDs
            track_artist = pd.read_csv(csv_path)
            artist_ids_new = list(track_artist["artist_id"].unique())

            # Get artist ids from db to filter out already present ids
            pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
            artist_ids_db = get_db_ids(pg_hook, "artist")

            artist_ids = list(set(artist_ids_new) - set(artist_ids_db))


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
                    artist.to_csv("dags/data/artist_history.csv", index=False)
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
        csv_path = "dags/data/track_artist_history.csv"
        
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
                audio_features = get_audio_features(sp, track_ids, 100)

                if audio_features.shape[0] > 0:
                    # Save the DataFrame to a CSV file
                    audio_features.to_csv("dags/data/audio_features_history.csv", index=False)
                
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
        tbl_names = ["track_history", "audio_features_history", "played_history", "artist_history", "track_artist_history"]
        for tbl_name in tbl_names:
            csv_to_staging(pg_hook, tbl_name, f"dags/data/{tbl_name}.csv")
            logging.info(f"Pushed {tbl_name} data to staging database")

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
    extract_history() >> extract_track()  >> extract_artist()  >> extract_audio_features() >> load_tables() # >> cleanup()

# Instantiate the DAG
dag_run = history_etl()