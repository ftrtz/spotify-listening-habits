from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from datetime import datetime, timedelta
import spotipy
from spotipy.oauth2 import SpotifyOAuth
from spotify_utils import get_played_from_history, get_tracks, clean_track_and_played, create_track_artist, finalize_track, get_artists, get_audio_features, csv_to_staging, staging_to_prod
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
    dag_id="history_upload",
    default_args=default_args,
    template_searchpath='dags/sql',
    schedule_interval="@once",
    start_date=datetime(2024, 1, 1),
    catchup=False
)
def history_etl():
    """
    Airflow DAG to extract, transform, and load Spotify listening history data.
    The DAG:
    1. Backs up the existing production database.
    2. Creates necessary database tables (if not already created).
    3. Extracts and transforms Spotify data such as tracks, artists, and audio features.
    4. Loads transformed data into a PostgreSQL staging schema.
    5. Inserts the staging data into the production schema, handling updates and conflicts.
    6. Cleans up temporary files and old database backups.
    """

    @task.bash
    def backup_db() -> str:
        """
        Creates a backup of the Spotify production database schema.

        Returns:
            str: The bash command to execute the pg_dump to backup the database.
        """
        # create backup data directory if it doesn't exist
        backup_dir = "dags/data/backup/"
        if not os.path.exists(backup_dir):
            os.makedirs(backup_dir)

        # get connection info from hook
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        conn = pg_hook.get_connection(conn_id="spotify_postgres")

        # Get connection details dynamically from the connection object
        host = conn.host
        port = conn.port
        user = conn.login
        password = conn.password
        dbname = "spotify"
        prod_schema = Variable.get("PROD_SCHEMA")

        dump_file_path = "${AIRFLOW_HOME}/" + backup_dir + datetime.today().strftime('%Y%m%d') + "_spotify_prod_dump.sql"  # Path where the dump will be saved

        # Return the bash command to execute the pg_dump command
        return f"PGPASSWORD={password} pg_dump -h {host} -p {port} -U {user} {dbname} -n {prod_schema} > {dump_file_path}"


    create_db_tables = SQLExecuteQueryOperator(
        task_id='create_db_tables',
        conn_id='spotify_postgres',
        sql='create_tables.sql',
        parameters={"prod_schema": Variable.get("PROD_SCHEMA"), "staging_schema": "history"}
    )

    @task()
    def extract_history() -> None:
        """
        Extracts Spotify listening history data from local history files and saves it to a CSV file.
        """
        # Create history data directory if it doesn't exist
        if not os.path.exists("dags/data/history"):
            os.makedirs("dags/data/history")

        history_folder = "dags/SpotifyExtendedStreamingHistory"
        played = get_played_from_history(history_folder)
        played.to_csv("dags/data/history/played.csv", index=False)

    @task()
    def extract_track() -> None:
        """
        Extracts track data from Spotify API based on track IDs in the played history, 
        and saves it to a CSV file.
        """
        # Prepare the Spotify API client with the required scope
        sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
            client_id=Variable.get("SPOTIPY_CLIENT_ID"),
            client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
            redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
            cache_path="dags/.cache"
        ))

        track_ids = list(pd.read_csv("dags/data/history/played.csv")["track_id"].unique())

        # Call Spotify API to fetch track details
        track = get_tracks(sp, track_ids, 100)

        if track.shape[0] > 0:
            logging.info(f"Retrieved {track.shape[0]} history tracks from Spotify.")
            
            # Save the track data to a CSV file
            track.to_csv("dags/data/history/track.csv", index=False)
        else:
            logging.info(f"Retrieved no track data from Spotify.")

    @task()
    def transform_track_history() -> None:
        """
        Cleans and transforms track and played data, then saves the transformed data
        to CSV files for further processing.
        """
        data_path = "dags/data/history/"
        track = pd.read_csv(data_path + "track.csv", converters={'artist_ids': pd.eval}) # Ensure 'artist_ids' is properly parsed as a list
        played = pd.read_csv(data_path + "played.csv")

        # Clean and transform the data
        track, played = clean_track_and_played(track, played)
        track_artist = create_track_artist(track)
        track = finalize_track(track)

        # Save the transformed data to CSV
        track_artist.to_csv(data_path + "track_artist.csv", index=False)
        track.to_csv(data_path + "track.csv", index=False)
        played.to_csv(data_path + "played.csv", index=False)

    @task()
    def extract_artist() -> None:
        """
        Extracts artist data from the Spotify API based on artist IDs in the 'track_artist' CSV file,
        and saves it to a CSV file.
        """
        csv_path = "dags/data/history/track_artist.csv"
        
        if os.path.exists(csv_path):
            track_artist = pd.read_csv(csv_path)
            artist_ids = list(track_artist["artist_id"].unique())

            if artist_ids:
                # Prepare the Spotify API client
                sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                    client_id=Variable.get("SPOTIPY_CLIENT_ID"),
                    client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
                    redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
                    cache_path="dags/.cache"
                ))

                # Fetch artist details from Spotify API
                artist = get_artists(sp, artist_ids, 50)

                if artist.shape[0] > 0:
                    artist.to_csv("dags/data/history/artist.csv", index=False)
                    logging.info(f"Retrieved {artist.shape[0]} artists from Spotify.")
                else:
                    logging.info(f"Retrieved no new artist data from Spotify.")
            else:
                logging.info("No artists to extract.")
        else:
            logging.info("No 'track_artist' CSV file found. No artists to extract.")

    @task()
    def extract_audio_features() -> None:
        """
        Extracts audio features for tracks in the 'track_artist' CSV file from the Spotify API,
        and saves them to a CSV file.
        """
        csv_path = "dags/data/history/track_artist.csv"
        
        if os.path.exists(csv_path):
            track_artist = pd.read_csv(csv_path)
            track_ids = list(track_artist["track_id"].unique())

            if track_ids:
                # Prepare the Spotify API client
                sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                    client_id=Variable.get("SPOTIPY_CLIENT_ID"),
                    client_secret=Variable.get("SPOTIPY_CLIENT_SECRET"),
                    redirect_uri=Variable.get("SPOTIPY_REDIRECT_URI"),
                    cache_path="dags/.cache"
                ))

                # Fetch audio features from Spotify API
                audio_features = get_audio_features(sp, track_ids, 100)

                if audio_features.shape[0] > 0:
                    audio_features.to_csv("dags/data/history/audio_features.csv", index=False)
                    logging.info(f"Retrieved {audio_features.shape[0]} audio features from Spotify.")
                else:
                    logging.info(f"Retrieved no audio features data from Spotify.")
            else:
                logging.info("No audio features to extract.")
        else:
            logging.info("No 'track_artist' CSV file found. No audio features to extract.")

    @task(retries=0)
    def load_tables() -> None:
        """
        Loads the extracted and transformed CSV data into the PostgreSQL staging schema.
        """
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        tbl_names = ["track", "audio_features", "played", "artist", "track_artist"]

        # Load each table's CSV into the staging schema
        for tbl_name in tbl_names:
            csv_to_staging(pg_hook, tbl_name, f"dags/data/history/{tbl_name}.csv", staging_schema="history", prod_schema=Variable.get("PROD_SCHEMA"))
            logging.info(f"Pushed {tbl_name} data to staging database")

    @task(retries=0)
    def insert_prod() -> None:
        """
        Inserts data from the staging schema to the production schema, handling conflicts.
        """
        pg_hook = PostgresHook(postgres_conn_id="spotify_postgres")
        tbl_names = ["track", "audio_features", "played", "artist", "track_artist"]

        # Insert data from staging into production, handling updates where necessary
        for tbl_name in tbl_names:
            staging_to_prod(pg_hook, tbl_name, staging_schema="history", prod_schema=Variable.get("PROD_SCHEMA"))

    @task()
    def cleanup_dir() -> None:
        """
        Cleans up the temporary CSV files used during the ETL process.
        """
        filenames = glob.glob("dags/data/history/*.csv")

        for f in filenames:
            os.remove(f)
            logging.info(f"Removed {f}")

    @task()
    def cleanup_backups() -> None:
        """
        Cleans up old database backup files, preserving only the most recent 3 backups.
        """
        backup_dir = "dags/data/backup/"
        filenames = sorted(os.listdir(backup_dir))

        # Remove all but the latest 3 backups
        while len(filenames) > 3:
            oldest_file = filenames.pop(0)
            os.remove(os.path.join(backup_dir, oldest_file))
            logging.info(f"Removed {oldest_file}")

    # Define task dependencies to set the order of execution
    backup_db() >> create_db_tables >> extract_history() >> extract_track() >> transform_track_history() >> extract_artist()  >> extract_audio_features() >> load_tables() >> insert_prod() >> cleanup_dir() >> cleanup_backups()

# Instantiate the DAG
dag_run = history_etl()
