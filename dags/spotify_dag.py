from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from spotify_api import extract_recently_played, convert_time, extract_artists, csv_to_postgresql
import os
import logging
import pandas as pd
import ast

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
        
        # Execute the query to get the most recent 'played_at' timestamp
        result = postgres_hook.get_first('SELECT MAX(played_at) FROM played')[0]
        
        # Convert the timestamp if available, else set to None
        if result:
            last_played_at = convert_time(result)
        else:
            last_played_at = None
            logging.info("No last played timestamp found. Using default settings.")

        # Extract recently played songs from the Spotify API after the last played timestamp
        extract_recently_played(last_played_at)

    @task()
    def extract_artist():
        """
        Extracts artist information for all recently played songs from the Spotify API.
        It reads the artist IDs from the 'played' CSV file and fetches details for those artists.
        """
        # Path to the played songs CSV file
        csv_path = "dags/data/played.csv"
        
        if os.path.exists(csv_path):
            # Read the CSV file into a DataFrame
            played_df = pd.read_csv(csv_path, converters={'artists_spotify_id': ast.literal_eval})
            # Get a list of unique artist IDs
            artist_ids = played_df["artists_spotify_id"].explode().to_list()
            artist_ids = list(set(artist_ids))

            # TODO: Check if the artists are already in the database to avoid unnecessary API requests

            if artist_ids:
                # Extract artist information from the Spotify API
                extract_artists(artist_ids)
            else:
                logging.info("No artists to extract.")
        else:
            logging.info("No 'played' CSV file found. No artists to extract.")
    
    @task()
    def load_played():
        """
        Loads the recently played songs data from the CSV file into the PostgreSQL 'played' table.
        """
        csv_to_postgresql("played", "dags/data/played.csv")

    @task()
    def load_artist():
        """
        Loads the artist information data from the CSV file into the PostgreSQL 'artist' table.
        """
        csv_to_postgresql("artist", "dags/data/artist.csv")

    @task()
    def cleanup():
        """
        Cleans up the temporary CSV files after loading the data into the PostgreSQL database.
        """
        csv_paths = ["dags/data/played.csv", "dags/data/artist.csv"]

        for csv_path in csv_paths:
            if os.path.exists(csv_path):
                os.remove(csv_path)
                logging.info(f"Removed {csv_path}")
            else:
                logging.info(f"{csv_path} does not exist. No need to remove.")
        
    # Define task dependencies to set the order of execution
    create_db_tables >> extract_played() >> load_played() >> extract_artist() >> load_artist() >> cleanup()

# Instantiate the DAG
dag_run = etl()
