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
    Extracts recently played songs, transforms the data,
    and loads it into a PostgreSQL database.
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
        postgres_hook = PostgresHook(postgres_conn_id='spotify_postgres')
        # Execute the query to get the most recent 'played_at' timestamp
        result = postgres_hook.get_first('SELECT MAX(played_at) FROM played')[0]
        if result:
            last_played_at = convert_time(result)
        else:
            last_played_at = None
            logging.info("No last played timestamp. Use default settings.")

        # Extract recently played songs after the last played timestamp
        extract_recently_played(last_played_at)

    @task()
    def extract_artist():
        """
        Extracts the artists for all recently played songs from the Spotify API.
        """
        # get artist_ids from the played csv
        played_df = pd.read_csv("dags/data/played.csv", converters={'artists_spotify_id': ast.literal_eval})
        artist_ids = played_df["artists_spotify_id"].explode().to_list()
        artist_ids = list(set(artist_ids))

        # check if they are already in the db to avoid unnecessary requests to the spotify api
        #postgres_hook = PostgresHook(postgres_conn_id='spotify_postgres')
        #sql = f""
        #conn = postgres_hook.get_conn()
        #cursor = conn.cursor()
        #cursor.execute(sql)

        if len(artist_ids) > 0:
            # extract the artist information
            extract_artists(artist_ids)
        else:
            logging.info("No artists to extract")
    
    @task()
    def load_played():
        csv_to_postgresql("played", "dags/data/played.csv")

    @task()
    def load_artist():
        csv_to_postgresql("artist", "dags/data/artist.csv")

    @task()
    def cleanup():
        """
        Cleans up the temporary CSV file after loading the data into the database.
        """
        csv_paths = ["dags/data/played.csv", "dags/data/artist.csv"]

        for csv_path in csv_paths:
            if os.path.exists(csv_path):
                os.remove(csv_path)
                logging.info(f"Removed {csv_path}")
            else:
                logging.info(f"{csv_path} does not exist. No need to remove.")
        
    # Set task dependencies
    create_db_tables >> extract_played() >> load_played() >> extract_artist() >> load_artist() >> cleanup()

# Run the DAG
dag_run = etl()
