from airflow.decorators import dag, task
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.hooks.postgres_hook import PostgresHook
from datetime import datetime, timedelta
from spotify_api import extract_recently_played, convert_time
import os
import logging

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
    create_db_table = SQLExecuteQueryOperator(
        task_id='create_db_table',
        conn_id='spotify_postgres',
        sql='played_schema.sql',
        params={"tbl": "played"}
    )
    
    @task()
    def extract():
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
    def load():
        """
        Loads the extracted Spotify data into the 'played' table in PostgreSQL.
        """
        csv_path = "dags/data/spotify.csv"

        if os.path.exists(csv_path):
            postgres_hook = PostgresHook(postgres_conn_id="spotify_postgres")

            # Load data into the 'played' table using the COPY command
            with postgres_hook.get_conn() as connection:
                postgres_hook.copy_expert(
                    """
                    COPY played (track_spotify_id, track_name, artists_spotify_id, artists_name, album_spotify_id, album_name, played_at)
                    FROM stdin WITH CSV HEADER DELIMITER as ','
                    """,
                    f"{csv_path}",
                )
                connection.commit()
        else:
            logging.info("No data to push - CSV file can't be found.")

    @task()
    def cleanup():
        """
        Cleans up the temporary CSV file after loading the data into the database.
        """
        csv_path = "dags/data/spotify.csv"
        if os.path.exists(csv_path):
            os.remove(csv_path)
            logging.info("CSV file removed.")
        else:
            logging.info("CSV file does not exist. No need to remove.")
        
    # Set task dependencies
    create_db_table >> extract() >> load() >> cleanup()

# Run the DAG
dag_run = etl()
