from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_sqlalchemy import SqlAlchemyConnector
from spotipy.oauth2 import SpotifyOAuth
import spotipy
import pandas as pd
import os
import glob
from jinja2 import Template
import json

from utils.spotify_utils import (
    get_recently_played,
    get_track_from_played,
    clean_track_and_played,
    create_track_artist,
    finalize_track,
    get_artists,
    csv_to_staging,
    staging_to_prod
)

DATA_PATH = "data"

PROD_SCHEMA="prod"
STAGING_SCHEMA="staging"

spotipy_block = Secret.load("spotipy")
spotify_access_block = Secret.load("spotify-access-token")
with open(".cache", "w") as f:
    json.dump(spotify_access_block.get(), f)

@task
def create_db_tables():
    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        with open("sql/create_tables.sql", "r") as file:
            sql = Template(file.read()).render(prod_schema=PROD_SCHEMA, staging_schema=STAGING_SCHEMA)
            db.execute(sql)
@task
def extract_played():
    logger = get_run_logger()

    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        result = db.fetch_one(f"SELECT MAX(unix_timestamp) FROM {PROD_SCHEMA}.played")
        unix_timestamp = result[0] if result else None

    if not os.path.exists(DATA_PATH):
        os.makedirs(DATA_PATH)

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        client_id=spotipy_block.get()["SPOTIPY_CLIENT_ID"],
        client_secret=spotipy_block.get()["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=spotipy_block.get()["SPOTIPY_REDIRECT_URI"],
        scope="user-read-recently-played",
        cache_path=".cache"
    ))

    played_raw = get_recently_played(sp, unix_timestamp)
    if not played_raw.empty:
        played_raw.to_csv(f"{DATA_PATH}/played_raw.csv", index=False)
        logger.info(f"Saved {len(played_raw)} records to played_raw.csv")
    else:
        logger.info("No new recently played data.")

@task
def extract_track():
    logger = get_run_logger()
    file_path = f"{DATA_PATH}/played_raw.csv"
    if os.path.exists(file_path):
        played_raw = pd.read_csv(file_path)
        track = get_track_from_played(played_raw)
        track.to_csv(f"{DATA_PATH}/track.csv", index=False)
        logger.info("Track data extracted.")
    else:
        logger.info("played_raw.csv not found.")

@task
def transform_track():
    logger = get_run_logger()
    track_file = f"{DATA_PATH}/track.csv"
    played_file = f"{DATA_PATH}/played_raw.csv"

    if os.path.exists(track_file) and os.path.exists(played_file):
        track = pd.read_csv(track_file, converters={"artist_ids": pd.eval})
        played = pd.read_csv(played_file)

        track, played = clean_track_and_played(track, played)
        track_artist = create_track_artist(track)
        track = finalize_track(track)

        track_artist.to_csv(f"{DATA_PATH}/track_artist.csv", index=False)
        track.to_csv(f"{DATA_PATH}/track.csv", index=False)
        played.to_csv(f"{DATA_PATH}/played.csv", index=False)
        logger.info("Track and played data transformed.")
    else:
        logger.info("Required CSVs not found for transformation.")

@task
def extract_artist():
    logger = get_run_logger()
    file_path = f"{DATA_PATH}/track_artist.csv"
    if os.path.exists(file_path):
        track_artist = pd.read_csv(file_path)
        artist_ids = list(track_artist["artist_id"].unique())

        if artist_ids:
            sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
                client_id=spotipy_block.get()["SPOTIPY_CLIENT_ID"],
                client_secret=spotipy_block.get()["SPOTIPY_CLIENT_SECRET"],
                redirect_uri=spotipy_block.get()["SPOTIPY_REDIRECT_URI"],
                scope="user-read-recently-played",
                cache_path=".cache"
            ))

            artist = get_artists(sp, artist_ids, 50)
            if not artist.empty:
                artist.to_csv(f"{DATA_PATH}/artist.csv", index=False)
                logger.info("Saved artist.csv.")
        else:
            logger.info("No artist IDs found.")
    else:
        logger.info("track_artist.csv not found.")

@task
def load_tables():
    logger = get_run_logger()
    tbl_names = ["track", "played", "artist", "track_artist"]

    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        engine = db.get_engine()
        sql_path = "sql/load_tables.sql"
        for tbl in tbl_names:
            csv_path = f"{DATA_PATH}/{tbl}.csv"
            if os.path.exists(csv_path):
                csv_to_staging(engine, tbl, csv_path, sql_path, staging_schema=STAGING_SCHEMA, prod_schema=PROD_SCHEMA)
                logger.info(f"Loaded {tbl} into staging.")
            else:
                logger.info(f"{tbl}.csv not found.")

@task
def insert_prod():
    logger = get_run_logger()
    tbl_names = ["track", "played", "artist", "track_artist"]
    sql_path = "sql/staging_to_prod.sql"
    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        engine = db.get_engine()
        for tbl in tbl_names:
            staging_to_prod(engine, tbl, sql_path, staging_schema=STAGING_SCHEMA, prod_schema=PROD_SCHEMA)
            logger.info(f"Inserted {tbl} from staging to prod.")

@task
def cleanup():
    logger = get_run_logger()
    files = glob.glob(f"{DATA_PATH}/*.csv")
    for f in files:
        os.remove(f)
        logger.info(f"Deleted {f}")

@flow()
def spotify_etl():
    create_db_tables()
    extract_played()
    extract_track()
    transform_track()
    extract_artist()
    load_tables()
    insert_prod()
    cleanup()

if __name__ == "__main__":
    spotify_etl()
