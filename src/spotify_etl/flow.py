from prefect import flow, task, get_run_logger
from prefect.blocks.system import Secret
from prefect_sqlalchemy import SqlAlchemyConnector
from spotipy.oauth2 import SpotifyOAuth
import spotipy
import pandas as pd
from jinja2 import Template
import json
from pathlib import Path

from src.spotify_etl.utils import (
    get_recently_played,
    get_track_from_played,
    clean_track_and_played,
    create_track_artist,
    finalize_track,
    get_artists,
    csv_to_staging,
    staging_to_prod,
)

# ----------------------------------------------------------------------
# Constants
# ----------------------------------------------------------------------
DATA_PATH = Path("src/spotify_etl/data")
CACHE_PATH = Path("src/spotify_etl/.cache")
SQL_PATH = Path("src/spotify_etl/sql")

PROD_SCHEMA = "prod"
STAGING_SCHEMA = "staging"

# ----------------------------------------------------------------------
# Secrets & Auth setup
# ----------------------------------------------------------------------
spotipy_block = Secret.load("spotipy")
spotify_access_block = Secret.load("spotify-access-token")

CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
with open(CACHE_PATH, "w") as f:
    json.dump(spotify_access_block.get(), f)


def get_spotify_client():
    """Return an authenticated Spotipy client."""
    creds = spotipy_block.get()
    return spotipy.Spotify(
        auth_manager=SpotifyOAuth(
            client_id=creds["SPOTIPY_CLIENT_ID"],
            client_secret=creds["SPOTIPY_CLIENT_SECRET"],
            redirect_uri=creds["SPOTIPY_REDIRECT_URI"],
            scope="user-read-recently-played",
            cache_path=str(CACHE_PATH),
        )
    )


# ----------------------------------------------------------------------
# Tasks
# ----------------------------------------------------------------------

@task
def create_db_tables():
    """Create necessary database tables."""
    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        sql_file = SQL_PATH / "create_tables.sql"
        sql = Template(sql_file.read_text()).render(
            prod_schema=PROD_SCHEMA, staging_schema=STAGING_SCHEMA
        )
        db.execute(sql)


@task
def extract_played():
    """Extract recently played songs from Spotify."""
    logger = get_run_logger()

    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        result = db.fetch_one(f"SELECT MAX(unix_timestamp) FROM {PROD_SCHEMA}.played")
        last_timestamp = result[0] if result else None

    DATA_PATH.mkdir(parents=True, exist_ok=True)
    sp = get_spotify_client()
    played_raw = get_recently_played(sp, last_timestamp)

    if not played_raw.empty:
        played_raw.to_csv(DATA_PATH / "played_raw.csv", index=False)
        logger.info(f"Saved {len(played_raw)} records to played_raw.csv")
    else:
        logger.info("No new recently played data found.")


@task
def extract_track():
    """Extract track data from played songs."""
    logger = get_run_logger()
    file_path = DATA_PATH / "played_raw.csv"

    if not file_path.exists():
        logger.info("No played_raw.csv found. Skipping track extraction.")
        return

    played_raw = pd.read_csv(file_path)
    track = get_track_from_played(played_raw)
    track.to_csv(DATA_PATH / "track.csv", index=False)
    logger.info("Extracted track data.")


@task
def transform_track():
    """Clean, join, and prepare track and played data."""
    logger = get_run_logger()
    track_file = DATA_PATH / "track.csv"
    played_file = DATA_PATH / "played_raw.csv"

    if not track_file.exists() or not played_file.exists():
        logger.info("Missing CSVs for transformation. Skipping.")
        return

    track = pd.read_csv(track_file, converters={"artist_ids": pd.eval})
    played = pd.read_csv(played_file)

    track, played = clean_track_and_played(track, played)
    track_artist = create_track_artist(track)
    track = finalize_track(track)

    track_artist.to_csv(DATA_PATH / "track_artist.csv", index=False)
    track.to_csv(DATA_PATH / "track.csv", index=False)
    played.to_csv(DATA_PATH / "played.csv", index=False)
    logger.info("Track and played data transformed.")


@task
def extract_artist():
    """Extract artist metadata from Spotify."""
    logger = get_run_logger()
    file_path = DATA_PATH / "track_artist.csv"

    if not file_path.exists():
        logger.info("No track_artist.csv found. Skipping artist extraction.")
        return

    track_artist = pd.read_csv(file_path)
    artist_ids = list(track_artist["artist_id"].unique())

    if not artist_ids:
        logger.info("No artist IDs found.")
        return

    sp = get_spotify_client()
    artist = get_artists(sp, artist_ids, chunksize=50)

    if not artist.empty:
        artist.to_csv(DATA_PATH / "artist.csv", index=False)
        logger.info("Saved artist.csv.")


@task
def load_tables():
    """Load extracted CSVs into staging schema."""
    logger = get_run_logger()
    tables = ["track", "played", "artist", "track_artist"]

    sql_path = SQL_PATH / "load_tables.sql"
    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        engine = db.get_engine()
        for tbl in tables:
            csv_path = DATA_PATH / f"{tbl}.csv"
            if csv_path.exists():
                csv_to_staging(
                    engine, tbl, str(csv_path), str(sql_path),
                    staging_schema=STAGING_SCHEMA, prod_schema=PROD_SCHEMA
                )
                logger.info(f"Loaded {tbl} into staging.")
            else:
                logger.info(f"Missing {tbl}.csv — skipping.")


@task
def insert_prod():
    """Insert staged tables into production schema."""
    logger = get_run_logger()
    tables = ["track", "played", "artist", "track_artist"]
    sql_path = SQL_PATH / "staging_to_prod.sql"

    with SqlAlchemyConnector.load("spotify-postgresql") as db:
        engine = db.get_engine()
        for tbl in tables:
            staging_to_prod(
                engine, tbl, str(sql_path),
                staging_schema=STAGING_SCHEMA, prod_schema=PROD_SCHEMA
            )
            logger.info(f"Inserted {tbl} from staging to prod.")


@task
def cleanup():
    """Remove temporary CSV files."""
    logger = get_run_logger()
    for f in DATA_PATH.glob("*.csv"):
        f.unlink()
        logger.info(f"Deleted {f.name}")


# ----------------------------------------------------------------------
# Flow
# ----------------------------------------------------------------------

@flow(name="Spotify ETL")
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
