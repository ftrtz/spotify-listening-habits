from src.spotify_etl.prefect_blocks.create_spotipy_block import create_spotipy_block
from src.spotify_etl.prefect_blocks.create_token_block import create_token_block
from src.spotify_etl.prefect_blocks.create_pg_block import create_pg_block


if __name__ == "__main__":
    create_spotipy_block()
    create_token_block()
    create_pg_block()