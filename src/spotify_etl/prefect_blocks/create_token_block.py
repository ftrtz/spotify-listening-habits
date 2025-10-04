import spotipy
from spotipy.oauth2 import SpotifyOAuth
from prefect.blocks.system import Secret

def create_token_block(token_block_name: str = "spotify-access-token", spotipy_block_name: str = "spotipy"):
    # Check if block exists
    try:
        Secret.load(token_block_name)
        print(f"⚠️  A Prefect Secret block named '{token_block_name}' already exists.")
        overwrite = input("Do you want to overwrite it? (y/N): ").strip().lower()
        if overwrite != "y":
            print("Aborted. No changes made.")
            return
    except Exception:
        # Block does not exist, continue
        pass

    spotipy_block = Secret.load(spotipy_block_name)

    sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
        cache_handler=spotipy.cache_handler.MemoryCacheHandler(),
        client_id=spotipy_block.get()["SPOTIPY_CLIENT_ID"],
        client_secret=spotipy_block.get()["SPOTIPY_CLIENT_SECRET"],
        redirect_uri=spotipy_block.get()["SPOTIPY_REDIRECT_URI"],
        scope="user-read-recently-played",
        ))

    # Make any request to invoke the authentication
    sp.user_playlists('spotify')

    if sp.auth_manager.cache_handler.token_info:
        Secret(value=sp.auth_manager.cache_handler.token_info).save(token_block_name, overwrite=True)
        print(f"✅ Created Prefect Secret block: {token_block_name}")
    else:
        print(f"⚠️  Skipped {token_block_name} block (token not provided)")
