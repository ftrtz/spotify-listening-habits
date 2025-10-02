import spotipy
from spotipy.oauth2 import SpotifyOAuth
from prefect.blocks.system import Secret


spotipy_block = Secret.load("spotipy")

sp = spotipy.Spotify(auth_manager=SpotifyOAuth(
    client_id=spotipy_block.get()["SPOTIPY_CLIENT_ID"],
    client_secret=spotipy_block.get()["SPOTIPY_CLIENT_SECRET"],
    redirect_uri=spotipy_block.get()["SPOTIPY_REDIRECT_URI"],
    scope="user-read-recently-played",
    cache_path=".cache"
))

# Make any request to invoke the authentification
sp.user_playlists('spotify')