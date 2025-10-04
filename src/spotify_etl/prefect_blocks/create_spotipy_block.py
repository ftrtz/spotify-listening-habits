from prefect.blocks.system import Secret
import getpass

def create_spotipy_block(block_name: str = "spotipy"):
    """
    Interactively create or update a Prefect Secret block for Spotipy credentials.

    Prompts the user for Spotify Developer credentials (Client ID, Client Secret, Redirect URI).
    If a block with the given name exists, asks for confirmation before overwriting.
    Stores the credentials as a JSON object in the Prefect block store.

    Args:
        block_name (str): The name of the Prefect Secret block to create or update (default: "spotipy").
    """
    # Check if block exists
    try:
        Secret.load(block_name)
        print(f"⚠️  A Prefect Secret block named '{block_name}' already exists.")
        overwrite = input("Do you want to overwrite it? (y/N): ").strip().lower()
        if overwrite != "y":
            print("Aborted. No changes made.")
            return
    except Exception:
        # Block does not exist, continue
        pass

    # Spotipy credentials
    print("Enter Spotipy credentials (from your Spotify Developer Dashboard - https://developer.spotify.com/):")
    client_id = input("SPOTIPY_CLIENT_ID: ").strip()
    client_secret = getpass.getpass("SPOTIPY_CLIENT_SECRET: ").strip()
    redirect_uri = input("SPOTIPY_REDIRECT_URI: ").strip()

    spotipy_data = {
        "SPOTIPY_CLIENT_ID": client_id,
        "SPOTIPY_CLIENT_SECRET": client_secret,
        "SPOTIPY_REDIRECT_URI": redirect_uri,
    }
    Secret(value=spotipy_data).save(block_name, overwrite=True)
    print(f"✅ Created Prefect Secret block: {block_name}")
