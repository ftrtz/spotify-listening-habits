# spotify-listening-habits
*ETL-Pipeline to extract recently played tracks and corresponding artist info from spotify and load to a postgresql database using Airflow.*

## Airflow DAG & Database Schema
#### Airflow DAG
![dag](https://github.com/ftrtz/spotify-listening-habits/assets/63648399/f45a8952-8c9a-42ee-85b2-0bbf0a992777)

#### ER Diagram of the final Database
![er_diagram](https://github.com/ftrtz/spotify-listening-habits/assets/63648399/cf80122e-7575-4dce-b024-e76d78ceb9ce)

## Prerequisites 
- Spotify Account
- A running PostgreSQL database named *spotify*

## Spotify API
To authorize with the Spotify API needs some preparations
1. Create an app in the Spotify Developers Dashboard ([spotify docs](https://developer.spotify.com/documentation/web-api/concepts/apps)) to retrieve your *Client ID* and *Client Secret* and set a *Redirect URI*
2. Assign the information to the environment variables *SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI* in ```.env``` 
3. Run ```token_init.py``` and follow the instructions. This will create an access token stored in ```dags/.cache```

## Airflow Setup
Use the provided ```docker-compose.yaml```, optionally modify to your needs and spin airflow up with:
```
docker compose up
```
Open the airflow webui, login and add connections and variables
- Connections: Add your postgres database and name it *spotify_postgres*
- Variables: Add the variables *SPOTIPY_CLIENT_ID, SPOTIPY_CLIENT_SECRET, SPOTIPY_REDIRECT_URI*
