# spotify-listening-habits
*ETL-Pipeline to extract recently played tracks and corresponding artist info from spotify and load to a postgresql database using Airflow.*

## Airflow DAG & Database Schema
#### Airflow DAG
![airflow dag](https://github.com/ftrtz/spotify-listening-habits/assets/63648399/1f809f2d-2d96-434c-9b70-5725cb5d4ee1)


#### ER Diagram of the final Database
![spotify - public](https://github.com/ftrtz/spotify-listening-habits/assets/63648399/c34b31d8-971c-45ad-89dd-2ff417b3b5f2)

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
