-- create played table
CREATE TABLE IF NOT EXISTS played (
    id SERIAL PRIMARY KEY,
    track_spotify_id VARCHAR NOT NULL,
    track_name VARCHAR NOT NULL,
    artists_spotify_id VARCHAR NOT NULL,
    artists_name VARCHAR NOT NULL,
    album_spotify_id VARCHAR NOT NULL,
    album_name VARCHAR NOT NULL,
    played_at TIMESTAMP WITH TIME ZONE
    );

-- create artist table
CREATE TABLE IF NOT EXISTS artist (
    id SERIAL PRIMARY KEY,
    spotify_id VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    followers VARCHAR NOT NULL,
    genres VARCHAR NOT NULL,
    popularity VARCHAR NOT NULL,
    uri VARCHAR NOT NULL
    );

-- create played_artist table
CREATE TABLE IF NOT EXISTS played_artist (
    id SERIAL PRIMARY KEY,
    played_id INT REFERENCES played(id),
    artist_id INT REFERENCES artist(id)
    );
