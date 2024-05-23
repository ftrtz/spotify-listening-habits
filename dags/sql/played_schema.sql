-- create played table
CREATE TABLE IF NOT EXISTS {{ params.tbl }} (
    id SERIAL PRIMARY KEY,
    track_spotify_id VARCHAR NOT NULL,
    track_name VARCHAR NOT NULL,
    artists_spotify_id VARCHAR NOT NULL,
    artists_name VARCHAR NOT NULL,
    album_spotify_id VARCHAR NOT NULL,
    album_name VARCHAR NOT NULL,
    played_at TIMESTAMP WITH TIME ZONE
    );
