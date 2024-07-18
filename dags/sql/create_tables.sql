-- FINAL TABLES
-- create track table
CREATE TABLE IF NOT EXISTS track (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    popularity INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    album_id VARCHAR NOT NULL,
    album_name VARCHAR NOT NULL,
    album_images JSONB,
    uri VARCHAR NOT NULL,
    created TIMESTAMP,
    updated TIMESTAMP
    );

-- create audio_features table
CREATE TABLE IF NOT EXISTS audio_features (
    track_id VARCHAR PRIMARY KEY REFERENCES track(id),
    danceability FLOAT,
    energy FLOAT,
    key INTEGER,
    loudness FLOAT,
    mode INTEGER,
    speechiness FLOAT,
    acousticness FLOAT,
    instrumentalness FLOAT,
    liveness FLOAT,
    valence FLOAT,
    tempo FLOAT,
    time_signature INTEGER,
    analysis_url VARCHAR,
    created TIMESTAMP,
    updated TIMESTAMP
    );

-- create artist table
CREATE TABLE IF NOT EXISTS artist (
    id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    followers INTEGER NOT NULL,
    genres TEXT[],
    popularity INTEGER NOT NULL,
    uri VARCHAR NOT NULL,
    images JSONB,
    created TIMESTAMP,
    updated TIMESTAMP
    );

-- create artist_track link table
CREATE TABLE IF NOT EXISTS track_artist (
    PRIMARY KEY(track_id, artist_id),
    track_id VARCHAR NOT NULL REFERENCES track(id),
    artist_id VARCHAR NOT NULL REFERENCES artist(id),
    artist_position INTEGER NOT NULL
);

-- create played table
CREATE TABLE IF NOT EXISTS played (
    unix_timestamp BIGINT PRIMARY KEY,
    played_at TIMESTAMP WITH TIME ZONE,
    track_id VARCHAR NOT NULL REFERENCES track(id)
    );