-- CREATE SCHEMAS
CREATE SCHEMA IF NOT EXISTS {{ prod_schema }};
CREATE SCHEMA IF NOT EXISTS {{ staging_schema }};


-- CREATE TABLES ON PROD
-- create track table
CREATE TABLE IF NOT EXISTS {{ prod_schema }}.track (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    popularity INTEGER NOT NULL,
    duration_ms INTEGER NOT NULL,
    album_id VARCHAR NOT NULL,
    album_name VARCHAR NOT NULL,
    album_images JSONB,
    uri VARCHAR NOT NULL,
    created TIMESTAMP WITH TIME ZONE,
    updated TIMESTAMP WITH TIME ZONE
    );


-- create audio_features table
CREATE TABLE IF NOT EXISTS {{ prod_schema }}.audio_features (
    track_id VARCHAR PRIMARY KEY REFERENCES {{ prod_schema }}.track(id),
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
    created TIMESTAMP WITH TIME ZONE,
    updated TIMESTAMP WITH TIME ZONE
    );

-- create artist table
CREATE TABLE IF NOT EXISTS {{ prod_schema }}.artist (
    id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    followers INTEGER NOT NULL,
    genres TEXT[],
    popularity INTEGER NOT NULL,
    uri VARCHAR NOT NULL,
    images JSONB,
    created TIMESTAMP WITH TIME ZONE,
    updated TIMESTAMP WITH TIME ZONE
    );

-- create artist_track link table
CREATE TABLE IF NOT EXISTS {{ prod_schema }}.track_artist (
    PRIMARY KEY(track_id, artist_id),
    track_id VARCHAR NOT NULL REFERENCES {{ prod_schema }}.track(id),
    artist_id VARCHAR NOT NULL REFERENCES {{ prod_schema }}.artist(id),
    artist_position INTEGER NOT NULL
);

-- create played table
CREATE TABLE IF NOT EXISTS {{ prod_schema }}.played (
    unix_timestamp BIGINT PRIMARY KEY,
    played_at TIMESTAMP WITH TIME ZONE,
    track_id VARCHAR NOT NULL REFERENCES {{ prod_schema }}.track(id)
    );
