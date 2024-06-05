-- FINAL TABLES
-- create track table
CREATE TABLE IF NOT EXISTS track (
    id VARCHAR PRIMARY KEY,
    name VARCHAR NOT NULL,
    album_id VARCHAR NOT NULL,
    album_name VARCHAR NOT NULL,
    album_images JSONB,
    uri VARCHAR NOT NULL
    );

-- create artist table
CREATE TABLE IF NOT EXISTS artist (
    id VARCHAR NOT NULL PRIMARY KEY,
    name VARCHAR NOT NULL,
    followers VARCHAR NOT NULL,
    genres TEXT[],
    popularity VARCHAR NOT NULL,
    uri VARCHAR NOT NULL,
    images JSONB
    );

-- create artist_track link table
CREATE TABLE IF NOT EXISTS track_artist (
    PRIMARY KEY(track_id, artist_id),
    track_id VARCHAR NOT NULL REFERENCES track(id),
    artist_id VARCHAR NOT NULL REFERENCES artist(id)
);

-- create played table
CREATE TABLE IF NOT EXISTS played (
    unix_timestamp VARCHAR PRIMARY KEY,
    played_at TIMESTAMP WITH TIME ZONE,
    track_id VARCHAR NOT NULL REFERENCES track(id)
    );