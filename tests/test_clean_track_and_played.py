import pandas as pd
from src.spotify_etl.utils import clean_track_and_played

def test_clean_track_and_played():
    # Create sample data for the track DataFrame
    track_data = {
        "id": ["a1", "b2", "c3", None, ""],
        "name": ["Song A", "Song B", "Song C", "Song D", "Song E"],
        "popularity": [90, 80, 85, 70, 50],
        "duration_ms": [180000, 240000, 210000, 220000, 220000],
        "album_id": [100, 101, 102, 103, 104],
        "album_name": ["Album A", "Album B", "Album C", "Album D", "Album A"],
        "uri": ["uri1", "uri2", "uri3", None, ""]
    }
    track_df = pd.DataFrame(track_data)

    # Create sample data for the played DataFrame
    played_data = {
        "unix_timestamp": [1609459200000, 1609459210000, 1609459215000, 1609459220000, 1609459255000],
        "played_at": ["2021-01-01 00:00:00", "2021-01-01 00:00:10", "2021-01-01 00:00:15", "2021-01-01 00:00:20", "2021-01-01 00:00:55"],
        "track_id": ["a1", "a1", "a1", "b2", "b2"]
    }
    played_df = pd.DataFrame(played_data)

    # Expected cleaned track DataFrame (null rows removed)
    expected_track_data = {
        "id": ["a1", "b2", "c3"],
        "name": ["Song A", "Song B", "Song C"],
        "popularity": [90, 80, 85],
        "duration_ms": [180000, 240000, 210000],
        "album_id": [100, 101, 102],
        "album_name": ["Album A", "Album B", "Album C"],
        "uri": ["uri1", "uri2", "uri3"]
    }
    expected_track_df = pd.DataFrame(expected_track_data)

    # Expected cleaned played DataFrame (filtering and deduplication)
    expected_played_data = {
        "unix_timestamp": [1609459200000, 1609459220000, 1609459255000],
        "played_at": ["2021-01-01 00:00:00", "2021-01-01 00:00:20", "2021-01-01 00:00:55"],
        "track_id": ["a1", "b2", "b2"]
    }
    expected_played_df = pd.DataFrame(expected_played_data)

    # Call the function
    cleaned_track_df, cleaned_played_df = clean_track_and_played(track_df, played_df)

    # Assert that the cleaned track and played DataFrames match the expected output
    pd.testing.assert_frame_equal(cleaned_track_df.reset_index(drop=True), expected_track_df.reset_index(drop=True))
    pd.testing.assert_frame_equal(cleaned_played_df.reset_index(drop=True), expected_played_df.reset_index(drop=True))

