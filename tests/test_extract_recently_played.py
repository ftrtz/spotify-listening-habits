import pytest
import pandas as pd
import json
from datetime import datetime
from dags.spotify_utils import extract_recently_played

def test_extract_recently_played():
    # Mock JSON response similar to the one returned by the Spotify API
    mock_response = {
        "items": [
            {
                "played_at": "2024-09-23T15:02:15.005Z",
                "track": {
                    "id": "track_1",
                    "name": "Track One",
                    "popularity": 85,
                    "duration_ms": 180000,
                    "artists": [
                        {"id": "artist_1", "name": "Artist One"},
                        {"id": "artist_2", "name": "Artist Two"}
                    ],
                    "album": {
                        "id": "album_1",
                        "name": "Album One",
                        "images": [
                            {"url": "image_1_url"},
                            {"url": "image_2_url"},
                            {"url": "image_3_url"}
                        ]
                    },
                    "uri": "spotify:track:track_1"
                }
            },
            {
                "played_at": "2023-11-01T14:54:27.000Z",
                "track": {
                    "id": "track_2",
                    "name": "Track Two",
                    "popularity": 90,
                    "duration_ms": 200000,
                    "artists": [
                        {"id": "artist_3", "name": "Artist Three"}
                    ],
                    "album": {
                        "id": "album_2",
                        "name": "Album Two",
                        "images": [
                            {"url": "image_4_url"},
                            {"url": "image_5_url"}
                        ]
                    },
                    "uri": "spotify:track:track_2"
                }
            }
        ]
    }

    # Expected DataFrame
    expected_data = {
        "unix_timestamp": [1727103735005, 1698850467000],
        "played_at": ["2024-09-23T15:02:15.005Z", "2023-11-01T14:54:27.000Z"],
        "track_id": ["track_1", "track_2"],
        "track_name": ["Track One", "Track Two"],
        "popularity": [85, 90],
        "duration_ms": [180000, 200000],
        "artist_ids": [["artist_1", "artist_2"], ["artist_3"]],
        "artist_names": [["Artist One", "Artist Two"], ["Artist Three"]],
        "album_id": ["album_1", "album_2"],
        "album_name": ["Album One", "Album Two"],
        "album_images": [json.dumps([{"url": "image_1_url"}, {"url": "image_2_url"}, {"url": "image_3_url"}]), 
                         json.dumps([{"url": "image_4_url"}, {"url": "image_5_url"}])],
        "track_uri": ["spotify:track:track_1", "spotify:track:track_2"]
    }
    expected_df = pd.DataFrame(expected_data)

    # Call the function with the mock response
    result_df = extract_recently_played(mock_response)

    # Assert that the DataFrame returned by the function matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

