import pytest
import pandas as pd
import json
from utils.spotify_utils import extract_artists

def test_extract_artists():
    # Mock JSON response similar to the one returned by the Spotify API
    mock_response = {
        "artists": [
            {
                "id": "artist_1",
                "name": "Artist One",
                "followers": {"total": 1000},
                "genres": ["pop", "rock"],
                "popularity": 90,
                "uri": "spotify:artist:artist_1",
                "images": [
                    {"url": "image_1_url"},
                    {"url": "image_2_url"}
                ]
            },
            {
                "id": "artist_2",
                "name": "Artist Two",
                "followers": {"total": 2000},
                "genres": ["jazz", "blues"],
                "popularity": 80,
                "uri": "spotify:artist:artist_2",
                "images": [
                    {"url": "image_3_url"},
                    {"url": "image_4_url"}
                ]
            }
        ]
    }

    # Expected DataFrame
    expected_data = {
        "id": ["artist_1", "artist_2"],
        "name": ["Artist One", "Artist Two"],
        "followers": [1000, 2000],
        "genres": ['{"pop","rock"}', '{"jazz","blues"}'],
        "popularity": [90, 80],
        "uri": ["spotify:artist:artist_1", "spotify:artist:artist_2"],
        "images": [
            json.dumps([{"url": "image_1_url"}, {"url": "image_2_url"}]),
            json.dumps([{"url": "image_3_url"}, {"url": "image_4_url"}])
        ]
    }
    expected_df = pd.DataFrame(expected_data)

    # Call the function with the mock response
    result_df = extract_artists(mock_response)

    # Assert that the DataFrame returned by the function matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

