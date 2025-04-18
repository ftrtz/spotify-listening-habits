import pytest
import pandas as pd
from utils.spotify_utils import extract_audio_features

def test_extract_audio_features():
    # Mock JSON response similar to the one returned by the Spotify API
    mock_response = [
        {
            "id": "track_1",
            "danceability": 0.8,
            "energy": 0.9,
            "key": 5,
            "loudness": -5.2,
            "mode": 1,
            "speechiness": 0.05,
            "acousticness": 0.1,
            "instrumentalness": 0.00001,
            "liveness": 0.12,
            "valence": 0.85,
            "tempo": 120.0,
            "time_signature": 4,
            "analysis_url": "http://example.com/track_1_analysis"
        },
        {
            "id": "track_2",
            "danceability": 0.7,
            "energy": 0.8,
            "key": 2,
            "loudness": -6.1,
            "mode": 0,
            "speechiness": 0.06,
            "acousticness": 0.15,
            "instrumentalness": 0.00002,
            "liveness": 0.20,
            "valence": 0.75,
            "tempo": 130.0,
            "time_signature": 4,
            "analysis_url": "http://example.com/track_2_analysis"
        }
    ]

    # Expected DataFrame
    expected_data = {
        "track_id": ["track_1", "track_2"],
        "danceability": [0.8, 0.7],
        "energy": [0.9, 0.8],
        "key": [5, 2],
        "loudness": [-5.2, -6.1],
        "mode": [1, 0],
        "speechiness": [0.05, 0.06],
        "acousticness": [0.1, 0.15],
        "instrumentalness": [0.00001, 0.00002],
        "liveness": [0.12, 0.20],
        "valence": [0.85, 0.75],
        "tempo": [120.0, 130.0],
        "time_signature": [4, 4],
        "analysis_url": [
            "http://example.com/track_1_analysis",
            "http://example.com/track_2_analysis"
        ]
    }
    expected_df = pd.DataFrame(expected_data)

    # Call the function with the mock response
    result_df = extract_audio_features(mock_response)

    # Assert that the DataFrame returned by the function matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

