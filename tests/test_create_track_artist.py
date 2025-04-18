import pytest
import pandas as pd
import json
from utils.spotify_utils import create_track_artist

def test_create_track_artist():
    # Mock DataFrame similar to the track DataFrame
    mock_tracks = pd.DataFrame({
        "id": ["track_1", "track_2"],
        "track_name": ["Track One", "Track Two"],
        "popularity": [85, 90],
        "duration_ms": [180000, 200000],
        "artist_ids": [["artist_1", "artist_2"], ["artist_3"]],
        "artist_names": [["Artist One", "Artist Two"], ["Artist Three"]],
        "album_id": ["album_1", "album_2"],
        "album_name": ["Album One", "Album Two"],
        "album_images": [
            json.dumps([{"url": "image_1_url"}, {"url": "image_2_url"}]),
            json.dumps([{"url": "image_3_url"}])
        ],
        "uri": ["spotify:track:track_1", "spotify:track:track_2"]
    })

    # Expected DataFrame after processing
    expected_data = {
        "track_id": ["track_1", "track_1", "track_2"],
        "artist_id": ["artist_1", "artist_2", "artist_3"],
        "artist_position": [0, 1, 0]
    }
    expected_df = pd.DataFrame(expected_data)

    # Call the function with the mock DataFrame
    result_df = create_track_artist(mock_tracks)

    print(result_df)

    # Assert that the DataFrame returned by the function matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

