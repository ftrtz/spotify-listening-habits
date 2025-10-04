import pandas as pd
import json
from src.spotify_etl.utils import get_track_from_played

def test_get_track_from_played():
    # Mock DataFrame similar to the played tracks DataFrame
    mock_played = pd.DataFrame({
        "unix_timestamp": [1632130496789, 1632232830123, 1632330496789],  # Added timestamps
        "played_at": ["2021-09-20T12:34:56.789Z", "2021-09-21T14:20:30.123Z", "2021-09-22T16:30:45.456Z"],  # Added played_at
        "track_id": ["track_1", "track_2", "track_3"],  # Added a third track_id
        "track_name": ["Track One", "Track Two", "Track Three"],
        "popularity": [85, 90, 95],
        "duration_ms": [180000, 200000, 240000],
        "artist_ids": [["artist_1", "artist_2"], ["artist_3"], ["artist_4"]],
        "artist_names": [["Artist One", "Artist Two"], ["Artist Three"], ["Artist Four"]],
        "album_id": ["album_1", "album_2", "album_3"],
        "album_name": ["Album One", "Album Two", "Album Three"],
        "album_images": [
            json.dumps([{"url": "image_1_url"}, {"url": "image_2_url"}, {"url": "image_3_url"}]),
            json.dumps([{"url": "image_4_url"}, {"url": "image_5_url"}]),
            json.dumps([{"url": "image_6_url"}])
        ],
        "track_uri": ["spotify:track:track_1", "spotify:track:track_2", "spotify:track:track_3"]
    })

    # Expected DataFrame after processing
    expected_data = {
        "id": ["track_1", "track_2", "track_3"],
        "name": ["Track One", "Track Two", "Track Three"],
        "popularity": [85, 90, 95],
        "duration_ms": [180000, 200000, 240000],
        "artist_ids": [["artist_1", "artist_2"], ["artist_3"], ["artist_4"]],
        "artist_names": [["Artist One", "Artist Two"], ["Artist Three"], ["Artist Four"]],
        "album_id": ["album_1", "album_2", "album_3"],
        "album_name": ["Album One", "Album Two", "Album Three"],
        "album_images": [
            json.dumps([{"url": "image_1_url"}, {"url": "image_2_url"}, {"url": "image_3_url"}]),
            json.dumps([{"url": "image_4_url"}, {"url": "image_5_url"}]),
            json.dumps([{"url": "image_6_url"}])
        ],
        "uri": ["spotify:track:track_1", "spotify:track:track_2", "spotify:track:track_3"]
    }
    expected_df = pd.DataFrame(expected_data)

    # Call the function with the mock DataFrame
    result_df = get_track_from_played(mock_played)

    # Assert that the DataFrame returned by the function matches the expected DataFrame
    pd.testing.assert_frame_equal(result_df.reset_index(drop=True), expected_df.reset_index(drop=True))

