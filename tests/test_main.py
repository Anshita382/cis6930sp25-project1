import pytest
from unittest.mock import patch
from data_extractor import fetch_data
import main

def test_distance_calculation():
    coord1 = (29.6516, -82.3248)
    coord2 = (29.6520, -82.3252)
    distance = main.haversine(coord1[0], coord1[1], coord2[0], coord2[1])
    assert distance < 0.1  # Less than 100 meters

# @patch("data_extractor.requests.get")
# def test_fetch_data(mock_get):
#     mock_response = [
#         {
#             "case_number": "202500001",
#             "arrest_date": "2025-01-01T12:34:56.000",
#             "latitude": "29.6516",
#             "longitude": "-82.3248",
#             "totalpeopleinvolved": 5
#         }
#     ]
#     mock_get.return_value.json.return_value = mock_response

#     data = fetch_data(main.ARRESTS_URL, "arrest_date", 2025, 1, 1)
#     assert len(data) == 1
#     assert data[0]["case_number"] == "202500001"

# def test_find_incidents_within_radius():
#     # This is a helper function you need to add in main.py (I'll show it below)
#     incidents = [
#         ("202500001", 29.6516, -82.3248, 5),   # Exactly at center
#         ("202500002", 29.6520, -82.3252, 3),   # Very close
#         ("202500003", 30.0000, -83.0000, 2)    # Far away
#     ]
#     center = (29.6516, -82.3248)

#     nearby = main.find_incidents_within_radius(incidents, center)

#     assert len(nearby) == 2
#     assert ("202500001", 5) in nearby
#     assert ("202500002", 3) in nearby
#     assert ("202500003", 2) not in nearby
