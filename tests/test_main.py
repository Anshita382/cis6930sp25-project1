import main
import pytest
from unittest.mock import patch

def test_distance_calculation():
    coord1 = (29.6516, -82.3248)  
    coord2 = (29.6520, -82.3252)  
    distance = main.calculate_distance(coord1, coord2)
    assert distance < 0.1  

@patch("main.requests.get")
def test_fetch_data(mock_get):
    mock_response = [
        {"case_number": "202500001", "datetime": "2025-01-01T12:34:56.000", "latitude": "29.6516", "longitude": "-82.3248", "total_involved": 5}
    ]
    mock_get.return_value.json.return_value = mock_response

    data = main.fetch_data(main.ARRESTS_URL, "2025-01-01")
    assert len(data) == 1
    assert data[0]["case_number"] == "202500001"

def test_find_incidents_within_radius():
    incidents = [
        ("202500001", "29.6516", "-82.3248", 5),  
        ("202500002", "29.6520", "-82.3252", 3), 
        ("202500003", "30.0000", "-83.0000", 2)   
    ]
    center = (29.6516, -82.3248)
    nearby = main.find_incidents_within_radius(incidents, center)

    assert len(nearby) == 2
    assert ("202500002", 3) in nearby
    assert ("202500003", 2) not in nearby
