# CIS 6930 - Project 1: Canvassing the Scene

This project analyzes incidents (crimes, traffic crashes, and arrests) from the **City of Gainesville's Open Data Portal**.  
The goal is to identify all incidents occurring **within 1 km** of the worst traffic crash (the crash involving the highest number of people).

---

## Data Sources

The following datasets are used, sourced from the City of Gainesville:
- **Arrests:** [Arrests Data](https://data.cityofgainesville.org/Public-Safety/Arrests/aum6-79zv)
- **Traffic Crashes:** [Traffic Crashes Data](https://data.cityofgainesville.org/Public-Safety/Traffic-Crashes/iecn-3sxx)
- **Crime Responses:** [Crime Responses Data](https://data.cityofgainesville.org/Public-Safety/Crime-Responses/gvua-xt9q)

---

## Project Structure

cis6930sp25-project1/ |-- main.py # Main script to run the analysis |-- data_extractor.py # Fetches and cleans data from the City of Gainesville's API |-- test_main.py # Unit tests for core functionality |-- custom_tests.py # Additional custom test cases |-- README.md # This file |-- requirements.txt # Dependencies (optional - can be generated if needed)


---

## Setup

1. Clone the repository:
    ```bash
    git clone https://github.com/Anshita382/cis6930sp25-project1.git
    ```

2. Navigate to the project folder:
    ```bash
    cd cis6930sp25-project1
    ```

3. Install required libraries:
    ```bash
    pip install -r requirements.txt
    ```

---

Usage

Run the analysis for a given date using:
```bash
python main.py --year 2025 --month 2 --day 28
```
This will:

Fetch incidents data for the specified date.
Identify the worst traffic crash (most people involved).
Find all other incidents (crimes, arrests, traffic crashes) that occurred within 1 km of that crash.
Output the results sorted by number of people involved (descending) and case number (descending).

Running Tests
You can run all tests using:


pytest
This will automatically pick up test_main.py and custom_tests.py.

Requirements
Python 3.8+
Libraries: pandas, duckdb, requests, pytest

Example Output

5   202500001
3   202500002
This means:

Case 202500001 involved 5 people and occurred within 1 km of the worst crash.
Case 202500002 involved 3 people and also occurred nearby.

Notes
The 1 km radius check uses the Haversine formula, which calculates the great-circle distance between two coordinates.
Incidents with missing or invalid coordinates are automatically skipped.

Author
Anshita Rayalla
Repository: cis6930sp25-project1
University of Florida, Spring 2025

