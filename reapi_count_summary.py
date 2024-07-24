# Script Name: REAPI Property Search with Count, Summary, and Comprehensive CSV Output
# Version: 6.8

import requests
import json
import logging
import csv
import pandas as pd
from typing import Dict, Any, List
from google.colab import userdata
from datetime import datetime
import pytz

# 1. Configuration and Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://api.realestateapi.com/v2/PropertySearch"
API_KEY = userdata.get('x-api-key')

if not API_KEY:
    raise ValueError("API key not found in Colab secrets. Please set the 'x-api-key' secret.")

# 2. Helper Functions
# 2.1 API Request Function
def make_api_request(payload: Dict[str, Any]) -> Dict[str, Any]:
    """Make an API request and return the JSON response."""
    headers = {
        "accept": "application/json",
        "x-user-id": "UniqueUserIdentifier",
        "content-type": "application/json",
        "x-api-key": API_KEY
    }
    try:
        response = requests.post(API_URL, json=payload, headers=headers)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        raise

# 2.2 Query Parameter Extraction
def extract_query_params(payload: Dict[str, Any]) -> Dict[str, List[str]]:
    """Extract cities and counties from the payload."""
    cities = []
    counties = []
    for condition in payload.get('and', []):
        if 'or' in condition:
            for or_condition in condition['or']:
                if 'city' in or_condition:
                    cities.append(or_condition['city'])
                elif 'county' in or_condition:
                    counties.append(or_condition['county'])
    return {'cities': cities, 'counties': counties}

# 2.3 API Response Printing
def print_api_response(response: Dict[str, Any]):
    """Print the API response, highlighting any errors."""
    print("API Response:")
    if 'error' in response:
        print("API Error:")
        print(json.dumps(response['error'], indent=2))
    elif 'errors' in response:
        print("API Errors:")
        print(json.dumps(response['errors'], indent=2))
    else:
        print("Response Details:")
        print(json.dumps({k: v for k, v in response.items() if k != 'data'}, indent=2))

# 2.4 Summary Formatting
def format_summary(summary: Dict[str, Any]) -> str:
    """Format the summary data for printing."""
    return json.dumps(summary, indent=2)

# 2.5 JSON to CSV Conversion (Updated)
def json_to_csv(main_response: Dict[str, Any], city_county_summaries: List[Dict[str, Any]], filename: str):
    """Convert JSON data to a readable CSV table and save to a file."""
    if not main_response or not city_county_summaries:
        logger.warning("No data to convert to CSV.")
        return

    # Create a list to hold all rows for the CSV
    csv_rows = []

    # Add main summary
    main_summary = main_response.get('summary', {})
    for key, value in main_summary.items():
        csv_rows.append(['Main Summary', key, str(value)])

    # Add city/county summaries
    for summary in city_county_summaries:
        location = summary.get('location', 'Unknown')
        summary_data = summary.get('summary', {})
        for key, value in summary_data.items():
            csv_rows.append([f'{location} Summary', key, str(value)])

    # Add property data
    properties = main_response.get('data', [])
    for prop in properties:
        for key, value in prop.items():
            csv_rows.append(['Property Data', key, str(value)])

    # Create DataFrame
    df = pd.DataFrame(csv_rows, columns=['Category', 'Field', 'Value'])

    # Save to CSV
    df.to_csv(filename, index=False)
    logger.info(f"CSV file saved: {filename}")

# 3. Main Execution
def main():
    # 3.1 Define base payload
    base_payload = {
        "state": "FL",
        "corporate_owned": False,
        "pre_foreclosure": True,
        "search_range": "3_MONTH",
        "and": [
            {
                "or": [
                    {"city": "Green Cove Springs"},
                    {"city": "Orange Park"},
                    {"city": "Jacksonville Beach"},
                    {"city": "Jacksonville"}
                ]
            },
            {
                "or": [
                    {"county": "Duval"},
                    {"county": "St Johns"},
                    {"county": "Clay"},
                    {"county": "Baker"},
                    {"county": "Orange"}
                ]
            },
            {
                "or": [
                    {"property_type": "SFR"},
                    {"property_type": "CONDO"}
                ]
            }
        ]
    }

    # 3.2 Extract query parameters
    query_params = extract_query_params(base_payload)

    # 3.3 Hardcoded count query
    count_payload = base_payload.copy()
    count_payload["count"] = True
    count_response = make_api_request(count_payload)
    print("\n3.3 Count Query Response:")
    print_api_response(count_response)

    # 3.4 Hardcoded summary query
    summary_payload = base_payload.copy()
    summary_payload["summary"] = True
    summary_response = make_api_request(summary_payload)
    print("\n3.4 Summary Query Response:")
    print_api_response(summary_response)

    # 3.5 Extract and print summaries for each city and county
    print("\n3.5 City and County Summaries:")
    city_county_summaries = []
    for city in query_params['cities']:
        city_payload = base_payload.copy()
        city_payload["and"] = [cond for cond in city_payload["and"] if "city" not in str(cond)]
        city_payload["and"].append({"city": city})
        city_payload["summary"] = True
        city_response = make_api_request(city_payload)
        print(f"\n3.5.1 Summary for {city}:")
        print(format_summary(city_response.get('summary', {})))
        city_county_summaries.append({'location': city, 'summary': city_response.get('summary', {})})

    for county in query_params['counties']:
        county_payload = base_payload.copy()
        county_payload["and"] = [cond for cond in county_payload["and"] if "county" not in str(cond)]
        county_payload["and"].append({"county": county})
        county_payload["summary"] = True
        county_response = make_api_request(county_payload)
        print(f"\n3.5.2 Summary for {county} County:")
        print(format_summary(county_response.get('summary', {})))
        city_county_summaries.append({'location': f"{county} County", 'summary': county_response.get('summary', {})})

    # 3.6 Main query (includes both count and summary)
    main_payload = base_payload.copy()
    main_payload["count"] = True
    main_payload["summary"] = True
    main_response = make_api_request(main_payload)
    print("\n3.6 Main Query Response:")
    print_api_response(main_response)

      # 3.7 Generate CSV filename
    est_tz = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(est_tz)
    date_str = current_datetime.strftime("%m%d%y")
    time_str = current_datetime.strftime("%H%M")
    
    cities_str = "_".join(query_params['cities'][:2])  # Use first two cities to keep filename short
    if len(query_params['cities']) > 2:
        cities_str += "_etc"
    
    csv_filename = f"SummaryCount_{date_str}{time_str}_pre-foreclosures_{cities_str}.csv"

    # 3.8 Save the data to CSV in a readable table format
    json_to_csv(main_response, city_county_summaries, csv_filename)
    print(f"Output saved to CSV as: {csv_filename}")

    # 3.9 Prompt user to download the CSV file
    try:
        files.download(csv_filename)
        print(f"Download initiated for {csv_filename}. Please check your browser's download folder.")
    except Exception as e:
        print(f"An error occurred while trying to initiate the download: {str(e)}")
        print("You can manually download the file from the Colab file browser on the left sidebar.")

    # 3.10 Save the full response to a JSON file with timestamp
    json_filename = f"api_response_{date_str}{time_str}.json"
    with open(json_filename, 'w') as f:
        json.dump(main_response, f, indent=2)
    logger.info(f"Full API response saved to {json_filename}")

if __name__ == "__main__":
    main()