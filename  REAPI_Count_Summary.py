# Script Name: REAPI Property Search (Count, Summary) with Dynamic Query and Enhanced Response Handling
# Version: 6.2

import requests
import json
import logging
from typing import Dict, Any, List
from google.colab import userdata

# Configuration and Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://api.realestateapi.com/v2/PropertySearch"
API_KEY = userdata.get('x-api-key')

if not API_KEY:
    raise ValueError("API key not found in Colab secrets. Please set the 'x-api-key' secret.")

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

def format_summary(summary: Dict[str, Any]) -> str:
    """Format the summary dictionary into a readable string."""
    return json.dumps(summary, indent=2)

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

def print_api_response(response: Dict[str, Any]):
    """Print the API response, excluding the 'data' field and highlighting any errors."""
    print("API Response:")
    response_copy = response.copy()
    response_copy.pop('data', None)  # Remove 'data' field if present
    
    # Check for and print any error messages
    if 'error' in response_copy:
        print("API Error:")
        print(json.dumps(response_copy['error'], indent=2))
    elif 'errors' in response_copy:
        print("API Errors:")
        print(json.dumps(response_copy['errors'], indent=2))
    
    # Print the rest of the response
    print("Response Details:")
    print(json.dumps(response_copy, indent=2))

def main():
    base_payload = {
        "summary": True,
        "count" : True,
 #       "size": 1000,
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

    # Extract cities and counties from the payload
    query_params = extract_query_params(base_payload)

    # Make the API request
    response = make_api_request(base_payload)
    
    # Print the API response
    print_api_response(response)
    
    # Extract and print the overall summary
    overall_summary = response.get('summary', {})
    print("\nOverall Summary:")
    print(format_summary(overall_summary))
    
    # Extract and print summaries for each city and county
    for city in query_params['cities']:
        city_payload = base_payload.copy()
        city_payload["and"] = [cond for cond in city_payload["and"] if "city" not in str(cond)]
        city_payload["and"].append({"city": city})
        city_response = make_api_request(city_payload)
        print(f"\nSummary for {city}:")
        print(format_summary(city_response.get('summary', {})))
    
    for county in query_params['counties']:
        county_payload = base_payload.copy()
        county_payload["and"] = [cond for cond in county_payload["and"] if "county" not in str(cond)]
        county_payload["and"].append({"county": county})
        county_response = make_api_request(county_payload)
        print(f"\nSummary for {county} County:")
        print(format_summary(county_response.get('summary', {})))
    
    # Save the full response to a JSON file
    with open('api_response.json', 'w') as f:
        json.dump(response, f, indent=2)
    logger.info("Full API response saved to api_response.json")

if __name__ == "__main__":
    main()