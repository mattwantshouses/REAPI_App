# PropertySearch - chatgpt
# Version 2 - claude
!pip install requests

import requests
import pandas as pd
import json

# Step 1: Set the URL for the API request
url = "https://api.realestateapi.com/v2/PropertySearch"

# Step 2: Define the payload with multiple property types
payload = {
    "ids_only": True,
    "obfuscate": False,
    "summary": True,
    "size": 100,
    "count": True,
    "state": "FL",
    "county": "duval",
   # OR condition for property types
    # "or": [
    #     {"property_type": "SFR"},
    #     {"property_type": "CONDO"},
    #     {"property_type": "MULTI-FAMILY"}
    # ],
#    "absentee_owner": True,
    "corporate_owned": False,
    "death": False,
#    "foreclosure": True,
#    "out_of_state_owner": True,
    "pre_foreclosure": True,
}

# Step 3: Set the headers for the API request
headers = {
    "accept": "application/json",
    "x-user-id": "UniqueUserIdentifier",
    "content-type": "application/json",
    "x-api-key": '<Your API Key>',
}

# Step 4: Make the POST request to the API
response = requests.post(url, json=payload, headers=headers)

# Step 5: Parse the JSON response and handle potential errors
if response.status_code == 200:
    data = response.json()

    print("Raw API response:", response.text)
    # Check if 'data' key exists in the response, which usually contains property details
    if 'data' in data:
        df = pd.DataFrame(data['data'])
        print(df)
    else:
        print("Error: 'data' key not found in response. Check the API documentation for the correct response structure.")
        print("Raw response:", data)  # Print the raw response for debugging
else:
    print(f"API request failed with status code: {response.status_code}")
    print("Error response:", response.text)  # Print the error response for debugging


# Step 6: Print DataFrame and handle errors
if 'df' in locals():
    # Additional operations on df can be performed here
    print("Additional DataFrame operations can be performed here if needed.")
else:
    print("DataFrame 'df' was not created due to issues in the API response.")

# Step 7: Save the IDs to a JSON file
with open('ids_response.json', 'w') as f:
    json.dump(data['data'], f)
# Step 8: Save the full API response to a JSON file
with open('count_response.json', 'w') as f:
    json.dump(data, f)
