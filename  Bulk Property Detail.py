# Script Name: REAPI Bulk Property Detail Query with CSV output
# Version: 1.0

import requests
import json
import logging
import pandas as pd
from google.colab import userdata, files
from datetime import datetime
import pytz
import io
import os
import glob

# 1. Configuration and Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://api.realestateapi.com/v2/PropertyDetailBulk"
API_KEY = userdata.get('x-api-key')

if not API_KEY:
    raise ValueError("API key not found in Colab secrets. Please set the 'x-api-key' secret.")

# 2. Helper Functions
# 2.1 API Request Function
def make_api_request(payload):
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

# 2.2 File Selection Function
def select_ids_file():
    """Allow user to select the IDs file or upload a new one."""
    id_files = glob.glob("ids_only_*.json")

    if not id_files:
        print("No existing ID files found. Please upload a file containing property IDs.")
        uploaded = files.upload()
        if not uploaded:
            raise ValueError("No file uploaded. Cannot proceed without property IDs.")
        return list(uploaded.keys())[0]

    print("Available ID files:")
    for i, file in enumerate(id_files, 1):
        print(f"{i}. {file}")

    while True:
        choice = input("Enter the number of the file to use, or 'u' to upload a new file: ")
        if choice.lower() == 'u':
            uploaded = files.upload()
            if not uploaded:
                print("No file uploaded. Please try again.")
                continue
            return list(uploaded.keys())[0]
        try:
            index = int(choice) - 1
            if 0 <= index < len(id_files):
                return id_files[index]
        except ValueError:
            pass
        print("Invalid choice. Please try again.")

# 2.3 Load IDs Function
def load_ids(filename):
    """Load property IDs from the selected file."""
    _, ext = os.path.splitext(filename)
    if ext.lower() == '.json':
        with open(filename, 'r') as f:
            data = json.load(f)
            if isinstance(data, list):
                return data
            elif isinstance(data, dict) and 'data' in data:
                return data['data']
    elif ext.lower() == '.csv':
        df = pd.read_csv(filename)
        return df['id'].tolist() if 'id' in df.columns else df.iloc[:, 0].tolist()
    elif ext.lower() == '.txt':
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    raise ValueError(f"Unsupported file format: {ext}")

# 3. Main Execution
def main():
    # 3.1 Select and load property IDs
    id_file = select_ids_file()
    property_ids = load_ids(id_file)
    logger.info(f"Loaded {len(property_ids)} property IDs from {id_file}")

    # 3.2 Prepare API payload
    payload = {"ids": property_ids}

    # 3.3 Make API request
    try:
        response = make_api_request(payload)
    except Exception as e:
        logger.error(f"Failed to get property details: {str(e)}")
        return

    # 3.4 Process API response
    if 'error' in response or 'errors' in response:
        logger.error("API returned an error:")
        logger.error(json.dumps(response.get('error') or response.get('errors'), indent=2))
        return

    # 3.5 Convert response to DataFrame
    df = pd.json_normalize(response.get('data', []))

    # 3.6 Generate filename
    est_tz = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(est_tz)
    date_str = current_datetime.strftime("%m%d%y%H%M")
    summary = "_".join(id_file.split('_')[2:]).replace('.json', '')  # Extract summary from id file name
    filename = f"BulkPD{date_str}_{summary}.csv"

    # 3.7 Save DataFrame to CSV in Colab environment
    df.to_csv(filename, index=False)
    logger.info(f"CSV file saved in Colab: {filename}")

    # 3.8 Display CSV content preview
    print("\nCSV Content Preview:")
    print(df.head().to_string())

    # 3.9 Download the CSV file
    try:
        files.download(filename)
        print(f"\nDownload initiated for {filename}.")
        print("Your browser should prompt you to save the file or it may be automatically saved to your default downloads folder.")
        print("If you don't see a prompt, check your browser's download manager or your default downloads folder.")
    except Exception as e:
        print(f"\nAn error occurred while trying to download the file: {str(e)}")
        print("You can manually download the file from the Colab file browser on the left sidebar.")

if __name__ == "__main__":
    main()