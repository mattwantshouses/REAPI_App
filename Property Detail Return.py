# Script Name: REAPI Property Detail Retrieval with Pagination and CSV Output
# Version: 1.0

import requests
import json
import logging
import pandas as pd
from typing import Dict, Any, List
from google.colab import userdata, files
from datetime import datetime
import pytz
import io
import os
import time

# 1. Configuration and Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

API_URL = "https://api.realestateapi.com/v2/PropertyDetail"
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
        json_response = response.json()
        if 'error' in json_response or 'errors' in json_response:
            handle_api_error(json_response)
        return json_response
    except requests.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        raise

# 2.2 API Error Handling
def handle_api_error(response: Dict[str, Any]):
    """Handle API errors and print detailed error information."""
    if 'error' in response:
        print("API Error:")
        print(json.dumps(response['error'], indent=2))
    elif 'errors' in response:
        print("API Errors:")
        print(json.dumps(response['errors'], indent=2))

# 2.3 File Selection Function
def select_ids_file() -> str:
    """Allow user to select the IDs file or upload a new one."""
    files_list = [f for f in os.listdir() if f.startswith('ids_only_') and f.endswith('.json')]
    
    if not files_list:
        print("No existing IDs files found. Please upload a file.")
        uploaded = files.upload()
        return list(uploaded.keys())[0]
    
    print("Available IDs files:")
    for i, file in enumerate(files_list, 1):
        print(f"{i}. {file}")
    
    while True:
        choice = input("Enter the number of the file to use, or 'u' to upload a new file: ")
        if choice.lower() == 'u':
            uploaded = files.upload()
            return list(uploaded.keys())[0]
        try:
            index = int(choice) - 1
            if 0 <= index < len(files_list):
                return files_list[index]
        except ValueError:
            pass
        print("Invalid choice. Please try again.")

# 2.4 Load IDs Function
def load_ids(filename: str) -> List[str]:
    """Load property IDs from the selected file."""
    _, ext = os.path.splitext(filename)
    if ext == '.json':
        with open(filename, 'r') as f:
            return json.load(f)
    elif ext == '.csv':
        df = pd.read_csv(filename)
        return df['id'].tolist()
    elif ext == '.txt':
        with open(filename, 'r') as f:
            return [line.strip() for line in f if line.strip()]
    else:
        raise ValueError(f"Unsupported file format: {ext}")

# 2.5 Pagination Function
def paginated_property_detail_retrieval(ids: List[str], batch_size: int = 50) -> List[Dict[str, Any]]:
    """Retrieve property details with pagination."""
    all_results = []
    total_ids = len(ids)
    
    for i in range(0, total_ids, batch_size):
        batch = ids[i:i+batch_size]
        logger.info(f"Processing batch {i//batch_size + 1} of {(total_ids + batch_size - 1)//batch_size}")
        
        for property_id in batch:
            payload = {"id": property_id}
            try:
                response = make_api_request(payload)
                all_results.append(response)
            except Exception as e:
                logger.error(f"Error processing property ID {property_id}: {str(e)}")
            
            # Add a small delay to avoid overwhelming the API
            time.sleep(0.5)
    
    return all_results

# 3. Main Execution
def main():
    # 3.1 Select and load property IDs
    ids_file = select_ids_file()
    property_ids = load_ids(ids_file)
    logger.info(f"Loaded {len(property_ids)} property IDs from {ids_file}")

    # 3.2 Retrieve property details with pagination
    property_details = paginated_property_detail_retrieval(property_ids)

    # 3.3 Convert to DataFrame
    df = pd.json_normalize(property_details)

    # 3.4 Generate timestamp and filename components
    est_tz = pytz.timezone('US/Eastern')
    current_datetime = datetime.now(est_tz)
    date_str = current_datetime.strftime("%m%d%y")
    time_str = current_datetime.strftime("%H%M")

    # Extract summary of the query from the previous cell's output
    # This is a placeholder. You should replace it with actual logic to get the summary.
    query_summary = "property_details"

    # 3.5 Generate CSV filename
    csv_filename = f"PD_{date_str}{time_str}_{query_summary}.csv"

    # 3.6 Save DataFrame to CSV in Colab environment
    df.to_csv(csv_filename, index=False)
    print(f"CSV file saved in Colab: {csv_filename}")

    # 3.7 Display CSV content preview
    print("\nCSV Content Preview:")
    print(df.head().to_string())

    # 3.8 Download the CSV file
    try:
        files.download(csv_filename)
        print(f"\nDownload initiated for {csv_filename}.")
        print("Your browser should prompt you to save the file or it may be automatically saved to your default downloads folder.")
        print("If you don't see a prompt, check your browser's download manager or your default downloads folder.")
    except Exception as e:
        print(f"\nAn error occurred while trying to download the file: {str(e)}")
        print("You can manually download the file from the Colab file browser on the left sidebar.")

if __name__ == "__main__":
    main()