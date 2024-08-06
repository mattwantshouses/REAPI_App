# Script Name: Single Record SkipTrace Processing for Colab with Enhanced Features
# Version: 3.9 (improved address parsing to handle multiple scenarios correctly)


# 1. Imports and Configuration
# 1.1 Import required libraries
import os
import json
import time
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import requests
import csv
import pandas as pd
import glob
from tqdm import tqdm
from google.colab import auth, drive, files, userdata
from google.auth import default
from datetime import datetime

# 1.2 Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 1.3 Set up constants
API_URL = 'https://api.realestateapi.com/v1/SkipTrace'
OUTPUT_FOLDER = '/content/drive/MyDrive/B - RMD Home Buyers/RMD Marketing/RMD Marketing Lists/2024/Pre-Foreclosure Project May 2024/PFC Project - 2 - Skip Traced Files/PFC Project - 2.5 - Single Skip Traced Files'  # Placeholder for Google Drive output
COLAB_OUTPUT_FOLDER = '/content/skip_trace_results'  # Folder in Colab environment
RATE_LIMIT = 10  # Requests per second
RETRY_DELAY = 60  # Seconds to wait after hitting rate limit
REQUEST_DELAY = 0.1  # Seconds to wait between requests

# 2. Helper Functions
# 2.1 File Selection Function
def select_file() -> Optional[str]:
    """Present a list of files for the user to select or upload a new file."""
    files_list = [f for f in os.listdir() if f.endswith(('.json', '.txt', '.csv', '.xlsx', '.xls'))]

    print("Available files:")
    for i, file in enumerate(files_list, 1):
        print(f"{i}. {file}")
    print(f"{len(files_list) + 1}. Upload a new file")

    while True:
        try:
            selection = int(input(f"Enter your choice (1-{len(files_list) + 1}): "))
            if 1 <= selection <= len(files_list):
                return files_list[selection - 1]
            elif selection == len(files_list) + 1:
                uploaded = files.upload()
                if uploaded:
                    return list(uploaded.keys())[0]
                else:
                    logger.error("No file was uploaded.")
                    return None
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

# 2.2 Prepare skip trace input
def prepare_skip_trace_input(row: pd.Series) -> Dict[str, Any]:
    """Prepare a single row for the SkipTrace API input."""
    # 2.2.1 Initialize the payload dictionary
    payload = {
        'first_name': row['Owner 1 First Name'],
        'last_name': row['Owner 1 Last Name']
    }

    # 2.2.2 Parse Property Address
    if 'Property Address' in row and pd.notna(row['Property Address']):
        address_parts = row['Property Address'].split(',')
        # 2.2.2.1 Handle single-cell address format
        if len(address_parts) >= 3:
            payload['address'] = address_parts[0].strip()
            payload['city'] = address_parts[1].strip()
            state_zip = address_parts[2].strip().split()
            if len(state_zip) >= 2:
                payload['state'] = state_zip[0]
                payload['zip'] = state_zip[1]
        # 2.2.2.2 Fallback for unexpected formats
        else:
            payload['address'] = row['Property Address']

    # 2.2.3 Parse Mailing Address
    if 'Mailing Address' in row and pd.notna(row['Mailing Address']):
        mail_parts = row['Mailing Address'].split(',')
        # 2.2.3.1 Handle single-cell address format
        if len(mail_parts) >= 3:
            payload['mail_address'] = mail_parts[0].strip()
            payload['mail_city'] = mail_parts[1].strip()
            mail_state_zip = mail_parts[2].strip().split()
            if len(mail_state_zip) >= 2:
                payload['mail_state'] = mail_state_zip[0]
                payload['mail_zip'] = mail_state_zip[1]
        # 2.2.3.2 Fallback for unexpected formats
        else:
            payload['mail_address'] = row['Mailing Address']

    # 2.2.4 Handle separate address columns if present
    if all(col in row.index for col in ['Property Street', 'Property City', 'Property State', 'Property Zip']):
        payload['address'] = row['Property Street']
        payload['city'] = row['Property City']
        payload['state'] = row['Property State']
        payload['zip'] = str(row['Property Zip'])

    if all(col in row.index for col in ['Mailing Street', 'Mailing City', 'Mailing State', 'Mailing Zip']):
        payload['mail_address'] = row['Mailing Street']
        payload['mail_city'] = row['Mailing City']
        payload['mail_state'] = row['Mailing State']
        payload['mail_zip'] = str(row['Mailing Zip'])

    return payload

# 2.3 Process a single record
def process_record(record: Dict[str, Any], api_key: str) -> Dict[str, Any]:
    """Process a single record using the SkipTrace API."""
    headers = {
        "Accept": 'application/json',
        "Content-Type": 'application/json',
        "x-api-key": api_key
    }

    logger.info(f"Sending data to API: {json.dumps(record, indent=2)}")

    try:
        response = requests.post(API_URL, headers=headers, json=record, timeout=30)
        if response.status_code == 429:  # Too Many Requests
            logger.warning("Rate limit reached. Waiting before retrying...")
            time.sleep(RETRY_DELAY)
            return None  # Indicate need for retry
        
        json_response = response.json()
        
        if response.status_code == 404:
            logger.warning("Unable to locate valid property from address(es) provided.")
            json_response["is_hit"] = False
        elif response.status_code == 200:
            json_response["is_hit"] = json_response.get("match", False)
        else:
            response.raise_for_status()
        
        return json_response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            logger.error(f"Response content: {e.response.text}")
        return None

# 2.4 Save result to file
def save_result(result: Dict[str, Any], street_address: str) -> None:
    """Save a single result to a CSV file in both Google Drive and Colab environment."""
    filename = f"REapi_Skip_{street_address.replace(' ', '_')}.csv"
    drive_path = os.path.join(OUTPUT_FOLDER, filename)
    colab_path = os.path.join(COLAB_OUTPUT_FOLDER, filename)

    df = pd.DataFrame([flatten_dict(result)])
    for path in [drive_path, colab_path]:
        df.to_csv(path, index=False)
        logger.info(f"Saved result to {path}")

# 2.5 Validate input data
def validate_input_data(df: pd.DataFrame) -> bool:
    """Validate that the input data is in the correct format."""
    # 2.5.1 Check for required name columns
    required_columns = ['Owner 1 First Name', 'Owner 1 Last Name']
    for column in required_columns:
        if column not in df.columns:
            logger.error(f"Missing required column: {column}")
            return False
    
    # 2.5.2 Check for address columns
    address_columns = ['Property Address', 'Mailing Address']
    separate_address_columns = [
        ['Property Street', 'Property City', 'Property State', 'Property Zip'],
        ['Mailing Street', 'Mailing City', 'Mailing State', 'Mailing Zip']
    ]

    # 2.5.3 Check if at least one address format is present
    has_combined_address = any(col in df.columns for col in address_columns)
    has_separate_address = any(all(col in df.columns for col in column_set) for column_set in separate_address_columns)

    if not (has_combined_address or has_separate_address):
        logger.error("Missing required address columns. Need either combined or separate address components.")
        return False

    # 2.5.4 Log the detected address format
    if has_combined_address:
        logger.info("Detected combined address format.")
    if has_separate_address:
        logger.info("Detected separate address component format.")

    return True

# 2.6 Print Summary
def print_summary(df: pd.DataFrame, results: List[Dict[str, Any]]) -> None:
    """Print a summary of the processing results."""
    total_properties = len(df)
    processed_properties = len(results)
    successful_hits = sum(1 for result in results if result.get("is_hit", False))

    logger.info("Processing Summary:")
    logger.info(f"Total properties to be processed: {total_properties}")
    logger.info(f"Properties processed: {processed_properties}")
    logger.info(f"Properties with successful hit: {successful_hits}")

# 2.7 Print Progress
def print_progress(processed: int, total: int, hits: int) -> None:
    """Print progress of the processing."""
    percentage = (processed / total) * 100
    logger.info(f"{total} Records | {processed} Records Processed | HITs {hits} out of {processed}")
    logger.info(f"{percentage:.2f}% Complete")

    # 2.8 Flatten dictionary
def flatten_dict(d, parent_key='', sep='.'):
    items = []
    for k, v in d.items():
        new_key = f"{parent_key}{sep}{k}" if parent_key else k
        if isinstance(v, dict):
            items.extend(flatten_dict(v, new_key, sep=sep).items())
        elif isinstance(v, list):
            for i, item in enumerate(v):
                if isinstance(item, dict):
                    items.extend(flatten_dict(item, f"{new_key}{sep}{i}", sep=sep).items())
                else:
                    items.append((f"{new_key}{sep}{i}", item))
        else:
            items.append((new_key, v))
    return dict(items)

# 2.9 Parse address string
def parse_address(address_string: str) -> Dict[str, str]:
    """
    Parse an address string into components.
    
    2.9.1 Input: Full address string
    2.9.2 Output: Dictionary with address components
    """
    components = {'street': '', 'city': '', 'state': '', 'zip': ''}
    
    # 2.9.3 Split the address by comma
    parts = [part.strip() for part in address_string.split(',')]
    
    # 2.9.4 Handle different address formats
    if len(parts) >= 3:
        # 2.9.4.1 Format: Street, City, State ZIP
        components['street'] = parts[0]
        components['city'] = parts[1]
        state_zip = parts[2].split()
        if len(state_zip) >= 2:
            components['state'] = state_zip[0]
            components['zip'] = state_zip[1]
    elif len(parts) == 2:
        # 2.9.4.2 Format: Street, City State ZIP
        components['street'] = parts[0]
        city_state_zip = parts[1].split()
        if len(city_state_zip) >= 3:
            components['city'] = ' '.join(city_state_zip[:-2])
            components['state'] = city_state_zip[-2]
            components['zip'] = city_state_zip[-1]
    else:
        # 2.9.4.3 Unable to parse, return the full string as street address
        components['street'] = address_string
    
    return components

# 3. Main Execution
def main() -> None:
    try:
        # 3.1 Authenticate and get API key
        API_KEY = userdata.get('x-api-key')
        if API_KEY is None:
            raise ValueError("API key not found in Colab secrets. Please add it with the key 'x-api-key'.")

        # 3.2 Mount Google Drive
        drive.mount('/content/drive')

        # 3.3 Create output folders if they don't exist
        os.makedirs(OUTPUT_FOLDER, exist_ok=True)
        os.makedirs(COLAB_OUTPUT_FOLDER, exist_ok=True)

        # 3.4 Select input file
        selected_file = select_file()
        if not selected_file:
            logger.error("No file selected. Exiting.")
            return

           # 3.5 Load and validate the spreadsheet
        logger.info(f"Loading data from {selected_file}")
        if selected_file.endswith(('.xlsx', '.xls')):
            df = pd.read_excel(selected_file)
        elif selected_file.endswith('.csv'):
            df = pd.read_csv(selected_file)
        elif selected_file.endswith('.json'):
            df = pd.read_json(selected_file)
        elif selected_file.endswith('.txt'):
            df = pd.read_csv(selected_file, sep='\t')
        else:
            logger.error(f"Unsupported file format: {selected_file}")
            return

        # 3.5.1 Debugging: Print DataFrame info
        logger.info("\nDataFrame Info:")
        logger.info(df.info())

        # 3.5.2 Debugging: Display the first 5 rows
        logger.info("\nFirst 5 rows of the DataFrame:")
        logger.info(df.head().to_string())

        # 3.5.3 Debugging: Check for null values
        logger.info("\nNull value counts:")
        logger.info(df.isnull().sum())

        # 3.5.4 Debugging: Verify exact column names
        logger.info("\nColumn names:")
        logger.info(df.columns.tolist())

        # 3.5.5 Debugging: Display data types of columns
        logger.info("\nColumn data types:")
        logger.info(df.dtypes)

        if not validate_input_data(df):
            logger.error("Input data validation failed. Stopping script.")
            return

        logger.info(f"Loaded and validated {len(df)} records from the spreadsheet.")

        # 3.6 Add result columns to the dataframe
        df['API_Sent'] = False
        df['API_Response'] = ''
        df['API_Hit'] = False

        # 3.7 Process records
        results: List[Dict[str, Any]] = []
        retry_queue: List[Dict[str, Any]] = []
        total_records = len(df)
        processed_records = 0
        hits = 0
        for index, row in tqdm(df.iterrows(), total=total_records, desc="Processing records"):
            logger.info(f"Processing Record {index + 1}: ")
            skip_trace_input = prepare_skip_trace_input(row)
            df.at[index, 'API_Sent'] = True
            result = process_record(skip_trace_input, API_KEY)
            if result is None:
                retry_queue.append((index, skip_trace_input))
                df.at[index, 'API_Response'] = 'Error'
                logger.info(f"Record {index + 1}: ❌ Error")
            elif result:
                results.append(result)
                df.at[index, 'API_Response'] = 'Success'
                df.at[index, 'API_Hit'] = result.get("is_hit", False)
                save_result(result, row['Property Address'].split(',')[0].strip())
                logger.info(f"Record {index + 1}: ✅ Success {'🎯' if result.get('is_hit', False) else ''}")
                if result.get("is_hit", False):
                    hits += 1
            processed_records += 1
            print_progress(processed_records, total_records, hits)
            time.sleep(REQUEST_DELAY)


        # 3.8 Process retry queue
        logger.info(f"Processing {len(retry_queue)} records in retry queue...")
        for index, record in tqdm(retry_queue, desc="Retrying records"):
            result = process_record(record, API_KEY)
            if result:
                results.append(result)
                df.at[index, 'API_Response'] = 'Success'
                df.at[index, 'API_Hit'] = 'identity' in result
                save_result(result, df.at[index, 'Property Address'].split(',')[0].strip())
                logger.info(f"Retry Record {index + 1}: ✅ Success {'🎯' if 'identity' in result else ''}")
            else:
                logger.info(f"Retry Record {index + 1}: ❌ Failed")
            time.sleep(REQUEST_DELAY)

        # 3.9 Save final results
        current_datetime = datetime.now().strftime("%m%d%y_%H%M%S")
        final_output = os.path.join(OUTPUT_FOLDER, f'REapi_Skip_Results_{current_datetime}.csv')
        colab_final_output = os.path.join(COLAB_OUTPUT_FOLDER, f'REapi_Skip_Results_{current_datetime}.csv')
        df_results = pd.DataFrame([flatten_dict(result) for result in results])
        for path in [final_output, colab_final_output]:
            df_results.to_csv(path, index=False)
            logger.info(f"All results saved to {path}")

        # 3.10 Save and download summary results
        summary_filename = f"{os.path.splitext(os.path.basename(selected_file))[0]}_Skip_Results_Summary_{current_datetime}.csv"
        summary_path = os.path.join(OUTPUT_FOLDER, summary_filename)
        colab_summary_path = os.path.join(COLAB_OUTPUT_FOLDER, summary_filename)
        for path in [summary_path, colab_summary_path]:
            df.to_csv(path, index=False)
            logger.info(f"Summary results saved to {path}")

        # 3.11 Prompt user to download files
        files.download(colab_final_output)
        files.download(colab_summary_path)

        # 3.12 Print summary
        print_summary(df, results)

        logger.info(f"Processed {len(results)} records successfully.")
        logger.info(f"Failed to process {len(retry_queue) - (len(results) - len(df))} records after retry.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()