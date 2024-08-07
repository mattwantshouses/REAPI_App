# Script Name: Single Record SkipTrace Processing for Colab with Enhanced Features
# Version: 5.2 (Corrected and Linted)

# 1. Imports and Configuration
# 1.1 Import required libraries
!pip install fuzzywuzzy
!pip install python-Levenshtein
import os
import json
import time
import logging
from typing import Dict, Any, List, Optional
import pandas as pd
import requests
from tqdm import tqdm
from google.colab import auth, drive, files, userdata
from datetime import datetime
from fuzzywuzzy import process

# 1.2 Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# 1.3 Set up constants
API_URL = 'https://api.realestateapi.com/v1/SkipTrace'
OUTPUT_FOLDER = '/content/drive/MyDrive/B - RMD Home Buyers/RMD Marketing/RMD Marketing Lists/2024/Pre-Foreclosure Project May 2024/PFC Project - 2 - Skip Traced Files/PFC Project - 2.5 - Single Skip Traced Files'
COLAB_OUTPUT_FOLDER = '/content/skip_trace_results'
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
def prepare_skip_trace_input(row: pd.Series, column_mapping: Dict[str, str]) -> Dict[str, Any]:
    """Prepare a single row for the SkipTrace API input using flexible column mapping."""
    return {
        "first_name": row[column_mapping.get('Owner 1 First Name', 'Owner 1 First Name')],
        "last_name": row[column_mapping.get('Owner 1 Last Name', 'Owner 1 Last Name')],
        "address": row[column_mapping.get('Property Address', 'Property Address')],
        "city": row[column_mapping.get('Property City', 'Property City')],
        "state": row[column_mapping.get('Property State', 'Property State')],
        "zip": row[column_mapping.get('Property Zip', 'Property Zip')],
        "mail_address": row[column_mapping.get('Mailing Address', 'Mailing Address')],
        "mail_city": row[column_mapping.get('Mailing City', 'Mailing City')],
        "mail_state": row[column_mapping.get('Mailing State', 'Mailing State')],
        "mail_zip": row[column_mapping.get('Mailing Zip', 'Mailing Zip')]
    }

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
        log_api_request(record, response.json())  # Log the API request and response
        if response.status_code == 429:  # Too Many Requests
            logger.warning("Rate limit reached. Waiting before retrying...")
            time.sleep(RETRY_DELAY)
            return None  # Indicate need for retry
        response.raise_for_status()
        json_response = response.json()
        # Check if the response was successful
        json_response["is_hit"] = json_response.get("responseMessage") == "Successful"
        return json_response
    except requests.exceptions.RequestException as e:
        logger.error(f"API request failed: {str(e)}")
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
    required_columns = [
        'Owner 1 First Name', 'Owner 1 Last Name', 'Property Address', 'Property City', 
        'Property State', 'Property Zip', 'Mailing Address', 'Mailing City', 'Mailing State', 'Mailing Zip'
    ]
    for column in required_columns:
        if column not in df.columns:
            logger.error(f"Missing required column: {column}")
            return False
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

# 2.9 Map column names
def map_column_names(df):
    """
    Create a mapping between expected column names and actual column names in the dataframe.
    Uses fuzzy matching to handle slight variations in column names.
    """
    expected_columns = {
        'Owner 1 First Name': ['first name', 'firstname', 'fname'],
        'Owner 1 Last Name': ['last name', 'lastname', 'lname'],
        'Property Address': ['property street', 'property address', 'address'],
        'Property City': ['property city', 'city'],
        'Property State': ['property state', 'state'],
        'Property Zip': ['property zip', 'zip'],
        'Mailing Address': ['mailing street', 'mailing address'],
        'Mailing City': ['mailing city'],
        'Mailing State': ['mailing state'],
        'Mailing Zip': ['mailing zip']
    }
    
    column_mapping = {}
    for expected, alternatives in expected_columns.items():
        best_match = process.extractOne(expected, df.columns, score_cutoff=80)
        if best_match:
            column_mapping[expected] = best_match[0]
    
    return column_mapping

# 2.10 Log API requests
def log_api_request(request, response):
    """Log API requests and responses to a CSV file."""
    with open('api_calls_log.csv', 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerow([datetime.now(), json.dumps(request), json.dumps(response)])

# 2.11 Load and prepare data
def load_and_prepare_data(selected_file):
    """Load data from file and prepare it for processing."""
    logger.info(f"Loading data from {selected_file}")
    if selected_file.endswith(('.xlsx', '.xls')):
        df = pd.read_excel(selected_file, dtype={'Property Zip': str, 'Mailing Zip': str})
    elif selected_file.endswith('.csv'):
        df = pd.read_csv(selected_file, dtype={'Property Zip': str, 'Mailing Zip': str})
    elif selected_file.endswith('.json'):
        df = pd.read_json(selected_file)
        df['Property Zip'] = df['Property Zip'].astype(str)
        df['Mailing Zip'] = df['Mailing Zip'].astype(str)
    elif selected_file.endswith('.txt'):
        df = pd.read_csv(selected_file, sep='\t', dtype={'Property Zip': str, 'Mailing Zip': str})
    else:
        logger.error(f"Unsupported file format: {selected_file}")
        return None
    
    # Debugging information
    logger.info("\nDataFrame Info:")
    logger.info(df.info())
    logger.info("\nFirst 5 rows of the DataFrame:")
    logger.info(df.head().to_string())
    logger.info("\nNull value counts:")
    logger.info(df.isnull().sum())
    logger.info("\nColumn names:")
    logger.info(df.columns.tolist())
    logger.info("\nColumn data types:")
    logger.info(df.dtypes)

    return df

# 2.12 Validate and map columns
def validate_and_map_columns(df):
    """Validate input data and create column mapping."""
    column_mapping = map_column_names(df)
    logger.info("Column mapping created:")
    logger.info(json.dumps(column_mapping, indent=2))

    required_columns = [
        'Owner 1 First Name', 'Owner 1 Last Name', 'Property Address', 'Property City', 
        'Property State', 'Property Zip', 'Mailing Address', 'Mailing City', 'Mailing State', 'Mailing Zip'
    ]
    
    for column in required_columns:
        if column not in column_mapping:
            logger.error(f"Missing required column: {column}")
            return False, None
    
    return True, column_mapping

# 2.13 Process records
def process_records(df, column_mapping, API_KEY):
    """Process records using the SkipTrace API."""
    results = []
    retry_queue = []
    total_records = len(df)
    processed_records = 0
    hits = 0

    # Add result columns to the dataframe
    df['API_Sent'] = False
    df['API_Response'] = ''
    df['API_Hit'] = False

    for index, row in tqdm(df.iterrows(), total=total_records, desc="Processing records"):
        logger.info(f"Processing Record {index + 1}: ")
        skip_trace_input = prepare_skip_trace_input(row, column_mapping)
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
            save_result(result, row[column_mapping['Property Address']].split(',')[0].strip())
            logger.info(f"Record {index + 1}: ✅ Success {'🎯' if result.get('is_hit', False) else ''}")
            if result.get("is_hit", False):
                hits += 1
        
        processed_records += 1
        if processed_records % 10 == 0:  # Print progress every 10 records
            print_progress(processed_records, total_records, hits)
        time.sleep(REQUEST_DELAY)

    return results, retry_queue, df

# 2.14 Save final results
def save_final_results(results, output_folder, colab_output_folder):
    current_datetime = datetime.now().strftime("%m%d%y_%H%M%S")
    final_output = os.path.join(output_folder, f'REapi_Skip_Results_{current_datetime}.csv')
    colab_final_output = os.path.join(colab_output_folder, f'REapi_Skip_Results_{current_datetime}.csv')
    df_results = pd.DataFrame([flatten_dict(result) for result in results])
    for path in [final_output, colab_final_output]:
        df_results.to_csv(path, index=False)
        logger.info(f"All results saved to {path}")
    return colab_final_output

# 2.15 Save summary results
def save_summary_results(df, selected_file, output_folder, colab_output_folder):
    current_datetime = datetime.now().strftime("%m%d%y_%H%M%S")
    summary_filename = f"{os.path.splitext(os.path.basename(selected_file))[0]}_Skip_Results_Summary_{current_datetime}.csv"
    summary_path = os.path.join(output_folder, summary_filename)
    colab_summary_path = os.path.join(colab_output_folder, summary_filename)
    for path in [summary_path, colab_summary_path]:
        df.to_csv(path, index=False)
        logger.info(f"Summary results saved to {path}")
    return colab_summary_path

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
        df = load_and_prepare_data(selected_file)
        if df is None:
            return

        # 3.6 Validate input data and create column mapping
        is_valid, column_mapping = validate_and_map_columns(df)
        if not is_valid:
            return

        # 3.7 Process records
        results, retry_queue, df = process_records(df, column_mapping, API_KEY)

        # 3.8 Process retry queue
        logger.info(f"Processing {len(retry_queue)} records in retry queue...")
        for index, record in tqdm(retry_queue, desc="Retrying records"):
            result = process_record(record, API_KEY)
            if result:
                results.append(result)
                df.at[index, 'API_Response'] = 'Success'
                df.at[index, 'API_Hit'] = result.get("is_hit", False)
                save_result(result, df.at[index, column_mapping['Property Address']].split(',')[0].strip())
                logger.info(f"Retry Record {index + 1}: ✅ Success {'🎯' if result.get('is_hit', False) else ''}")
            else:
                logger.info(f"Retry Record {index + 1}: ❌ Failed")
            time.sleep(REQUEST_DELAY)

        # 3.9 Save final results
        colab_final_output = save_final_results(results, OUTPUT_FOLDER, COLAB_OUTPUT_FOLDER)

        # 3.10 Save and download summary results
        colab_summary_path = save_summary_results(df, selected_file, OUTPUT_FOLDER, COLAB_OUTPUT_FOLDER)

        # 3.11 Prompt user to download files
        files.download(colab_final_output)
        files.download(colab_summary_path)

        # 3.12 Print summary
        print_summary(df, results)

        logger.info(f"Processed {len(results)} records successfully.")
        logger.info(f"Failed to process {len(retry_queue) - (len(results) - len(df))} records after retry.")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")

if __name__ == "__main__":
    main()