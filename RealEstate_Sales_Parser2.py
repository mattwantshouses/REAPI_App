# RealEstate_Sales_Parser
# Version 2.0 
# Merged 1.5 General Parsing with 1.25 LO Parsing

import pandas as pd
import re
import os
from google.colab import files
from google.colab import drive
from datetime import datetime
import pytz
import gspread
from google.auth import default
from gspread_dataframe import set_with_dataframe
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# 1 - Constants
GOOGLE_DRIVE_FOLDER_ID = "1dq-zCFoHpl30_f7BRquB_q5wOJnQsxWL"
GOOGLE_SHEET_ID = "141HNY6bJmz6FKodd3-qCChtyZfmbq9bRqF_9GZ5oqgc"  # Replace with your Google Sheet ID
GOOGLE_SHEET_NAME = "Master tab"  # Replace with your worksheet name if different

# 2 - Google Drive Functions
def mount_drive():
    """
    2.1 - Mounts Google Drive to the Colab environment.
    """
    try:
        drive.mount('/content/drive')
        logging.info("Google Drive mounted successfully.")
    except Exception as e:
        logging.error(f"An error occurred while mounting Google Drive: {e}")

# 3 - Google Sheets Authentication
def authenticate_gsheets():
    """
    3.1 - Authenticates and returns a gspread client.
    """
    try:
        from google.colab import auth  # Ensure auth is imported here
        auth.authenticate_user()
        creds, _ = default()
        client = gspread.authorize(creds)
        logging.info("Google Sheets authenticated successfully.")
        return client
    except Exception as e:
        logging.error(f"An error occurred during Google Sheets authentication: {e}")
        return None

# 4 - File Listing and Selection
def list_files_in_colab():
    """
    4.1 - List relevant files in the current Colab directory and provide an option to upload a new file.
    Returns:
        list: A list of relevant filenames.
    """
    try:
        all_files = os.listdir('.')
        # 4.2 - Get all files in the current directory
        # 4.3 - Filter out system files and directories
        relevant_files = [f for f in all_files if not f.startswith('.') and not os.path.isdir(f)]
        # 4.4 - Print relevant files
        print("Relevant files available in the Colab environment:")
        for i, file_name in enumerate(relevant_files):
            print(f"{i + 1}. {file_name}")
        # 4.5 - Add upload option
        print(f"{len(relevant_files) + 1}. Upload a new file")
        logging.info("Listed relevant files in Colab environment.")
        return relevant_files
    except Exception as e:
        logging.error(f"An error occurred while listing files: {e}")
        return []

# 5 - User File Selection
def get_user_file_selection(file_list):
    """
    5.1 - Prompt the user to select a file from the list or upload a new one.
    Args:
        file_list (list): List of available filenames.
    Returns:
        str: Path to the selected or uploaded file.
    """
    while True:
        try:
            selected_option = int(input("Enter the number corresponding to the file you want to use: "))
            if 1 <= selected_option <= len(file_list):
                selected_file = file_list[selected_option - 1]
                logging.info(f"Selected file: {selected_file}")
                return selected_file
            elif selected_option == len(file_list) + 1:
                uploaded = files.upload()
                if uploaded:
                    uploaded_file = list(uploaded.keys())[0]
                    logging.info(f"Uploaded file: {uploaded_file}")
                    return uploaded_file
                else:
                    logging.warning("No file uploaded. Please try again.")
            else:
                print(f"Please enter a number between 1 and {len(file_list) + 1}.")
        except ValueError:
            print("Invalid input. Please enter a valid number.")
        except Exception as e:
            logging.error(f"An unexpected error occurred during file selection: {e}")

# 6 - Read File
def read_file(file_path):
    """
    6.1 - Reads the content of a file.
    Args:
        file_path (str): Path to the file.
    Returns:
        str: Content of the file.
    """
    try:
        with open(file_path, 'r', encoding='utf-8') as file:
            data = file.read()
        logging.info(f"File {file_path} read successfully.")
        return data
    except Exception as e:
        logging.error(f"An error occurred while reading the file: {e}")
        return ""

# 7 - Data Parsing
def parse_data(data):
    """
    7.1 - Parses the raw data using predefined regex patterns and returns a DataFrame.
    7.2 - Handles multiple records separated by 'MLS#'.
    Returns:
        pd.DataFrame: Parsed data.
    """
    try:
        # 7.3 - Define regex patterns for various fields with non-greedy matching and proper delimiters
        field_patterns = {
            'MLS#': r'MLS#\s*(\d+)',
            'DOM/CDOM': r'DOM/CDOM[:\t]+\s*([\d/]+)',
            'Address': r'DOM/CDOM[:\t]+\s*[\d/]+\s*([^\t\n]+)',
            'County': r'County[:\t]+\s*([^\t\n]+)',
            'List Price': r'List Price[:\t]+\$?([\d,]+)',
            'Close Price': r'Close Price\s*:\s*\$?([\d,]+)',  # Updated regex for robustness
            'Year Built': r'Year Built[:\t]+\s*(\d+)',
            'Living Area': r'Living Area[:\t]+\s*([\d,]+)',
            'Bedrooms Total': r'Bedrooms Total[:\t]+\s*(\d+)',
            'Bathrooms Total': r'Bathrooms Total[:\t]+\s*(\d+)',
            'Garage Spaces': r'Garage Spaces[:\t]+\s*(\d+)',
            'Parcel Number': r'Parcel Number[:\t]+\s*(\d+)',
            'Subdivision Name': r'Subdivision Name[:\t]+\s*([^\t\n]+)',
            'CDD Fee': r'CDD Fee[:\t]+\s*(Yes|No)',
            'New Construction': r'New Construction[:\t]+\s*(Yes|No)',
            'Waterfront': r'Waterfront[:\t]+\s*(Yes|No)',
            'Directions': r'Directions[:\t]+\s*([^\t\n]+)',
            'Public Remarks': r'Public Remarks[:\t]+\s*([\s\S]*?)\s*\nPrivate Remarks:',
            'Private Remarks': r'Private Remarks[:\t]+\s*([\s\S]*?)(?=\nAppliances:|$)',
            'Appliances': r'Appliances[:\t]+\s*([\w\s;,\-]+)',
            'Approx Parcel Size': r'Approx Parcel Size[:\t]+\s*([^\t\n]+)',
            'Architectural Style': r'Architectural Style[:\t]+\s*([^\t\n]+)',
            'Construction Materials': r'Construction Materials[:\t]+\s*([^\t\n]+)',
            'Cooling': r'Cooling[:\t]+\s*([^\t\n]+)',
            'Current Use': r'Current Use[:\t]+\s*([^\t\n]+)',
            'DPR Eligible': r'DPR Eligible[:\t]+\s*([^\t\n]*)',
            'Fencing': r'Fencing[:\t]+\s*([^\t\n]+)',
            'Fireplace Features': r'Fireplace Features[:\t]+\s*([^\t\n]+)',
            'Heating': r'Heating[:\t]+\s*([^\t\n]+)',
            'Interior Features': r'Interior Features[:\t]+\s*([^\t\n]+)',
            'Laundry Features': r'Laundry Features[:\t]+\s*([^\t\n]+)',
            'Listing Terms': r'Listing Terms[:\t]+\s*([^\t\n]+)',
            'Lot Features': r'Lot Features[:\t]+\s*([^\t\n]+)',
            'Parking Features': r'Parking Features[:\t]+\s*([^\t\n]+)',
            'Patio And Porch Features': r'Patio And Porch Features[:\t]+\s*([^\t\n]+)',
            'Pool Features': r'Pool Features[:\t]+\s*([^\t\n]+)',
            'Possession': r'Possession[:\t]+\s*([^\t\n]+)',
            'Road Surface Type': r'Road Surface Type[:\t]+\s*([^\t\n]+)',
            'Roof': r'Roof[:\t]+\s*([^\t\n]+)',
            'Security Features': r'Security Features[:\t]+\s*([^\t\n]+)',
            'Sewer': r'Sewer[:\t]+\s*([^\t\n]+)',
            'Special Listing Conditions': r'Special Listing Conditions[:\t]+\s*([^\t\n]+)',
            'Utilities': r'Utilities[:\t]+\s*([^\t\n]+)',
            'Water Source': r'Water Source[:\t]+\s*([^\t\n]+)',
            'Showing Requirements': r'Showing Requirements[:\t]+\s*([^\t\n]+)',
            'Showing Considerations': r'Showing Considerations[:\t]+\s*([^\t\n]+)',
            'Listing Contract Date': r'Listing Contract Date[:\t]+\s*([\d/]+)',
            'Purchase Contract Date': r'Purchase Contract Date[:\t]+\s*([\d/]+)',
            'Close Date': r'Close Date[:\t]+\s*([\d/]+)',
            'Listing Service': r'Listing Service[:\t]+\s*([^\t\n]+)',
            'Original List Price': r'Original List Price[:\t]+\$?([\d,]+)',
            'List Price/SqFt': r'List Price/SqFt[:\t]+\$?([\d\.]+)',
            'Sold Price/SqFt': r'Sold Price/SqFt[:\t]+\$?([\d\.]+)',
            'Listing Agreement': r'Listing Agreement[:\t]+\s*([^\t\n]+)',
            'Contingency Reason': r'Contingency Reason[:\t]+\s*([^\t\n]+)',
            'Buyer Financing': r'Buyer Financing[:\t]+\s*([^\t\n]+)',
            'Concessions': r'Concessions[:\t]+\s*(Yes|No)',
            'BuyersCountryReside': r'BuyersCountryReside[:\t]+\s*([^\t\n]+)',
            'SellersCountryReside': r'SellersCountryReside[:\t]+\s*([^\t\n]+)'
        }

        # 7.4 - Define the parse_agent_line function
        def parse_agent_line(designation, record):
            """
            Parses agent lines like LO, LA, CO-LA, SO, SA, CO-SA.

            Args:
                designation (str): The designation to look for (e.g., 'LO', 'LA').
                record (str): The text of the record.

            Returns:
                tuple: (name, contacts_dict)
            """
            pattern = r'^' + re.escape(designation) + r':\s*(.*)$'
            matches = re.findall(pattern, record, re.MULTILINE)
            if matches:
                # Concatenate all matches in case of multiple lines
                line = ' '.join(matches).strip()
                # The name is before the first '(' or till the end if no '('
                name_part = re.split(r'\s*\(', line, 1)[0].strip()
                contacts_part = line[len(name_part):].strip()
                # Now extract contacts, handling optional colons inside or outside parentheses
                contacts = re.findall(r'\(([^):]+):?\)\s*:?\s*([^()]+)?', contacts_part)
                contacts_dict = {contact_type.strip(): (contact_info.strip() if contact_info else '') for contact_type, contact_info in contacts}
                return name_part, contacts_dict
            return '', {}

        # 7.5 - Split records by 'MLS#', keeping 'MLS#' with the split records
        records = re.split(r'(MLS#\s*\d+)', data)
        # 7.5.1 - The first element is before the first 'MLS#', likely irrelevant, remove it
        if records and not re.search(r'MLS#\s*\d+', records[0]):
            records = records[1:]
        # 7.5.2 - Now, pair 'MLS#' with the corresponding record content
        paired_records = []
        for i in range(0, len(records), 2):
            mls = records[i].strip()
            content = records[i+1].strip() if i+1 < len(records) else ''
            if mls and content:
                paired_records.append(mls + "\n" + content)

        parsed_records = []

        # 7.6 - Parse each record
        for record in paired_records:
            record_data = {}
            # 7.6.1 - Extract fields
            for field_name, pattern in field_patterns.items():
                match = re.search(pattern, record, re.DOTALL)
                if match:
                    # ... [existing code for field extraction] ...
                    record_data[field_name] = match.group(1).strip()
                else:
                    record_data[field_name] = ''

            # 7.6.4 - Extract agent details using parse_agent_line
            try:
                designations = ['LO', 'LA', 'CO-LA', 'SO', 'SA', 'CO-SA']
                contact_types = ['Phone', 'Mobile', 'Office', 'Email', 'Fax']
                for desig in designations:
                    name_key = desig + ' Name'
                    prefix = desig + ' '
                    name, contacts_dict = parse_agent_line(desig, record)
                    record_data[name_key] = str(name)
                    for contact_type in contact_types:
                        key = prefix + contact_type
                        record_data[key] = str(contacts_dict.get(contact_type, ''))
            except Exception as e:
                logging.error(f"Error extracting agent details: {e}")
                continue  # Skip to the next record in case of error

            parsed_records.append(record_data)

        # 7.7 - Create DataFrame from parsed records
        df = pd.DataFrame(parsed_records)

        # 7.7.1 - Add "Created on" column with current date formatted as mm/dd/yy
        est = pytz.timezone('America/New_York')
        current_date = datetime.now(est).strftime("%m/%d/%y")
        df.insert(0, "Created on", current_date)

        # 7.7.2 - Add "Format" column after "Created on"
        df.insert(1, "Format", "Standard")  # You can customize the value as required

        # 7.9 - Diagnostic Logging: Print DataFrame Columns and Sample Data
        logging.info(f"DataFrame Columns: {df.columns.tolist()}")
        logging.info(f"Sample Data:\n{df.head()}")
        print("\n--- Parsed DataFrame Preview ---")
        print(df.head())
        print("---------------------------------\n")

        logging.info("Data parsed successfully into DataFrame.")
        return df

    except Exception as e:
        logging.error(f"An error occurred during data parsing: {e}")
        return pd.DataFrame()



# 7.1 - Ensure Required Columns Exist
def ensure_columns_exist(worksheet, required_columns):
    """
    7.1 - Ensures that the specified columns exist in the worksheet.
    If they don't, adds them at the beginning.

    Args:
        worksheet (gspread.models.Worksheet): The worksheet to modify.
        required_columns (list): List of column names to ensure exist.
    """
    try:
        existing_headers = worksheet.row_values(1)
        logging.info(f"Existing headers: {existing_headers}")
        # Determine which required columns are missing
        missing_columns = [col for col in required_columns if col not in existing_headers]
        if missing_columns:
            logging.info(f"Missing columns detected: {missing_columns}")
            # Insert missing columns at the beginning
            for col in reversed(missing_columns):  # Reverse to maintain order when inserting
                worksheet.insert_cols([col], 1)
                logging.info(f"Inserted missing column: {col}")
        else:
            logging.info("All required columns are present.")
    except Exception as e:
        logging.error(f"An error occurred while ensuring columns exist: {e}")

# 8 - Append to Google Sheets
def append_to_google_sheet(df, spreadsheet_id, sheet_name):
    """
    8.1 - Appends a DataFrame to the specified Google Sheet without overwriting existing data.

    Args:
        df (pd.DataFrame): The DataFrame to append.
        spreadsheet_id (str): The ID of the Google Sheet.
        sheet_name (str): The name of the worksheet/tab.
    """
    if df.empty:
        logging.warning("DataFrame is empty. Nothing to append to Google Sheet.")
        return
    try:
        client = authenticate_gsheets()
        if client is None:
            logging.error("Google Sheets client not initialized.")
            return

        # Open the Google Sheet
        sheet = client.open_by_key(spreadsheet_id)
        worksheet = sheet.worksheet(sheet_name)
        logging.info(f"Opened Google Sheet: {spreadsheet_id}, Worksheet: {sheet_name}")

        # 7.1 - Ensure required columns exist
        required_columns = ["Created on", "Format"]
        ensure_columns_exist(worksheet, required_columns)

        # Fetch existing headers after ensuring columns
        existing_headers = worksheet.row_values(1)
        df_columns = df.columns.tolist()

        # Check if headers match
        if existing_headers != df_columns:
            logging.warning("The DataFrame columns do not match the Google Sheet headers.")
            # Reorder DataFrame columns to match the sheet
            df = df.reindex(columns=existing_headers)
            logging.info("Reordered DataFrame columns to match Google Sheet headers.")
            # Alternatively, you can choose to update the sheet headers or handle mismatches differently

        # Find the next empty row
        existing_data = worksheet.get_all_values()
        next_row = len(existing_data) + 1
        logging.info(f"Appending data starting at row {next_row}.")

        # Convert DataFrame to list of lists
        data = df.values.tolist()

        # Append all rows at once (batch insertion for efficiency)
        worksheet.insert_rows(data, row=next_row)
        logging.info("Data appended successfully to Google Sheet.")
    except Exception as e:
        logging.error(f"An error occurred while appending to Google Sheet: {e}")

# 9 - Save to Google Drive
def save_to_google_drive(df, folder_id, filename='parsed_sales_data.csv'):
    """
    9.1 - Save DataFrame to a specified folder in Google Drive using folder ID.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        folder_id (str): ID of the folder in Google Drive.
        filename (str): Base filename for the saved CSV.
    """
    try:
        from google.colab import auth
        from googleapiclient.discovery import build
        from googleapiclient.http import MediaFileUpload

        # Get current date and time in EST
        est = pytz.timezone('America/New_York')
        current_time = datetime.now(est)
        timestamp = current_time.strftime("%m%d%y-%H%M%S")

        # Create the new filename with formatted date and time
        base_name, extension = os.path.splitext(filename)
        new_filename = f"{base_name}_{timestamp}{extension}"

        # Authenticate and build the Drive service
        auth.authenticate_user()
        drive_service = build('drive', 'v3')

        file_metadata = {
            'name': new_filename,
            'parents': [folder_id]
        }

        # Save DataFrame to a temporary CSV file
        temp_csv_path = f'/tmp/{new_filename}'
        df.to_csv(temp_csv_path, index=False)
        logging.info(f"CSV file saved locally at {temp_csv_path}.")

        media = MediaFileUpload(temp_csv_path, resumable=True)
        file = drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()

        logging.info(f"CSV file saved successfully to Google Drive with ID: {file.get('id')}")
    except Exception as e:
        logging.error(f"An error occurred while saving the CSV file to Google Drive: {e}")

# 10 - Download Parsed Data
def download_parsed_data(df, filename='parsed_sales_data.csv'):
    """
    10.1 - Saves the DataFrame locally and initiates a download in Colab.
    Args:
        df (pd.DataFrame): The DataFrame to download.
        filename (str): The name of the file to save and download.
    """
    try:
        # Get current date and time in EST
        est = pytz.timezone('America/New_York')
        current_time = datetime.now(est)
        timestamp = current_time.strftime("%m%d%y-%H%M%S")

        # Create the new filename with formatted date and time
        base_name, extension = os.path.splitext(filename)
        new_filename = f"{base_name}_{timestamp}{extension}"

        # Save and download the file
        df.to_csv(new_filename, index=False)
        logging.info(f"File saved locally as {new_filename}. Initiating download...")
        files.download(new_filename)
    except Exception as e:
        logging.error(f"An error occurred during the download: {e}")

# 11 - Main Function
def main():
    """
    11.1 - Main function to orchestrate the parsing and saving/appending processes.
    """
    # 2.1 - Mount Google Drive
    mount_drive()

    # 4.1 - File listing and selection
    file_list = list_files_in_colab()
    file_path = get_user_file_selection(file_list)

    # 6.1 - Read and parse data
    data = read_file(file_path)
    if not data:
        logging.warning("No data to parse. Exiting.")
        return
    parsed_data = parse_data(data)

    if parsed_data.empty:
        logging.warning("Parsed data is empty. Nothing to save or append.")
        return

    # Display DataFrame Preview
    print("\n--- Full Parsed DataFrame ---")
    print(parsed_data)
    print("------------------------------\n")

    # 9.1 - Save to Google Drive as CSV
    save_to_google_drive(parsed_data, GOOGLE_DRIVE_FOLDER_ID)

    # 8.1 - Append to Google Sheet
    append_to_google_sheet(parsed_data, GOOGLE_SHEET_ID, GOOGLE_SHEET_NAME)

    # 10.1 - Download the parsed data
    download_parsed_data(parsed_data)

if __name__ == "__main__":
    main()