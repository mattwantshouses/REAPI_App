# Merged Property Scoring Algorithm
# Version 2.1

# 1. Imports
# 1.1 Standard library imports
import json
import logging
import os
from datetime import datetime
from typing import Dict, List, Tuple

# 1.2 Third-party imports
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from google.colab import files

# 2. Setup and Configuration
# 2.1 Logging Configuration
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("property_scoring.log"),
        logging.StreamHandler()
    ]
)

# 3. File Selection and Data Loading
# 3.1 File Selection Function
def select_file() -> str:
    """Allow the user to select a file from the current directory or upload a new one."""
    files_list = [f for f in os.listdir() if f.endswith(('.json', '.txt', '.csv'))]

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
                logging.error("No file was uploaded.")
                return ""
            print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

# 3.2 Data Loading Function
def load_data(file_path: str) -> pd.DataFrame:
    """Load data from the specified file path."""
    logging.info(f"Loading data from {file_path}")
    if file_path.endswith('.csv'):
        return pd.read_csv(file_path)
    if file_path.endswith('.json'):
        return pd.read_json(file_path)
    raise ValueError("Unsupported file format. Please use CSV or JSON.")

# 4. Data Preparation
def prepare_data(df: pd.DataFrame) -> pd.DataFrame:
    """Prepare the data by handling missing values and converting date fields."""
    logging.info("Preparing data")
    
    # 4.1 Handle missing values
    df = df.fillna(0)
    
    # 4.2 Convert date fields
    date_columns = ['lastSaleDate', 'lastUpdateDate']
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # 4.3 Create derived features
    df['days_since_last_sale'] = (
        (datetime.now() - df['lastSaleDate']).dt.days
        if 'lastSaleDate' in df.columns else 0
    )
    df['is_absentee_owner'] = df['absenteeOwner'].astype(int) if 'absenteeOwner' in df.columns else 0
    df['is_vacant'] = df['vacant'].astype(int) if 'vacant' in df.columns else 0
    
    return df

# 5. Scoring System
# 5.1 Individual Scoring Functions
def equity_score(equity_percent: float) -> float:
    """Calculate the equity score based on equity percentage."""
    return np.clip(equity_percent / 100, 0, 1)

def ownership_length_score(days: int) -> float:
    """Calculate the ownership length score."""
    return 1 - np.sin(np.clip(days / 3650, 0, np.pi/2))

def calculate_additional_flags_score(row: pd.Series) -> float:
    """Calculate the score based on additional flags."""
    flags = ['vacant', 'taxLien', 'quitClaim', 'sheriffsDeed', 'spousalDeath', 'trusteeSale']
    flag_count = sum(row.get(flag, False) for flag in flags)
    
    if flag_count >= 2:
        return 100
    if flag_count == 1:
        return 50
    return 0

def calculate_financial_distress_score(row: pd.Series) -> float:
    """Calculate the financial distress score."""
    factors = [
        row.get('taxLien', False),
        row.get('preForeclosure', False),
        row.get('quitClaim', False),
        row.get('sheriffsDeed', False),
        row.get('trusteeSale', False),
        row.get('foreclosureStatus', 'None') != 'None',
        row.get('daysInForeclosure', 0) > 0
    ]
    
    return (sum(factors) / len(factors)) * 100

def calculate_mls_score(row: pd.Series) -> float:
    """Calculate the MLS score."""
    days_on_market = row.get('daysOnMarket', 0)
    mls_total_updates = row.get('mlsTotalUpdates', 0)
    
    dom_score = max(0, 100 - days_on_market)
    update_score = min(100, mls_total_updates * 20)
    
    return (dom_score + update_score) / 2

def calculate_market_attractiveness_score(row: pd.Series) -> float:
    """Calculate the market attractiveness score."""
    return row.get('schoolsRating', 50)

def calculate_property_condition_score(row: pd.Series) -> float:
    """Calculate the property condition score."""
    year_built = row.get('yearBuilt', 1900)
    current_year = pd.Timestamp.now().year
    age_score = max(0, 100 - (current_year - year_built))
    
    assessed_value = row.get('assessedValue', 0)
    market_value = row.get('marketValue', 0)
    
    value_score = min(100, (assessed_value / market_value) * 100) if market_value > 0 else 50
    
    return (age_score + value_score) / 2

# 5.2 Main Scoring Function
def calculate_property_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate all property scores based on various factors."""
    logging.info("Calculating property scores")
    
    df['additional_flags_score'] = df.apply(calculate_additional_flags_score, axis=1)
    df['financial_distress_score'] = df.apply(calculate_financial_distress_score, axis=1)
    df['equity_score'] = df['equityPercent'].fillna(0).clip(0, 100)
    df['ownership_score'] = df['days_since_last_sale'].apply(ownership_length_score) * 100
    df['mls_score'] = df.apply(calculate_mls_score, axis=1)
    df['market_attractiveness_score'] = df.apply(calculate_market_attractiveness_score, axis=1)
    df['property_condition_score'] = df.apply(calculate_property_condition_score, axis=1)
    
    # Calculate total score
    weights = {
        'additional_flags': 0.20,
        'financial_distress': 0.25,
        'equity': 0.15,
        'ownership': 0.10,
        'mls': 0.10,
        'market_attractiveness': 0.10,
        'property_condition': 0.10
    }
    
    df['total_score'] = sum(
        weights[key] * df[f'{key}_score']
        for key in weights
    )
    
    # Normalize total score to 0-100 range
    df['total_score'] = df['total_score'].clip(0, 100)
    df['rank'] = df['total_score'].rank(method='dense', ascending=False)
    
    return df

# 6. Analysis
def perform_analysis(df: pd.DataFrame) -> Tuple[str, pd.DataFrame]:
    """Perform analysis on the scored data."""
    logging.info("Performing analysis")
    
    # 6.1 Rank properties
    df['rank'] = df['total_score'].rank(method='dense', ascending=False)

    # 6.2 Identify top prospects
    top_percent = 20
    top_threshold = df['total_score'].quantile(1 - top_percent/100)
    df['is_top_prospect'] = df['total_score'] >= top_threshold
    
    top_prospects = df[df['is_top_prospect']].sort_values('total_score', ascending=False)
    
    # 6.3 Generate report
    report = f"""
    Analysis Report:
    Total properties analyzed: {len(df)}
    Number of top prospects: {len(top_prospects)}
    Average score: {df['total_score'].mean():.2f}
    Median score: {df['total_score'].median():.2f}
    """
    
    return report, top_prospects

# 7. Visualization
def generate_visualizations(df: pd.DataFrame) -> None:
    """Generate visualizations based on the analyzed data."""
    logging.info("Generating visualizations")

    # 7.1 Score distribution histogram
    plt.figure(figsize=(10, 6))
    sns.histplot(df['total_score'], kde=True)
    plt.title('Distribution of Total Scores')
    plt.savefig('score_distribution.png')
    plt.close()

    # 7.2 Equity vs Ownership Length scatter plot
    plt.figure(figsize=(10, 6))
    sns.scatterplot(data=df, x='equityPercent', y='days_since_last_sale', hue='is_top_prospect')
    plt.title('Equity vs Ownership Length')
    plt.savefig('equity_vs_ownership.png')
    plt.close()

# 8. Output
def save_results(df: pd.DataFrame, report: str, base_filename: str) -> None:
    """Save the analysis results and processed data."""
    logging.info("Preparing output")

    # 8.1 Save report
    with open('analysis_report.txt', 'w') as f:
        f.write(report)

    # 8.2 Save results
    json_output_file = f"{base_filename}_analyzed.json"
    csv_output_file = f"{base_filename}_analyzed.csv"

    # Sort the DataFrame by 'rank' in ascending order
    df_sorted = df.sort_values('rank', ascending=True)

    # Save as JSON
    with open(json_output_file, 'w') as f:
        json.dump(json.loads(df_sorted.to_json(orient='records')), f, indent=2)

    # Save as CSV
    df_sorted.to_csv(csv_output_file, index=False)

    logging.info(f"Analysis complete. Results saved to {json_output_file} and {csv_output_file}")
    print(f"Please download the following files:")
    print(f"1. {json_output_file}")
    print(f"2. {csv_output_file}")
    print("3. score_distribution.png")
    print("4. equity_vs_ownership.png")
    print("5. analysis_report.txt")

    # Prompt user to download all files
    files.download(json_output_file)
    files.download(csv_output_file)
    files.download('score_distribution.png')
    files.download('equity_vs_ownership.png')
    files.download('analysis_report.txt')

# 9. Additional Utility Functions
def describe_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Generate a statistical description of the calculated scores."""
    score_columns = [
        'additional_flags_score', 'financial_distress_score', 'equity_score',
        'ownership_score', 'mls_score', 'market_attractiveness_score',
        'property_condition_score', 'total_score'
    ]
    return df[score_columns].describe()

def export_top_prospects(df: pd.DataFrame, output_file: str, top_n: int = 100) -> None:
    """Export the top N prospects to a separate file."""
    top_prospects = df.nsmallest(top_n, 'rank')
    top_prospects.to_csv(output_file, index=False)
    logging.info(f"Top {top_n} prospects exported to {output_file}")

# 10. Data Validation Functions
def validate_input_data(df: pd.DataFrame) -> bool:
    """Validate the input data to ensure all required fields are present."""
    required_columns = [
        'equityPercent', 'lastSaleDate', 'absenteeOwner', 'vacant',
        'taxLien', 'preForeclosure', 'quitClaim', 'sheriffsDeed', 'trusteeSale'
    ]
    missing_columns = [col for col in required_columns if col not in df.columns]
    
    if missing_columns:
        logging.warning(f"Missing required columns: {', '.join(missing_columns)}")
        return False
    return True

def handle_missing_data(df: pd.DataFrame) -> pd.DataFrame:
    """Handle missing data in the dataframe."""
    # Fill numeric columns with 0
    numeric_columns = df.select_dtypes(include=[np.number]).columns
    df[numeric_columns] = df[numeric_columns].fillna(0)
    
    # Fill categorical columns with 'Unknown'
    categorical_columns = df.select_dtypes(include=['object']).columns
    df[categorical_columns] = df[categorical_columns].fillna('Unknown')
    
    # Fill date columns with a default date
    date_columns = ['lastSaleDate', 'lastUpdateDate']
    for col in date_columns:
        if col in df.columns:
            df[col] = df[col].fillna(pd.Timestamp('1900-01-01'))
    
    return df

# 11. Main Execution Function
def main() -> None:
    """Main execution function that orchestrates the entire process."""
    try:
        # 11.1 File Selection
        selected_file = select_file()
        if not selected_file:
            raise SystemExit("No file selected. Exiting.")

        # 11.2 Data Loading and Preparation
        df = load_data(selected_file)
        
        # 11.3 Data Validation
        if not validate_input_data(df):
            user_input = input("Some required columns are missing. Do you want to continue? (y/n): ")
            if user_input.lower() != 'y':
                raise SystemExit("Exiting due to missing data.")
        
        # 11.4 Handle Missing Data
        df = handle_missing_data(df)
        df = prepare_data(df)

        # 11.5 Score Calculation
        df = calculate_property_scores(df)

        # 11.6 Analysis
        report, top_prospects = perform_analysis(df)

        # 11.7 Visualization
        generate_visualizations(df)

        # 11.8 Save Results
        base_filename = os.path.splitext(selected_file)[0]
        save_results(df, report, base_filename)

        # 11.9 Additional Outputs
        score_description = describe_scores(df)
        print("\nScore Statistics:")
        print(score_description)

        # 11.10 Export Top Prospects
        export_top_prospects(df, f"{base_filename}_top_100_prospects.csv")

        logging.info("Script execution completed successfully")
    except Exception as e:
        logging.error(f"An error occurred: {str(e)}")
        print(f"An error occurred: {str(e)}")
        print("Please check the logs for more details.")

# 12. Script Execution
if __name__ == "__main__":
    main()