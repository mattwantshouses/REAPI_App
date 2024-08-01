# Property Scoring Algorithm 1
# Version 1.4

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from google.colab import files
import json
import logging
import os
from datetime import datetime

# 1. Setup and Data Loading
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def select_file():
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
                else:
                    logging.error("No file was uploaded.")
                    return None
            else:
                print("Invalid selection. Please try again.")
        except ValueError:
            print("Please enter a valid number.")

selected_file = select_file()
if not selected_file:
    raise SystemExit("No file selected. Exiting.")

logging.info(f"Loading data from {selected_file}")

# 2. Data Preparation
logging.info("Preparing data")

# 2.1 Handle missing values
df = df.fillna(0)  # Replace with more sophisticated handling if needed

# 2.2 Convert date fields
date_columns = ['data.lastSaleDate', 'data.lastUpdateDate']
for col in date_columns:
    df[col] = pd.to_datetime(df[col], errors='coerce')

# 2.3 Create derived features
df['days_since_last_sale'] = (datetime.now() - df['data.lastSaleDate']).dt.days
df['is_absentee_owner'] = df['data.absenteeOwner'].astype(int)
df['is_vacant'] = df['data.vacant'].astype(int)

# 3. Scoring System
logging.info("Applying scoring system")

def equity_score(equity_percent):
    return np.clip(equity_percent / 100, 0, 1)

def ownership_length_score(days):
    # U-shaped function: high for recent purchases and long-term ownership
    return 1 - np.sin(np.clip(days / 3650, 0, np.pi/2))

df['equity_score'] = df['data.equityPercent'].apply(equity_score)
df['ownership_score'] = df['days_since_last_sale'].apply(ownership_length_score)
df['absentee_score'] = df['is_absentee_owner'] * 0.5
df['vacant_score'] = df['is_vacant'] * 0.5

# Combine scores
df['total_score'] = (df['equity_score'] + df['ownership_score'] +
                     df['absentee_score'] + df['vacant_score']) / 4

# 4. Analysis
logging.info("Performing analysis")

# 4.1 Rank properties
df['rank'] = df['total_score'].rank(method='dense', ascending=False)

# 4.2 Identify top prospects
top_percent = 20
top_threshold = df['total_score'].quantile(1 - top_percent/100)
df['is_top_prospect'] = df['total_score'] >= top_threshold

# 5. Visualization
logging.info("Generating visualizations")

plt.figure(figsize=(10, 6))
sns.histplot(df['total_score'], kde=True)
plt.title('Distribution of Total Scores')
plt.savefig('score_distribution.png')
plt.close()

plt.figure(figsize=(10, 6))
sns.scatterplot(data=df, x='data.equityPercent', y='days_since_last_sale', hue='is_top_prospect')
plt.title('Equity vs Ownership Length')
plt.savefig('equity_vs_ownership.png')
plt.close()

# 6. Output
logging.info("Preparing output")

# 6.1 Generate report
top_prospects = df[df['is_top_prospect']].sort_values('total_score', ascending=False)
report = f"""
Analysis Report:
Total properties analyzed: {len(df)}
Number of top prospects: {len(top_prospects)}
Average score: {df['total_score'].mean():.2f}
Median score: {df['total_score'].median():.2f}
"""

with open('analysis_report.txt', 'w') as f:
    f.write(report)

# 6.2 Save results
base_filename = os.path.splitext(selected_file)[0]
json_output_file = f"{base_filename}_analyzed.json"
csv_output_file = f"{base_filename}_analyzed.csv"

# Sort the DataFrame by 'rank' in ascending order
df_sorted = df.sort_values('rank', ascending=True)

# Save as JSON
json_data = df_sorted.to_json(orient='records')
with open(json_output_file, 'w') as f:
    json.dump(json.loads(json_data), f, indent=2)

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

logging.info("Script execution completed")