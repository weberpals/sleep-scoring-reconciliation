import pandas as pd
import os
from pathlib import Path

def find_disagreements(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path,  sep=';')
    
    # Select only the columns we're interested in
    df_scores = df[['LS', 'ES', 'MS']]
    
    # Find rows where there's disagreement
    disagreements = df_scores[df_scores.apply(lambda row: len(set(row)) > 1, axis=1)]
    
    disagreements = disagreements.reset_index()
    
    return disagreements

def process_all_files(staging_data_dir, disagreement_dir):
    # Create the disagreement directory if it doesn't exist
    Path(disagreement_dir).mkdir(parents=True, exist_ok=True)

    # Process each file in the staging_data directory
    for filename in os.listdir(staging_data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(staging_data_dir, filename)
            disagreements = find_disagreements(file_path)
            
            # Create the output filename
            output_filename = f'{os.path.splitext(filename)[0]}_disagreements.csv'
            output_path = os.path.join(disagreement_dir, output_filename)
            
            # Save disagreements to CSV
            disagreements.to_csv(output_path, index=False)
            print(f"Processed {filename}: Found {len(disagreements)} disagreements.")

# Usage
staging_data_dir = 'staging_data'  
disagreement_dir = 'disagreement' 

process_all_files(staging_data_dir, disagreement_dir)