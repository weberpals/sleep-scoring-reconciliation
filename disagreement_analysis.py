import pandas as pd
import os
from pathlib import Path

def analyze_agreement(file_path):
    # Read the CSV file
    df = pd.read_csv(file_path, sep=';')
    
    # Select only the columns we're interested in
    df_scores = df[['ES', 'MS', 'LS']]
    
    total_periods = len(df_scores)
    unanimous = 0
    two_agree = 0
    no_agreement = 0
    
    for _, row in df_scores.iterrows():
        unique_scores = len(set(row))
        if unique_scores == 1:
            unanimous += 1
        elif unique_scores == 2:
            two_agree += 1
        else:
            no_agreement += 1
    
    return {
        'total': total_periods,
        'unanimous': (unanimous, unanimous/total_periods*100),
        'two_agree': (two_agree, two_agree/total_periods*100),
        'no_agreement': (no_agreement, no_agreement/total_periods*100)
    }

def process_all_files(staging_data_dir):
    results = []
    
    for filename in os.listdir(staging_data_dir):
        if filename.endswith('.csv'):
            file_path = os.path.join(staging_data_dir, filename)
            try:
                result = analyze_agreement(file_path)
                results.append((filename, result))
                print(f"Processed {filename}")
            except Exception as e:
                print(f"Error processing {filename}: {str(e)}")
    
    return results

def print_results(results):
    for filename, result in results:
        print(f"\nResults for {filename}:")
        print(f"Total periods: {result['total']}")
        print(f"Unanimous agreement: {result['unanimous'][0]} ({result['unanimous'][1]:.2f}%)")
        print(f"Two scorers agree: {result['two_agree'][0]} ({result['two_agree'][1]:.2f}%)")
        print(f"No agreement: {result['no_agreement'][0]} ({result['no_agreement'][1]:.2f}%)")

# Usage
staging_data_dir = 'staging_data'  # Replace with the path to your staging_data directory

results = process_all_files(staging_data_dir)
print_results(results)