import pandas as pd
import os
from pathlib import Path
import csv
import re

def analyze_agreement_and_generate_simplified_annotations(file_path, output_dir):
    # Read the CSV file
    df = pd.read_csv(file_path, sep=';')
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Select only the columns we're interested in
    df_scores = df[['ES', 'MS', 'LS']]

    # TODO: ignore data before start/stop signal
    
    # Prepare annotations list
    annotations = []
    epoch_duration = 30  # Assuming 30-second epochs, adjust if different

    for index, row in df_scores.iterrows():
        unique_scores = len(set(row))
        if unique_scores > 1:  # There's disagreement
            onset = index * epoch_duration
            duration = epoch_duration
            description = "Stage: -"
            annotations.append([onset, duration, description])
        else:
            # All scorers agree
            onset = index * epoch_duration
            duration = epoch_duration
            description = f"Stage: {row['ES']}"
            annotations.append([onset, duration, description])
    
    # Create annotations CSV file
    output_file = os.path.join(output_dir, f"{Path(file_path).stem}_stage_annotations.csv")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(['Onset', 'Duration', 'Description'])  # Header
        writer.writerows(annotations)
    
    return len(annotations), len(df_scores)

def process_all_files(data_dir, output_dir):
    results = []
    
    for study_folder in os.listdir(data_dir):
        study_path = os.path.join(data_dir, study_folder)
        if os.path.isdir(study_path):
            for filename in os.listdir(study_path):
                if re.match(r'AWV\d{3}', filename) and filename.endswith('.csv'):
                    file_path = os.path.join(study_path, filename)
                    try:
                        annotations_count, total_epochs = analyze_agreement_and_generate_simplified_annotations(file_path, output_dir)
                        results.append((filename, annotations_count, total_epochs))
                        print(f"Processed {filename}")
                    except Exception as e:
                        print(f"Error processing {filename}: {str(e)}")
    
    return results

def print_results(results):
    for filename, annotations_count, total_epochs in results:
        print(f"\nResults for {filename}:")
        print(f"Total epochs: {total_epochs}")
        print(f"Annotations for review: {annotations_count}")

# Usage
data_dir = '../data_all'  
output_dir = './output/staging_annotation'  

results = process_all_files(data_dir, output_dir)
print_results(results)
