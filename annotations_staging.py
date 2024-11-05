import pandas as pd
import os
from pathlib import Path
import csv
import re

def analyze_agreement_and_generate_simplified_annotations(file_path, output_dir):
    # Read the CSV file
    df = pd.read_csv(file_path, sep=';')
    
    # Find columns containing the strings
    es_cols = [col for col in df.columns if 'ES' in col]
    ms_cols = [col for col in df.columns if 'MS' in col]
    ls_cols = [col for col in df.columns if 'LS' in col]
    
    # Check for duplicate matches
    if len(es_cols) > 1 or len(ms_cols) > 1 or len(ls_cols) > 1:
        # For ES look for "AUTOSCORE ES" if it exists
        if "AUTOSCORE ES" in es_cols:
            es_cols = ["AUTOSCORE ES"]
        elif "AUTO-SCORE ES" in es_cols:
            es_cols = ["AUTO-SCORE ES"]
        elif "AUTO SCORE ES" in es_cols:
            es_cols = ["AUTO SCORE ES"]

        if "MS-AUTOSCORE" in ms_cols:
            ms_cols = ["MS-AUTOSCORE"]

        if "ASLS" in ls_cols:
            ls_cols = ["ASLS"]
        
        if len(es_cols) > 1 or len(ms_cols) > 1 or len(ls_cols) > 1:
            raise ValueError(f"Multiple columns found containing scorer strings: ES={es_cols}, MS={ms_cols}, LS={ls_cols}")
    
    # Check if all required columns were found
    if not (len(es_cols) == 1 and len(ms_cols) == 1 and len(ls_cols) == 1):
        raise ValueError(f"Missing required scorer columns: ES={es_cols}, MS={ms_cols}, LS={ls_cols}")
    
    # Select the columns
    df_scores = df[[es_cols[0], ms_cols[0], ls_cols[0]]]
    
    # Ensure the output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    # Prepare annotations list
    annotations = []
    epoch_duration = 30  # Assuming 30-second epochs, adjust if different

    rows_with_disagreement = 0

    for index, row in df_scores.iterrows():
        unique_scores = len(set(row))
        if unique_scores > 1:  # There's disagreement
            rows_with_disagreement += 1
            onset = index * epoch_duration
            duration = epoch_duration
            description = "Stage: -"
            annotations.append([onset, duration, description])
        else:
            # All scorers agree
            onset = index * epoch_duration
            duration = epoch_duration
            description = f"Stage: {row[es_cols[0]]}"
            annotations.append([onset, duration, description])
    
    # Create annotations CSV file
    output_file = os.path.join(output_dir, f"{Path(file_path).stem}_stage_annotations.csv")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(['Onset', 'Duration', 'Description'])  # Header
        writer.writerows(annotations)
    
    return rows_with_disagreement, len(df_scores)

def process_all_files(data_dir, output_dir):
    results = []
    
    for study_folder in os.listdir(data_dir):
        study_path = os.path.join(data_dir, study_folder)
        if os.path.isdir(study_path):
            for filename in os.listdir(study_path):
                if re.match(r'[A-Za-z]{3}\d{2,3}', filename) and filename.endswith('.csv'):
                    file_path = os.path.join(study_path, filename)
                    try:
                        annotations_count, total_epochs = analyze_agreement_and_generate_simplified_annotations(file_path, output_dir)
                        results.append((filename, annotations_count, total_epochs))
                        print(f"Processed {filename}")
                    except Exception as e:
                        print(f"Error processing {filename}: {str(e)}")
    
    return results

def print_results(results):
    total_epochs_all = 0
    total_annotations_all = 0
    percentages = []
    
    # Print individual file results
    for filename, annotations_count, total_epochs in results:
        percentage = (annotations_count / total_epochs) * 100 if total_epochs > 0 else 0
        percentages.append(percentage)
        total_epochs_all += total_epochs
        total_annotations_all += annotations_count
        
        print(f"\nResults for {filename}:")
        print(f"Total epochs: {total_epochs}")
        print(f"Annotations for review: {annotations_count}")
        print(f"Percentage needing review: {percentage:.2f}%")
    
    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total files processed: {len(results)}")
    print(f"Total epochs across all files: {total_epochs_all}")
    print(f"Total annotations needing review: {total_annotations_all}")
    
    if total_epochs_all > 0:
        overall_percentage = (total_annotations_all / total_epochs_all) * 100
        print(f"Overall percentage needing review: {overall_percentage:.2f}%")
    
    if percentages:
        print(f"Average percentage per file: {sum(percentages) / len(percentages):.2f}%")
        print(f"Min percentage: {min(percentages):.2f}%")
        print(f"Max percentage: {max(percentages):.2f}%")
        
        # Calculate quartiles
        percentages.sort()
        q1_idx = len(percentages) // 4
        q3_idx = q1_idx * 3
        print(f"25th percentile: {percentages[q1_idx]:.2f}%")
        print(f"Median percentage: {percentages[len(percentages) // 2]:.2f}%")
        print(f"75th percentile: {percentages[q3_idx]:.2f}%")

# Usage
data_dir = '../data_all'  
output_dir = './output/staging_annotation'  

results = process_all_files(data_dir, output_dir)
print_results(results)
