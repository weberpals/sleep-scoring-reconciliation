import pandas as pd
import os
from pathlib import Path
import csv
import re

def analyze_agreement_and_generate_simplified_annotations(file_path, output_dir, require_full_agreement=False):
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
        ls_cols = [col for col in df.columns if 'AS - ls' in col]

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
    rows_with_partial_agreement = 0

    for index, row in df_scores.iterrows():
        es_score = row[es_cols[0]]
        ms_score = row[ms_cols[0]]
        ls_score = row[ls_cols[0]]
        
        # Get all unique scores in this row
        unique_scores = set([es_score, ms_score, ls_score])
        
        # Calculate onset time
        onset = index * epoch_duration
        duration = epoch_duration
        
        if len(unique_scores) == 1:
            # All scorers agree
            description = f"Stage: {es_score}"
            annotations.append([onset, duration, description])
        elif len(unique_scores) == 2 and not require_full_agreement:
            # Two scorers agree - find the majority score
            rows_with_partial_agreement += 1
            
            if es_score == ms_score:
                majority_score = es_score
            elif es_score == ls_score:
                majority_score = es_score
            elif ms_score == ls_score:
                majority_score = ms_score
            else:
                # This should never happen when len(unique_scores) == 2
                majority_score = "-"
                
            description = f"Stage: {majority_score}"
            annotations.append([onset, duration, description])
        else:
            # No agreement (or only partial agreement but full agreement required)
            rows_with_disagreement += 1
            description = "Stage: -"
            annotations.append([onset, duration, description])
    
    # Create annotations CSV file
    output_file = os.path.join(output_dir, f"{Path(file_path).stem}_stage_annotations.csv")
    with open(output_file, 'w', newline='', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter='\t')
        writer.writerow(['Onset', 'Duration', 'Description'])  # Header
        writer.writerows(annotations)
    
    return rows_with_disagreement, rows_with_partial_agreement, len(df_scores)

def process_all_files(data_dir, output_dir, require_full_agreement=False):
    results = []
    
    for study_folder in os.listdir(data_dir):
        print(f"Processing {study_folder}")
        study_path = os.path.join(data_dir, study_folder)
        if os.path.isdir(study_path):
            for filename in os.listdir(study_path):
                if re.match(r'[A-Za-z]{3}\d{2,3}', filename) and filename.endswith('.csv'):
                    file_path = os.path.join(study_path, filename)
                    try:
                        disagreement_count, partial_agreement_count, total_epochs = analyze_agreement_and_generate_simplified_annotations(
                            file_path, output_dir, require_full_agreement)
                        results.append((filename, disagreement_count, partial_agreement_count, total_epochs))
                        print(f"Processed {filename}")
                    except Exception as e:
                        print(f"Error processing {filename}: {str(e)}")
                else:
                    print(f"Skipping {filename}")
        else:
            print(f"Skipping {study_folder}")
    
    return results

def print_results(results, require_full_agreement=False):
    total_epochs_all = 0
    total_disagreement_all = 0
    total_partial_agreement_all = 0
    disagreement_percentages = []
    
    # Print individual file results
    for filename, disagreement_count, partial_agreement_count, total_epochs in results:
        needing_review = disagreement_count if not require_full_agreement else (disagreement_count + partial_agreement_count)
        percentage = (needing_review / total_epochs) * 100 if total_epochs > 0 else 0
        disagreement_percentages.append(percentage)
        
        total_epochs_all += total_epochs
        total_disagreement_all += disagreement_count
        total_partial_agreement_all += partial_agreement_count
        
        print(f"\nResults for {filename}:")
        print(f"Total epochs: {total_epochs}")
        print(f"Complete agreement: {total_epochs - disagreement_count - partial_agreement_count}")
        print(f"Partial agreement (2 of 3): {partial_agreement_count}")
        print(f"No agreement: {disagreement_count}")
        print(f"Epochs needing review: {needing_review}")
        print(f"Percentage needing review: {percentage:.2f}%")
    
    # Print summary statistics
    print("\n=== Summary Statistics ===")
    print(f"Total files processed: {len(results)}")
    print(f"Total epochs across all files: {total_epochs_all}")
    total_needing_review = total_disagreement_all if not require_full_agreement else (total_disagreement_all + total_partial_agreement_all)
    print(f"Total epochs with complete agreement: {total_epochs_all - total_disagreement_all - total_partial_agreement_all}")
    print(f"Total epochs with partial agreement: {total_partial_agreement_all}")
    print(f"Total epochs with no agreement: {total_disagreement_all}")
    print(f"Total annotations needing review: {total_needing_review}")
    
    if total_epochs_all > 0:
        overall_percentage = (total_needing_review / total_epochs_all) * 100
        print(f"Overall percentage needing review: {overall_percentage:.2f}%")
    
    if disagreement_percentages:
        print(f"Average percentage needing review per file: {sum(disagreement_percentages) / len(disagreement_percentages):.2f}%")
        print(f"Min percentage: {min(disagreement_percentages):.2f}%")
        print(f"Max percentage: {max(disagreement_percentages):.2f}%")
        
        # Calculate quartiles
        disagreement_percentages.sort()
        q1_idx = len(disagreement_percentages) // 4
        q3_idx = q1_idx * 3
        print(f"25th percentile: {disagreement_percentages[q1_idx]:.2f}%")
        print(f"Median percentage: {disagreement_percentages[len(disagreement_percentages) // 2]:.2f}%")
        print(f"75th percentile: {disagreement_percentages[q3_idx]:.2f}%")

# Usage
data_dir = 'data_all'  
output_dir = 'output/staging_annotation'  
require_full_agreement = False  # Set to True if you want to require all 3 scorers to agree

results = process_all_files(data_dir, output_dir, require_full_agreement)
print_results(results, require_full_agreement)
