import csv
import os
from datetime import datetime

def parse_datetime(dt_string):
    return datetime.strptime(dt_string, "%Y-%m-%dT%H:%M:%S.%f")

def read_csv(filename):
    with open(filename, 'r') as f:
        reader = csv.DictReader(f, delimiter='\t')
        return [row for row in reader]

def combine_and_sort_events(files):
    all_events = []
    stage_counter = 1  # Initialize counter outside the loop
    
    for file in files:
        events = read_csv(file)
        # Add numbers to staging descriptions if this is a staging file
        if 'staging' in file.lower():
            for event in events:
                if 'Stage:' in event['Description']:
                    event['Description'] = f"{stage_counter}. {event['Description']}"
                    stage_counter += 1
        all_events.extend(events)
    
    return sorted(all_events, key=lambda x: parse_datetime(x['Onset']))

def write_combined_csv(events, output_file):
    fieldnames = ['Onset', 'Duration', 'Description']
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, delimiter='\t')
        writer.writeheader()
        for event in events:
            writer.writerow(event)

def process_all_files():
    base_dir = 'output'
    folders = {
        'flow': 'flow_reconciliation_output',
        'event': 'arousal_reconciliation_output'
    }
    
    # Create the combined folder if it doesn't exist
    combined_dir = os.path.join(base_dir, 'combined')
    os.makedirs(combined_dir, exist_ok=True)
    
    # Get all unique subject IDs
    subject_ids = set()
    for folder in folders.values():
        folder_path = os.path.join(base_dir, folder)
        for filename in os.listdir(folder_path):
            subject_id = filename.split('_')[0]
            subject_ids.add(subject_id)
    
    # Process each subject
    for subject_id in subject_ids:
        input_files = []
        for folder_name, folder_path in folders.items():
            full_folder_path = os.path.join(base_dir, folder_path)
            matching_files = [
                os.path.join(full_folder_path, filename)
                for filename in os.listdir(full_folder_path)
                if filename.startswith(subject_id)
            ]
            input_files.extend(matching_files)
        
        if input_files:
            output_file = os.path.join(combined_dir, f'{subject_id}_combined_events.csv')
            combined_events = combine_and_sort_events(input_files)
            write_combined_csv(combined_events, output_file)
            print(f"Combined events for {subject_id} have been written to {output_file}")

if __name__ == "__main__":
    process_all_files()
