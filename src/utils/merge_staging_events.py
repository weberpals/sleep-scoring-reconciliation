import pandas as pd
from datetime import datetime, timedelta
import os
import glob


def parse_markers_file(awv_id):
    """Extract start time from markers file"""
    markers_path = f"../data_all/{awv_id}/ES/Markers.txt"
    
    with open(markers_path, 'r') as f:
        for line in f:
            if '; Start' in line:
                time_str = line.split(';')[0]
                # Parse the time in HH:MM:SS,mmm format
                time_parts = time_str.split(',')
                dt = datetime.strptime(time_parts[0], '%H:%M:%S')
                return dt.time()
    return None

def combine_staging_and_events(awv_id):
    # Read the combined events file
    events_df = pd.read_csv(f'output/combined/{awv_id}_combined_events.csv', sep='\t')
    
    # Read the staging annotations
    staging_df = pd.read_csv(f'output/staging_annotation/{awv_id}_stage_annotations_numbered.csv', sep='\t')
    
    # Get the start time from markers file
    start_time = parse_markers_file(awv_id)
    if not start_time:
        print(f"Could not find start time for {awv_id}")
        return
    
    # Convert start_time to datetime using the date from events file
    first_event = pd.to_datetime(events_df['Onset'].iloc[0])
    base_date = first_event.date()
    start_datetime = datetime.combine(base_date, start_time)
    
    # Convert staging seconds to timestamps
    staging_df['Onset'] = staging_df['Onset'].apply(
        lambda x: (start_datetime + timedelta(seconds=float(x))).strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
    )
    
    
    # Combine the dataframes
    combined_df = pd.concat([events_df, staging_df])
    
    # Sort by onset time
    combined_df = combined_df.sort_values('Onset')
    
    # Save the result
    output_path = f'output/merged/{awv_id}_merged.csv'
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    combined_df.to_csv(output_path, sep='\t', index=False)
    
def main():
    # Find all combined event files
    event_files = glob.glob('output/combined/*_combined_events.csv')
    
    for event_file in event_files:
        # Extract AWV ID from filename
        awv_id = os.path.basename(event_file).split('_')[0]
        
        # Check if corresponding staging file exists
        staging_file = f'output/staging_annotation/{awv_id}_stage_annotations_numbered.csv'
        if os.path.exists(staging_file):
            print(f"Processing {awv_id}...")
            combine_staging_and_events(awv_id)
        else:
            print(f"No staging file found for {awv_id}")

if __name__ == "__main__":
    main() 