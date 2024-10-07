import os
from datetime import datetime, timedelta
import csv
import re

def parse_event_file(file_path):
    events = []
    start_time = None
    with open(file_path, 'r') as f:
        content = f.read()

    # Extract the Start Time
    start_time_match = re.search(r'Start Time:\s*(.*)', content)
    
    if start_time_match:
        start_time_str = start_time_match.group(1).strip()
        start_time = datetime.strptime(start_time_str, "%m/%d/%Y %I:%M:%S %p")
    else:
        raise ValueError("Start Time not found in the file.")

    # Adjusted regular expression to handle concatenated events
    event_pattern = r'(\d{2}:\d{2}:\d{2},\d{3}-\d{2}:\d{2}:\d{2},\d{3});\s*(\d+);\s*(.*?)(?=(\d{2}:\d{2}:\d{2},\d{3}-|$))'
    matches = re.finditer(event_pattern, content, re.DOTALL)

    for match in matches:
        time_range = match.group(1)
        duration = match.group(2)
        event_type = match.group(3).strip()

        start_str, end_str = time_range.split('-')

        # Parse start and end times
        start = datetime.strptime(start_str.strip(), '%H:%M:%S,%f')
        end = datetime.strptime(end_str.strip(), '%H:%M:%S,%f')

        # Adjust date components
        start = start.replace(year=start_time.year, month=start_time.month, day=start_time.day)
        end = end.replace(year=start_time.year, month=start_time.month, day=start_time.day)

        # Handle events that cross midnight
        if start.time() < start_time.time():
            start += timedelta(days=1)
        if end.time() < start_time.time() or end < start:
            end += timedelta(days=1)

        events.append((start, end, event_type))

    print(f"\nTotal events processed: {len(events)}")
    return events, start_time

def create_bins(events, bin_size=timedelta(seconds=1)):
    bins = set()
    for start, end, _ in events:
        current = start
        while current <= end:
            bins.add(current.replace(microsecond=0))
            current += bin_size
    return bins

def reconcile_study(study_path):
    scorers = ['LS', 'ES', 'MS']
    all_events = {}
    study_start_time = None

    print(f"Processing study: {study_path}")

    # Parse events from each scorer
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Classification Arousals.txt')
        if not os.path.exists(file_path):
            print(f"File not found for scorer {scorer}: {file_path}")
            continue  # Skip if the file doesn't exist
        events, start_time = parse_event_file(file_path)
        all_events[scorer] = events
        if study_start_time is None or start_time < study_start_time:
            study_start_time = start_time
        print(f"Parsed {len(events)} events for scorer {scorer}")

    print(f"Study start time: {study_start_time}")

    # Get all bins where any scorer has an event
    last_event_end = max(max(event[1] for event in events) for events in all_events.values())
    all_bins = []
    current_time = study_start_time
    while current_time <= last_event_end:
        all_bins.append(current_time)
        current_time += timedelta(seconds=1)
    
    print(f"Created {len(all_bins)} bins from {study_start_time} to {last_event_end}")

    # Create a mapping from bin_time to scores
    bin_scores = {}
    for bin_time in all_bins:
        bin_scores[bin_time] = {scorer: 0 for scorer in scorers}
    for scorer, events in all_events.items():
        for start, end, _ in events:
            current = start
            while current <= end:
                bin_time = current.replace(microsecond=0)
                if bin_time in bin_scores:
                    bin_scores[bin_time][scorer] = 1
                current += timedelta(seconds=1)

    # Group bins into contiguous events
    events = []
    current_event_bins = []
    for i, bin_time in enumerate(all_bins):
        score_sum = sum(bin_scores[bin_time].values())
        if score_sum > 0:
            current_event_bins.append(bin_time)
        else:
            if current_event_bins:
                events.append(current_event_bins)
                current_event_bins = []
    if current_event_bins:
        events.append(current_event_bins)


    final_events = []
   
    for event_index, event_bins in enumerate(events):
        # Get scores for the event
        # TODO: ensure that logic is reflected 
        # currently if event by 3 at any point, complete event  is scored as arousal
        event_scores = {scorer: 0 for scorer in scorers}
        for bin_time in event_bins:
            for scorer in scorers:
                if bin_scores[bin_time][scorer]:
                    event_scores[scorer] += 1

        scorers_with_arousal = [scorer for scorer, count in event_scores.items() if count > 0]
        num_scorers_with_arousal = len(scorers_with_arousal)

        # Check if the event is scored by all 3 technicians
        if num_scorers_with_arousal == 3:
            description = "Arousal"
        elif num_scorers_with_arousal == 2:
            # Check for a unanimous bin within the event
            unanimous_bin_exists = any(
                all(bin_scores[bin_time][scorer] == 1 for scorer in scorers)
                for bin_time in event_bins
            )
            if unanimous_bin_exists:
                description = "Arousal"
            else:
                # Need review
                first_bin_scores = {scorer: 'Arousal' if bin_scores[event_bins[0]][scorer] else 'No Arousal' for scorer in scorers}
                description = get_detailed_description(first_bin_scores)
        else:
            # Need review
            first_bin_scores = {scorer: 'Arousal' if bin_scores[event_bins[0]][scorer] else 'No Arousal' for scorer in scorers}
            description = get_detailed_description(first_bin_scores)

        # Add the event to final_events
        start = event_bins[0]
        end = event_bins[-1]
        final_events.append([start, end, description])

        print(f"Processed event {event_index + 1}: {start} - {end}, Description: {description}")


    print(f"Final number of events: {len(final_events)}")
    return final_events, study_start_time

def get_detailed_description(scores):
    return "Review: " + ", ".join([f"{scorer}={score}" for scorer, score in scores.items()])

def process_study(study_path, output_dir):
    study_name = os.path.basename(study_path)
    output_csv = os.path.join(output_dir, f"{study_name}_event_reconciliation.csv")

    final_events, study_start_time = reconcile_study(study_path)

    with open(output_csv, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile)
        csvwriter.writerow(['Onset', 'Duration', 'Description'])

        for idx, (start, end, description) in enumerate(final_events):
            onset = max(0, int((start - study_start_time).total_seconds()))
            duration = int((end - start).total_seconds()) + 1
            event_number = idx + 1
            csvwriter.writerow([onset, duration, f"E{event_number}: {description}"])

    print(f"Processed study: {study_name}")
    return output_csv

def process_all_studies(data_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    processed_files = []

    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            output_csv = process_study(study_path, output_dir)
            processed_files.append(output_csv)

    print(f"Processed {len(processed_files)} studies. CSV files created in: {output_dir}")
    return processed_files

# Usage
data_path = '../data_all'
output_dir = './event_reconciliation_output'
processed_files = process_all_studies(data_path, output_dir)
