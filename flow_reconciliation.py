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

def reconcile_study(study_path):
    scorers = ['LS', 'ES', 'MS']
    all_events = {}
    study_start_time = None

    print(f"Processing study: {study_path}")

    # Parse events from each scorer
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Flow Events.txt')
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

    # Create a mapping from bin_time to scores and event types
    bin_scores = {}
    for bin_time in all_bins:
        bin_scores[bin_time] = {scorer: {'score': 0, 'event_type': None} for scorer in scorers}
    for scorer, events in all_events.items():
        for start, end, event_type in events:
            current = start
            while current <= end:
                bin_time = current.replace(microsecond=0)
                if bin_time in bin_scores:
                    bin_scores[bin_time][scorer] = {'score': 1, 'event_type': event_type}
                current += timedelta(seconds=1)

    # Group bins into contiguous events
    events = []
    current_event_bins = []
    for i, bin_time in enumerate(all_bins):

        score_sum = sum(bin_scores[bin_time][scorer]['score'] for scorer in scorers)
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
        # Get scores and event types for each bin in the event
        bin_scores_for_event = {bin_time: {scorer: bin_scores[bin_time][scorer] for scorer in scorers} for bin_time in event_bins}
        
        # Find the start and end of the period scored by at least two techs with matching event types
        start_two_techs = None
        end_two_techs = None
        scored_by_all = False
        event_type = None
        exact_start = None
        for bin_time, scores in bin_scores_for_event.items():
            # Get all non-None event types
            current_event_types = [s['event_type'] for s in scores.values() if s['score'] == 1 and s['event_type'] is not None]
            # Count occurrences of each event type
            event_type_counts = {et: current_event_types.count(et) for et in set(current_event_types)}
            # Find the most common event type 
            matching_event_type = max(event_type_counts, key=event_type_counts.get, default=None)
            matching_scores = event_type_counts.get(matching_event_type, 0) if matching_event_type else 0

            if matching_scores >= 2:
                if matching_scores == 3:
                    scored_by_all = True
                if start_two_techs is None:
                    start_two_techs = bin_time
                    event_type = matching_event_type
                    # Find the earliest exact start time from original events
                    exact_start = min(event[0] for scorer, events in all_events.items() 
                                      for event in events if event[0].replace(microsecond=0) == bin_time)
                end_two_techs = bin_time
        
        if start_two_techs and end_two_techs and scored_by_all:
            # Add the event scored by at least two techs with matching event types
            final_events.append([exact_start, end_two_techs, event_type])
            
            # Check for periods scored by only one tech or with different event types
            one_tech_period_before = []
            one_tech_period_after = []

            for bin_time in event_bins:
                if bin_time < start_two_techs:
                    scores = bin_scores_for_event[bin_time]
                    if sum(s['score'] for s in scores.values()) == 1 or len(set(s['event_type'] for s in scores.values() if s['score'] == 1)) > 1:
                        if not one_tech_period_before:
                            # Find the exact start time for this period
                            scorer = next(scorer for scorer, s in scores.items() if s['score'] == 1)
                            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == bin_time),
                              default=bin_time)
                            one_tech_period_before.append((exact_start, bin_time))
                        else:
                            one_tech_period_before.append((bin_time, bin_time))
                elif bin_time > end_two_techs:
                    scores = bin_scores_for_event[bin_time]
                    if sum(s['score'] for s in scores.values()) == 1 or len(set(s['event_type'] for s in scores.values() if s['score'] == 1)) > 1:
                        if not one_tech_period_after:
                            # Find the exact start time for this period
                            scorer = next(scorer for scorer, s in scores.items() if s['score'] == 1)
                            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == bin_time),
                              default=bin_time)
                            one_tech_period_after.append((exact_start, bin_time))
                        else:
                            one_tech_period_after.append((bin_time, bin_time))

            # Add events for periods longer than 5 seconds
            for period in [one_tech_period_before, one_tech_period_after]:
                if period and len(period) > 5:  # More than 5 seconds
                    scores = bin_scores_for_event[period[0][1]]
                    description = get_detailed_description({scorer: scores[scorer]['event_type'] if scores[scorer]['score'] == 1 else None for scorer in scorers})
                    final_events.append([period[0][0], period[-1][1], description])
        else:
            # If no period is scored by at least two techs with matching event types, mark the entire event for review
            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == event_bins[0]),
                              default=event_bins[0])
            description = get_detailed_description({scorer: scores['event_type'] if any(bin_scores_for_event[bin_time][scorer]['score'] for bin_time in event_bins) else None for scorer, scores in bin_scores_for_event[event_bins[0]].items()})
            final_events.append([exact_start, event_bins[-1], description])

        print(f"Processed event {event_index + 1}: {event_bins[0]} - {event_bins[-1]}")
    
    print(f"Final number of events: {len(final_events)}")
    return final_events, study_start_time

def get_detailed_description(scores):
    return "Review: " + ", ".join([str(score) if score is not None else '-' for score in scores.values()])

def process_study(study_path, output_dir):
    study_name = os.path.basename(study_path)
    output_csv = os.path.join(output_dir, f"{study_name}_flow_reconciliation.csv")

    final_events, study_start_time = reconcile_study(study_path)

    with open(output_csv, 'w', newline='') as csvfile:
        csvwriter = csv.writer(csvfile, delimiter='\t')
        csvwriter.writerow(['Onset', 'Duration', 'Description'])

        for start, end, description in final_events:
            onset = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
            duration = int((end - start).total_seconds()) + 1
            csvwriter.writerow([onset, duration, description])

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
output_dir = './output/flow_reconciliation_output'
processed_files = process_all_studies(data_path, output_dir)
