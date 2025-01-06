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

def reconcile_study(study_path, output_dir):
    error_log = os.path.join(output_dir, "error_log.txt")
    scorers = ['LS', 'ES', 'MS']
    all_events = {}
    study_start_time = None

    print(f"Processing study: {study_path}")

    # Parse events from each scorer
    event_counts = []  # Track number of events per scorer
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Classification Arousals.txt')
        if not os.path.exists(file_path):
            print(f"File not found for scorer {scorer}: {file_path}")
            event_counts.append(0)
            continue
        events, start_time = parse_event_file(file_path)
        event_counts.append(len(events))
        if len(events) == 0:
            with open(error_log, 'a') as f:
                f.write(f"{datetime.now()}: WARNING - No events found for scorer {scorer} in study {study_path}\n")
            continue
        all_events[scorer] = events
        if study_start_time is None or start_time < study_start_time:
            study_start_time = start_time
        print(f"Parsed {len(events)} events for scorer {scorer}")

    # Check if we have any events at all
    if not all_events:
        raise ValueError(f"No valid event files found for any scorer. Event counts: {dict(zip(scorers, event_counts))}")

    if sum(event_counts) == 0:
        raise ValueError(f"No events found in any scorer files. Event counts: {dict(zip(scorers, event_counts))}")

    # Get all bins where any scorer has an event - with error handling
    try:
        last_event_end = max(max(event[1] for event in events) for events in all_events.values())
    except ValueError:
        raise ValueError("No events found in any of the parsed files")

    # If study start time and last event are more than 2 days apart, raise an error
    if (last_event_end - study_start_time).days > 2:
        raise ValueError(f"Study start time and last event are more than 2 days apart: {study_start_time} to {last_event_end}")

    print(f"Study start time: {study_start_time}")

    # Get all bins where any scorer has an event
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
        # Get scores for each bin in the event
        bin_scores_for_event = {bin_time: {scorer: bin_scores[bin_time][scorer] for scorer in scorers} for bin_time in event_bins}
        
        # Find the start and end of the period scored by at least two techs
        start_two_techs = None
        end_two_techs = None
        scored_by_all = False
        exact_start = None
        for bin_time, scores in bin_scores_for_event.items():
            # if bin time 2019-08-06T01:44:28.160 print marker
            if bin_time == datetime(2019, 8, 6, 1, 44, 28):
                print("marker")
            if sum(scores.values()) >= 2:
                if sum(scores.values()) == 3:
                    scored_by_all = True
                if start_two_techs is None:
                    start_two_techs = bin_time
                    # Find the earliest exact start time from original events
                    exact_start = min(event[0] for scorer, events in all_events.items() 
                                      for event in events if event[0].replace(microsecond=0) == bin_time)
                end_two_techs = bin_time
        
        if start_two_techs and end_two_techs and scored_by_all:
            # Find the exact start and end times
            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == start_two_techs),
                              key=time_only,
                              default=start_two_techs)
            
            end_events = [event[1] for scorer, events in all_events.items() 
                          for event in events if event[1].replace(microsecond=0) == end_two_techs]
            
            exact_end = max(end_events, key=time_only, default=end_two_techs)
            
            if exact_end == end_two_techs:
                print(f"Info: No exact end time found for event {event_index}. Using bin time: {exact_end}")
            
            # Add the event scored by at least two techs
            final_events.append([exact_start, exact_end, "Arousal"])
            
            # Check for periods scored by only one tech
            one_tech_period_before = []
            one_tech_period_after = []

            for bin_time in event_bins:
                if bin_time < start_two_techs:
                    if sum(bin_scores_for_event[bin_time].values()) == 1:
                        if not one_tech_period_before:
                            # Find the exact start time for this period
                            scorer = next(scorer for scorer, score in bin_scores_for_event[bin_time].items() if score == 1)
                            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == bin_time),
                              default=bin_time,
                              key=time_only)
                            one_tech_period_before.append((exact_start, bin_time))
                        else:
                            one_tech_period_before.append((bin_time, bin_time))
                elif bin_time > end_two_techs:
                    if sum(bin_scores_for_event[bin_time].values()) == 1:
                        if not one_tech_period_after:
                            # Find the exact start time for this period
                            scorer = next(scorer for scorer, score in bin_scores_for_event[bin_time].items() if score == 1)
                            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == bin_time),
                              default=bin_time,
                              key=time_only)
                            one_tech_period_after.append((exact_start, bin_time))
                        else:
                            one_tech_period_after.append((bin_time, bin_time))

            # Add events for periods longer than 5 seconds
            for period in [one_tech_period_before, one_tech_period_after]:
                if period and len(period) > 5:  # More than 5 seconds
                    scorer = next(scorer for scorer, score in bin_scores_for_event[period[0][1]].items() if score == 1)
                    description = get_detailed_description({scorer: 'Arousal', **{s: 'No Arousal' for s in scorers if s != scorer}})
                    final_events.append([period[0][0], period[-1][1], description])
        else:
            # If no period is scored by at least two techs, mark the entire event for review
            exact_start = min((event[0] for scorer, events in all_events.items() 
                               for event in events if event[0].replace(microsecond=0) == event_bins[0]),
                              default=event_bins[0],
                              key=time_only)
            exact_end = max((event[1] for scorer, events in all_events.items() 
                             for event in events if event[1].replace(microsecond=0) == event_bins[-1]),
                            default=event_bins[-1],
                            key=time_only)
            description = get_detailed_description({scorer: 'Arousal' if any(bin_scores_for_event[bin_time][scorer] for bin_time in event_bins) else 'No Arousal' for scorer in scorers})
            final_events.append([exact_start, exact_end, description])

        print(f"Processed event {event_index + 1}: {event_bins[0]} - {event_bins[-1]}")
    
    print(f"Final number of events: {len(final_events)}")
    return final_events, study_start_time

def get_detailed_description(scores):
    return "Review: Arousal"

def process_study(study_path, output_dir):
    study_name = os.path.basename(study_path)
    output_csv = os.path.join(output_dir, f"{study_name}_arousal_reconciliation_no_label.csv")
    error_log = os.path.join(output_dir, "error_log.txt")

    try:
        final_events, study_start_time = reconcile_study(study_path, output_dir)

        with open(output_csv, 'w', newline='') as csvfile:
            csvwriter = csv.writer(csvfile, delimiter='\t')
            csvwriter.writerow(['Onset', 'Duration', 'Description'])

            for start, end, description in final_events:
                onset = start.strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3]
                duration = (end - start).total_seconds()
                csvwriter.writerow([onset, f"{duration:.2f}", description])

        print(f"Successfully processed study: {study_name}")
        return output_csv, None
    except Exception as e:
        error_message = f"Error processing {study_name}: {str(e)}"
        print(error_message)
        with open(error_log, 'a') as f:
            f.write(f"{datetime.now()}: {error_message}\n")
        return None, error_message

def process_all_studies(data_path, output_dir):
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    processed_files = []
    failed_studies = []

    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            output_csv, error = process_study(study_path, output_dir)
            if output_csv:
                processed_files.append(output_csv)
            if error:
                failed_studies.append((study, error))

    # Print summary
    print(f"\nProcessing Summary:")
    print(f"Successfully processed: {len(processed_files)} studies")
    print(f"Failed: {len(failed_studies)} studies")
    if failed_studies:
        print("\nFailed studies:")
        for study, error in failed_studies:
            print(f"- {study}: {error}")
    print(f"\nCSV files created in: {output_dir}")
    print(f"See error_log.txt for detailed error information")

    return processed_files, failed_studies

# Helper function to compare only time components
def time_only(dt):
    return dt.time()

# Usage
data_path = '../../../data_all'
output_dir = '../../output/arousal_reconciliation_output'
processed_files, failed_studies = process_all_studies(data_path, output_dir)