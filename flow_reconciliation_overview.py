import os
import pandas as pd
from datetime import datetime, timedelta

def parse_event_file(file_path):
    events = []
    with open(file_path, 'r') as f:
        for line in f:
            if line.strip() and '-' in line and ';' in line:
                time_range, duration, event_type = line.strip().split(';')
                start, end = time_range.split('-')
                start = datetime.strptime(start.strip(), '%H:%M:%S,%f')
                end = datetime.strptime(end.strip(), '%H:%M:%S,%f')
                events.append((start, end, event_type.strip(), float(duration)))
    return events

def overlap(event1, event2):
    overlap_start = max(event1[0], event2[0])
    overlap_end = min(event1[1], event2[1])
    if overlap_start < overlap_end:
        return (overlap_end - overlap_start).total_seconds()
    return 0


def reconcile_events(events1, events2, events3):
    all_events = events1 + events2 + events3
    all_events.sort(key=lambda x: x[0])  # Sort by start time

    reconciled_events = []
    current_event = None

    for event in all_events:
        if current_event is None:
            current_event = list(event) + [1, False]  # Add counter and merged flag
        else:
            overlap_duration = overlap(current_event, event)
            if overlap_duration > 0:
                # Merge overlapping events
                current_event[1] = max(current_event[1], event[1])  # Update end time
                current_event[3] = max(current_event[3], event[3])  # Update duration
                current_event[4] += 1  # Increment counter
                current_event[5] = True  # Set merged flag to True
            else:
                # No overlap, add current event to reconciled list and start a new one
                reconciled_events.append(tuple(current_event))
                current_event = list(event) + [1, False]

    if current_event:
        reconciled_events.append(tuple(current_event))

    return reconciled_events

def categorize_event(event):
    counter = event[4]
    if counter == 3:
        return "Fully Agreed"
    elif counter == 2:
        return "Partially Agreed"
    else:
        return "Single Scorer"

def reconcile_study(study_path):
    scorers = ['LS', 'ES', 'MS']
    all_events = {}

    # Load events from all scorers
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Flow Events.txt')
        all_events[scorer] = parse_event_file(file_path)

    reconciled_events = reconcile_events(all_events['LS'], all_events['ES'], all_events['MS'])

    # Create a DataFrame for easy analysis and export
    df = pd.DataFrame(reconciled_events, columns=['Start', 'End', 'Event Type', 'Duration', 'Scorer Count', 'Merged'])
    df['Category'] = df.apply(lambda row: categorize_event(row), axis=1)

    # Save reconciled events
    output_file = os.path.join(study_path, 'reconciled_events.csv')
    df.to_csv(output_file, index=False)

    # Identify events needing manual review
    manual_review = df[df['Category'] != 'Fully Agreed']
    manual_review_file = os.path.join(study_path, 'events_for_manual_review.csv')
    manual_review.to_csv(manual_review_file, index=False)

    # Generate summary
    summary = f"Study: {os.path.basename(study_path)}\n"
    summary += f"Total events: {len(df)}\n"
    summary += f"Fully Agreed events: {len(df[df['Category'] == 'Fully Agreed'])}\n"
    summary += f"Partially Agreed events: {len(df[df['Category'] == 'Partially Agreed'])}\n"
    summary += f"Single Scorer events: {len(df[df['Category'] == 'Single Scorer'])}\n"
    summary += f"Merged events: {len(df[df['Merged'] == True])}\n"
    summary += f"Events needing manual review: {len(manual_review)}\n"

    return summary

def process_all_studies(data_path):
    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            summary = reconcile_study(study_path)
            print(summary)
            
            # Save summary to file
            summary_file = os.path.join(study_path, 'reconciliation_summary.txt')
            with open(summary_file, 'w') as f:
                f.write(summary)

if __name__ == "__main__":
    data_path = '../data_all'
    process_all_studies(data_path)