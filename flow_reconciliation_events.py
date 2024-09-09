import os
from datetime import datetime, timedelta
from collections import defaultdict

def parse_event_file(file_path):
    events = []
    start_time = None
    with open(file_path, 'r') as f:
        for line in f:
            if line.startswith("Start Time:"):
                start_time = datetime.strptime(line.split(": ")[1].strip(), "%m/%d/%Y %I:%M:%S %p")
            elif "-" in line and ";" in line:
                time_range, rest = line.strip().split(";", 1)
                start, end = time_range.split("-")
                duration, event_type = rest.split(";")
                
                start = datetime.strptime(start, '%H:%M:%S,%f')
                end = datetime.strptime(end, '%H:%M:%S,%f')
                
                start = start.replace(year=start_time.year, month=start_time.month, day=start_time.day)
                end = end.replace(year=start_time.year, month=start_time.month, day=start_time.day)
                
                if end < start:
                    end += timedelta(days=1)
                
                events.append((start, end, event_type.strip()))
    return events, start_time

def reconcile_events(all_events):
    merged_events = defaultdict(list)
    for scorer, events in all_events.items():
        for start, end, event_type in events:
            merged_events[start, end].append((scorer, event_type))
    
    reconciled_events = []
    events_needing_reconciliation = []
    
    for (start, end), scorers_and_types in merged_events.items():
        if len(scorers_and_types) == 3 and len(set(event_type for _, event_type in scorers_and_types)) == 1:
            # All three scorers agree on the event and its type
            reconciled_events.append((start, end, scorers_and_types[0][1]))
        else:
            # Disagreement in either the event timing or type
            events_needing_reconciliation.append((start, end, scorers_and_types))
    
    return reconciled_events, events_needing_reconciliation

def reconcile_study(study_path):
    scorers = ['LS', 'ES', 'MS']
    all_events = {}
    
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Flow Events.txt')
        events, _ = parse_event_file(file_path)
        all_events[scorer] = events
    
    reconciled_events, events_needing_reconciliation = reconcile_events(all_events)
    
    return reconciled_events, events_needing_reconciliation

def process_all_studies(data_path):
    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            reconciled_events, events_needing_reconciliation = reconcile_study(study_path)
            
            # Generate summary
            summary = f"Study: {study}\n"
            summary += f"Total events: {len(reconciled_events) + len(events_needing_reconciliation)}\n"
            summary += f"Reconciled events: {len(reconciled_events)}\n"
            summary += f"Events needing reconciliation: {len(events_needing_reconciliation)}\n"
            summary += f"Reconciliation success rate: {(len(reconciled_events) / (len(reconciled_events) + len(events_needing_reconciliation))) * 100:.2f}%\n\n"
            
            with open(os.path.join(study_path, 'summary_events.txt'), 'w') as f:
                f.write(summary)
            
            # Generate reconciliation file
            with open(os.path.join(study_path, 'events_needing_reconciliation.txt'), 'w') as f:
                for start, end, scorers_and_types in events_needing_reconciliation:
                    f.write(f"Event: {start.strftime('%H:%M:%S,%f')} - {end.strftime('%H:%M:%S,%f')}\n")
                    for scorer, event_type in scorers_and_types:
                        f.write(f"  {scorer}: {event_type}\n")
                    f.write("\n")
            
            print(f"Processed study: {study}")
# Usage
data_path = 'data_all'
process_all_studies(data_path)