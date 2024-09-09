import os
from datetime import datetime, timedelta

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
                
                start = datetime.strptime(start, '%H:%M:%S,%f').replace(microsecond=0)
                end = datetime.strptime(end, '%H:%M:%S,%f').replace(microsecond=0)
                
                start = start.replace(year=start_time.year, month=start_time.month, day=start_time.day)
                end = end.replace(year=start_time.year, month=start_time.month, day=start_time.day)
                
                if end < start:
                    end += timedelta(days=1)
                
                events.append((start, end, event_type.strip()))
    return events, start_time

def create_bins(events, bin_size=timedelta(seconds=1)):
    all_bins = set()
    for start, end, _ in events:
        current = start
        while current <= end:
            all_bins.add(current.replace(microsecond=0))
            current += bin_size
    return sorted(all_bins)

def score_bins(events, bins):
    scored_bins = {b: 0 for b in bins}
    for start, end, _ in events:
        for b in bins:
            if start <= b < end:
                scored_bins[b] += 1
    return scored_bins

def reconcile_study(study_path):
    scorers = ['LS', 'ES', 'MS']
    all_events = {}
    study_start_time = None
    
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Flow Events.txt')
        events, start_time = parse_event_file(file_path)
        all_events[scorer] = events
        if study_start_time is None or start_time < study_start_time:
            study_start_time = start_time
    
    all_bins = create_bins([event for scorer_events in all_events.values() for event in scorer_events])
    
    scored_bins = {scorer: score_bins(events, all_bins) for scorer, events in all_events.items()}
    
    reconciliation_needed = []
    final_events = []
    current_event = None
    successfully_reconciled = 0
    total_bins = len(all_bins)
    
    for bin_time in all_bins:
        score_count = sum(scored_bins[scorer][bin_time] for scorer in scorers)
        
        # Check if the previous bin exists and has a score of 3
        prev_bin_time = bin_time - timedelta(seconds=1)
        prev_bin_unanimous = prev_bin_time in all_bins and all(scored_bins[scorer].get(prev_bin_time, 0) == 3 for scorer in scorers)
        
        if score_count == 0:
            successfully_reconciled += 1
            if current_event:
                final_events.append(current_event)
                current_event = None
        elif score_count == 1:
            if prev_bin_unanimous:
                successfully_reconciled += 1
            else:
                reconciliation_needed.append(bin_time)
            if current_event:
                final_events.append(current_event)
                current_event = None
        elif score_count == 2:
            if prev_bin_unanimous:
                successfully_reconciled += 1
                if not current_event:
                    current_event = (bin_time, bin_time)
                else:
                    current_event = (current_event[0], bin_time)
            else:
                reconciliation_needed.append(bin_time)
        elif score_count == 3:
            successfully_reconciled += 1
            if not current_event:
                current_event = (bin_time, bin_time)
            else:
                current_event = (current_event[0], bin_time)
    
    if current_event:
        final_events.append(current_event)
    
    return final_events, reconciliation_needed, successfully_reconciled, total_bins


# Update the process_all_studies function to write reconciliation times without microseconds
def process_all_studies(data_path):
    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            final_events, reconciliation_needed, successfully_reconciled, total_bins = reconcile_study(study_path)
            
            # Generate summary
            summary = f"Study: {study}\n"
            summary += f"Total bins: {total_bins}\n"
            summary += f"Successfully reconciled bins: {successfully_reconciled}\n"
            summary += f"Bins needing reconciliation: {len(reconciliation_needed)}\n"
            summary += f"Reconciliation success rate: {(successfully_reconciled / total_bins) * 100:.2f}%\n"
            summary += f"Total events after reconciliation: {len(final_events)}\n\n"
            
            with open(os.path.join(study_path, 'summary_v2.txt'), 'w') as f:
                f.write(summary)
            
            # Generate reconciliation file
            with open(os.path.join(study_path, 'reconciliation_needed_v2.txt'), 'w') as f:
                for bin_time in reconciliation_needed:
                    f.write(f"{bin_time.strftime('%H:%M:%S')}\n")
            
            print(f"Processed study: {study}")

# Usage
data_path = 'data_all'
process_all_studies(data_path)