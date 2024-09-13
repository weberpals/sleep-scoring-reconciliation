import os
from matplotlib import pyplot as plt
import numpy as np
import pandas as pd
from datetime import datetime, timedelta

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
                
                start = datetime.strptime(start, '%H:%M:%S,%f')
                end = datetime.strptime(end, '%H:%M:%S,%f')
                
                # Adjust start and end times based on the start_time date
                start = start.replace(year=start_time.year, month=start_time.month, day=start_time.day)
                end = end.replace(year=start_time.year, month=start_time.month, day=start_time.day)
                
                # Handle events that cross midnight
                if end < start:
                    end += timedelta(days=1)
                
                events.append((start, end, event_type.strip()))
    return events, start_time
 
def create_bins(events, bin_size=timedelta(seconds=3)):
    all_bins = set()
    for start, end, _ in events:
        current = start
        while current < end:
            all_bins.add(current)
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
    initial_event_count = 0
    
    # Load events from all scorers
    for scorer in scorers:
        file_path = os.path.join(study_path, scorer, 'Flow Events.txt')
        events, start_time = parse_event_file(file_path)
        all_events[scorer] = events
        initial_event_count += len(events)
        if study_start_time is None or start_time < study_start_time:
            study_start_time = start_time
    
    # Create bins and score them
    all_bins = create_bins([event for scorer_events in all_events.values() for event in scorer_events])
    scored_bins = {scorer: {} for scorer in scorers}
    for scorer, events in all_events.items():
        for start, end, event_type in events:
            current = start
            while current < end and current in all_bins:
                if current not in scored_bins[scorer]:
                    scored_bins[scorer][current] = set()
                scored_bins[scorer][current].add(event_type)
                current += timedelta(seconds=3)
    
    reconciliation_needed = []
    final_events = []
    current_event = None
    successfully_reconciled = 0
    total_bins = len(all_bins)
    
    def check_unanimous(bin_time):
        event_types = set.intersection(*[scored_bins[scorer].get(bin_time, set()) for scorer in scorers])
        return len(event_types) == 1 and all(bin_time in scored_bins[scorer] for scorer in scorers)

    def get_most_common_type(bin_time):
        all_types = [type for scorer in scorers for type in scored_bins[scorer].get(bin_time, set())]
        return max(set(all_types), key=all_types.count) if all_types else None

    for i, bin_time in enumerate(all_bins):
        # Get all event types marked for this bin
        all_types = set.union(*[scored_bins[scorer].get(bin_time, set()) for scorer in scorers])
        score_count = sum(bool(scored_bins[scorer].get(bin_time)) for scorer in scorers)
        
        # Check if the previous or next bin was unanimously scored with the same event type
        prev_bin_time = all_bins[i-1] if i > 0 else None
        next_bin_time = all_bins[i+1] if i < len(all_bins) - 1 else None
        contiguous_unanimous = (prev_bin_time and check_unanimous(prev_bin_time)) or (next_bin_time and check_unanimous(next_bin_time))
        
        if score_count == 0:
            # No scorer marked this bin as an event
            successfully_reconciled += 1
            if current_event:
                final_events.append(current_event)
                current_event = None
        elif score_count == 1:
            # Only one scorer marked this bin
            if contiguous_unanimous:
                # If contiguous with a unanimous bin, consider this reconciled
                successfully_reconciled += 1
            else:
                # Otherwise, this needs reconciliation
                reconciliation_needed.append(bin_time)
            if current_event:
                final_events.append(current_event)
                current_event = None
        elif score_count == 2:
            # Two scorers marked this bin
            if contiguous_unanimous:
                # If contiguous with a unanimous bin, include this in the current event
                event_type = get_most_common_type(prev_bin_time or next_bin_time)
                successfully_reconciled += 1
                if not current_event:
                    current_event = (bin_time, bin_time, event_type)
                else:
                    current_event = (current_event[0], bin_time, event_type)
            else:
                # Otherwise, this needs reconciliation
                reconciliation_needed.append(bin_time)
        elif score_count == 3 and len(all_types) == 1:
            # All scorers agree on the same event type, include in current event
            event_type = next(iter(all_types))
            successfully_reconciled += 1
            if not current_event:
                current_event = (bin_time, bin_time, event_type)
            else:
                current_event = (current_event[0], bin_time, event_type)
        else:
            # All scorers marked an event, but they disagree on the type
            reconciliation_needed.append(bin_time)
    
    if current_event:
        final_events.append(current_event)
    
    return final_events, reconciliation_needed, successfully_reconciled, total_bins, initial_event_count

def process_all_studies(data_path):
    study_summaries = []
    
    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            final_events, reconciliation_needed, successfully_reconciled, total_bins, initial_event_count = reconcile_study(study_path)
            
            # Generate summary
            summary = {
                "Study": study,
                "Initial Events": initial_event_count,
                "Total Bins": total_bins,
                "Successfully Reconciled": successfully_reconciled,
                "Needing Reconciliation": len(reconciliation_needed),
                "Final Events": len(final_events),
                "Reconciliation Success Rate": (successfully_reconciled / total_bins) * 100
            }
            
            study_summaries.append(summary)
            
            print(f"Processed study: {study}")
    
    # Create graphical summary
    create_summary_graph(study_summaries)

def create_summary_graph(study_summaries):
    studies = [summary["Study"] for summary in study_summaries]
    x = np.arange(len(studies))
    width = 0.15

    fig, ax = plt.subplots(figsize=(15, 10))

    # Plot bars for each metric
    ax.bar(x - 2*width, [s["Initial Events"] for s in study_summaries], width, label='Initial Events', color='#8884d8')
    ax.bar(x - width, [s["Total Bins"] for s in study_summaries], width, label='Total Bins', color='#82ca9d')
    ax.bar(x, [s["Successfully Reconciled"] for s in study_summaries], width, label='Successfully Reconciled', color='#ffc658')
    ax.bar(x + width, [s["Needing Reconciliation"] for s in study_summaries], width, label='Needing Reconciliation', color='#ff8042')
    ax.bar(x + 2*width, [s["Final Events"] for s in study_summaries], width, label='Final Events', color='#a4de6c')

    ax.set_ylabel('Count')
    ax.set_title('Study Summary Dashboard')
    ax.set_xticks(x)
    ax.set_xticklabels(studies)
    ax.legend()

    # Add text for reconciliation success rates
    for i, summary in enumerate(study_summaries):
        ax.text(i, 10, f'{summary["Reconciliation Success Rate"]:.2f}%', ha='center', va='bottom', rotation=90)

    plt.tight_layout()
    plt.savefig('study_summary.png')
    plt.close()

    print("Graphical summary saved as 'study_summary.png'")

# Usage
data_path = '../data_all'
process_all_studies(data_path)