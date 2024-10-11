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
        current = start.replace(microsecond=0)
        end = end.replace(microsecond=0)
        while current < end:
            scored_bins[current] = 1
            current += timedelta(seconds=1)
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
        
        # Check if the previous and following bins exist and have a score of 3
        prev_bin_time = bin_time - timedelta(seconds=1)
        next_bin_time = bin_time + timedelta(seconds=1)

        def is_unanimous(bin_time):
            return sum(scored_bins[scorer].get(bin_time, 0) for scorer in scorers) == 3

        prev_bin_unanimous = prev_bin_time in all_bins and is_unanimous(prev_bin_time)
        next_bin_unanimous = next_bin_time in all_bins and is_unanimous(next_bin_time)

        # Determine if reconciliation is needed based on score count and neighboring bins
        if score_count == 0:
            # No scorers marked this bin, consider it successfully reconciled
            successfully_reconciled += 1
            if current_event:
                final_events.append(current_event)
                current_event = None
        elif score_count == 1:
            # Only one scorer marked this bin
            if prev_bin_unanimous or next_bin_unanimous:
                # If either neighboring bin is unanimous, consider this bin reconciled
                successfully_reconciled += 1
            else:
                # Otherwise, manual reconciliation is needed
                reconciliation_needed.append(bin_time)
            if current_event:
                final_events.append(current_event)
                current_event = None
        elif score_count == 2:
            # Two scorers marked this bin
            if prev_bin_unanimous or next_bin_unanimous:
                # If either neighboring bin is unanimous, consider this bin part of an event
                successfully_reconciled += 1
                if not current_event:
                    current_event = (bin_time, bin_time)
                else:
                    current_event = (current_event[0], bin_time)
            else:
                # Otherwise, manual reconciliation is needed
                reconciliation_needed.append(bin_time)
        elif score_count == 3:
            # All scorers agree, definitely part of an event
            successfully_reconciled += 1
            if not current_event:
                current_event = (bin_time, bin_time)
            else:
                current_event = (current_event[0], bin_time)
    
    if current_event:
        final_events.append(current_event)
    
    return final_events, reconciliation_needed, successfully_reconciled, total_bins

def process_all_studies(data_path):
    study_summaries = []
    
    for study in os.listdir(data_path):
        study_path = os.path.join(data_path, study)
        if os.path.isdir(study_path):
            final_events, reconciliation_needed, successfully_reconciled, total_bins = reconcile_study(study_path)
            
            # Generate summary
            summary = {
                "Study": study,
                "Total Bins": total_bins,
                "Successfully Reconciled": successfully_reconciled,
                "Needing Reconciliation": len(reconciliation_needed),
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
    ax.bar(x - 0.5*width, [s["Total Bins"] for s in study_summaries], width, label='Total Bins', color='#89CFF0')  
    successfully_reconciled = ax.bar(x + 0.5*width, [s["Successfully Reconciled"] for s in study_summaries], width, label='Successfully Reconciled', color='#b988e6')  
    ax.bar(x + 1.5*width, [s["Needing Reconciliation"] for s in study_summaries], width, label='Needing Reconciliation', color='#e74c3c')  
    
    ax.set_ylabel('Count')
    ax.set_title('Study Summary Dashboard')
    ax.set_xticks(x)
    ax.set_xticklabels(studies)
    ax.legend()

    # Add text for reconciliation success rates
    for i, summary in enumerate(study_summaries):
        percentage = summary["Reconciliation Success Rate"]
        bar_height = successfully_reconciled[i].get_height()
        ax.text(x[i] + 0.5*width, bar_height + 5, f'{percentage:.2f}%', 
                ha='center', va='bottom', rotation=90)

    plt.tight_layout()
    plt.savefig('study_summary_v2.png')
    plt.close()

    print("Graphical summary saved as 'study_summary.png'")

# Usage
data_path = '../data_all'
process_all_studies(data_path)