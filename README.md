# Sleep Scoring Reconciliation

A Python implementation for automated reconciliation of sleep scoring data from multiple scorers, supporting flow events, arousals, and sleep staging.

## Overview

This tool automates the initial reconciliation of sleep scoring data by:
1. Comparing scores from multiple technicians
2. Auto-reconciling clear agreements
3. Flagging discrepancies for manual review
4. Generating analysis reports and visualizations

## Project Structure
```
.
├── src/
│ ├── analysis/
│ │ └── event_reconciliation_analysis.py
│ ├── reconciliation/
│ │ ├── arousal.py
│ │ ├── flow.py
│ │ └── staging.py
│ └── utils/
│ ├── add_stage_numbers.py
│ ├── combine_events.py
│ └── merge_staging_events.py
├── output/
│ ├── flow_reconciliation_output/
│ ├── arousal_reconciliation_output/
│ ├── staging_annotation/
│ ├── combined/
│ └── merged/
├── pyproject.toml
├── README.md
└── .gitignore
```

## Features

- **Flow Event Reconciliation**: Automatically reconciles flow events marked by multiple scorers
- **Arousal Event Reconciliation**: Processes arousal events and identifies agreements/discrepancies
- **Sleep Stage Reconciliation**: Compares sleep staging between scorers
- **Analysis Tools**: Generates statistical analysis and visualizations of reconciliation results
- **Demographics Integration**: Analyzes reconciliation patterns across demographic groups

## Installation

1. Ensure you have Python 3.12+ installed
2. Poetry for dependency management
3. Clone the repository:

## Usage

1. **Prepare Data Structure**
   ```
   data_all/
   ├── STUDY_ID/
   │   ├── ES/
   │   │   ├── Flow Events.txt
   │   │   ├── Classification Arousals.txt
   │   │   └── Markers.txt
   │   ├── LS/
   │   └── MS/
   ```

2. **Run Reconciliation**
   ```bash
   poetry run python src/reconciliation/flow.py
   poetry run python src/reconciliation/arousal.py
   poetry run python src/reconciliation/staging.py
   ```



3. **Merge into one Annotation file**


4. **Generate Analysis**
   ```bash
   poetry run python src/analysis/event_reconciliation_analysis.py
   ```


## Dependencies

- Python ≥3.12
- pandas ≥2.2.3
- seaborn ≥0.13.2
- matplotlib ≥3.10.0