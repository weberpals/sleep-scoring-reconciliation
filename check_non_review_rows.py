import os
import csv
import logging
from collections import defaultdict

def check_non_review_rows(directory):
    results = defaultdict(lambda: {'total': 0, 'non_review': 0})
    
    for filename in os.listdir(directory):
        if filename.endswith('.csv'):
            file_path = os.path.join(directory, filename)
            try:
                with open(file_path, 'r', newline='') as tsvfile:
                    reader = csv.reader(tsvfile, delimiter='\t')
                    next(reader)  # Skip header
                    for row_num, row in enumerate(reader, start=2):  # Start at 2 to account for header
                        results[filename]['total'] += 1
                        if len(row) >= 3:
                            if 'Review' not in row[2]:
                                results[filename]['non_review'] += 1
                        else:
                            logging.warning(f"Row {row_num} in {filename} has fewer than 3 columns")
            except Exception as e:
                logging.error(f"Error processing {filename}: {str(e)}")
    
    return results

def print_results(results):
    for filename, counts in results.items():
        total = counts['total']
        non_review = counts['non_review']
        share = (non_review / total) * 100 if total > 0 else 0
        print(f"{filename}: {share:.2f}% non-review rows ({non_review}/{total})")

def main():
    directories = [
        'output/event_reconciliation_output',
        'output/flow_reconciliation_output'
    ]
    
    for directory in directories:
        print(f"\nChecking files in {directory}:")
        results = check_non_review_rows(directory)
        print_results(results)

if __name__ == "__main__":
    main()
