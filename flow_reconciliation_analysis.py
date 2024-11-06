import pandas as pd
import glob
import os

def analyze_reconciliation_files():
    # Get all CSV files in the flow_reconciliation_output directory
    csv_files = glob.glob('output/flow_reconciliation_output/*.csv')
    
    total_events = 0
    needs_review = 0
    auto_reconciled = 0
    
    for file in csv_files:
        # Read CSV file
        df = pd.read_csv(file, delimiter='\t')
        
        # Count events that need review (contain "Review: " in Description)
        review_mask = df['Description'].str.contains('Review: ', na=False)
        file_needs_review = review_mask.sum()
        
        # Count automatically reconciled events (don't contain "Review: ")
        file_auto_reconciled = (~review_mask).sum()
        
        # Update totals
        needs_review += file_needs_review
        auto_reconciled += file_auto_reconciled
        total_events += len(df)
        
        # Print per-file statistics
        print(f"\nFile: {os.path.basename(file)}")
        print(f"Total events: {len(df)}")
        print(f"Needs review: {file_needs_review}")
        print(f"Auto-reconciled: {file_auto_reconciled}")
        print(f"Auto-reconciliation rate: {(file_auto_reconciled/len(df)*100):.1f}%")
    
    # Print overall statistics
    print("\nOVERALL STATISTICS")
    print("=" * 20)
    print(f"Total events across all files: {total_events}")
    print(f"Total events needing review: {needs_review}")
    print(f"Total auto-reconciled events: {auto_reconciled}")
    print(f"Overall auto-reconciliation rate: {(auto_reconciled/total_events*100):.1f}%")

if __name__ == "__main__":
    analyze_reconciliation_files()
