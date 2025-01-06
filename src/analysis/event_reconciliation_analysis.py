import pandas as pd
import glob
import os

def analyze_reconciliation_files():
    # Load demographics data
    demographics_df = pd.read_csv('../../output/demographics.csv')
    
    # Analyze both flow and arousal directories
    directories = {
        'Flow': '../../output/flow_reconciliation_output/*.csv',
        'Arousal': '../../output/arousal_reconciliation_output/*.csv'
    }
    
    for data_type, path_pattern in directories.items():
        # Get all CSV files in the directory
        csv_files = glob.glob(path_pattern)
        
        total_events = 0
        needs_review = 0
        auto_reconciled = 0
        
        print(f"\n{data_type.upper()} RECONCILIATION ANALYSIS")
        print("=" * 30)
        
        # Create lists to store demographic-specific data
        demographic_data = []
        
        for file in csv_files:
            # Extract subject ID from filename (assumes format like "AWV002_flow_reconciliation.csv")
            subject_id = os.path.basename(file).split('_')[0]
            
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
            
            # Get demographic information for this subject
            subject_demographics = demographics_df[demographics_df['ID'] == subject_id].iloc[0] if len(demographics_df[demographics_df['ID'] == subject_id]) > 0 else None
            
            if subject_demographics is not None:
                demographic_data.append({
                    'ID': subject_id,
                    'Age': subject_demographics['Age during study'],
                    'Sex': subject_demographics['Sex'],
                    'Race': subject_demographics['Race'],
                    'Ethnicity': subject_demographics['Ethnicity'],
                    'Total_Events': len(df),
                    'Needs_Review': file_needs_review,
                    'Auto_Reconciled': file_auto_reconciled,
                    'Auto_Rate': (file_auto_reconciled/len(df)*100) if len(df) > 0 else 0
                })
        
        # Analyze demographics after processing all files
        if demographic_data:
            demo_df = pd.DataFrame(demographic_data)
            
            print(f"\n{data_type.upper()} DEMOGRAPHIC ANALYSIS")
            print("=" * 30)
            
            # Analysis by Sex
            print("\nAnalysis by Sex:")
            sex_stats = demo_df.groupby('Sex').agg({
                'Auto_Rate': ['mean', 'std', 'count']
            })['Auto_Rate']
            print(sex_stats)
            
            # Analysis by Age Groups
            demo_df['Age_Group'] = pd.cut(demo_df['Age'], 
                                        bins=[0, 18, 35, 50, 65, 100],
                                        labels=['<18', '18-35', '36-50', '51-65', '>65'])
            print("\nAnalysis by Age Group:")
            age_stats = demo_df.groupby('Age_Group').agg({
                'Auto_Rate': ['mean', 'std', 'count']
            })['Auto_Rate']
            print(age_stats)
            
            # Analysis by Race
            print("\nAnalysis by Race:")
            race_stats = demo_df.groupby('Race').agg({
                'Auto_Rate': ['mean', 'std', 'count']
            })['Auto_Rate']
            print(race_stats)
            
            # Analysis by Ethnicity
            print("\nAnalysis by Ethnicity:")
            ethnicity_stats = demo_df.groupby('Ethnicity').agg({
                'Auto_Rate': ['mean', 'std', 'count']
            })['Auto_Rate']
            print(ethnicity_stats)

if __name__ == "__main__":
    analyze_reconciliation_files()
