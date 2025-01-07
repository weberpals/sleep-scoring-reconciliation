import pandas as pd
import glob
import os
import seaborn as sns
import matplotlib.pyplot as plt

def analyze_staging_reconciliation(subject_id):
    """Analyze staging reconciliation for a given subject"""
    # Adjust path to match actual file location
    stage_file = f'../../output/staging_annotation/{subject_id}_stage_annotations.csv'
    if not os.path.exists(stage_file):
        print(f"File not found: {stage_file}")  # Debug print
        return None
    
    df = pd.read_csv(stage_file, delimiter='\t')
    
    # Count total epochs and epochs needing review
    total_epochs = len(df)
    needs_review = (df['Description'] == 'Stage: -').sum()
    auto_reconciled = total_epochs - needs_review
    
    # Analyze reconciliation by sleep stage
    stage_stats = {}
    for stage in ['Wake', 'N1', 'N2', 'N3', 'Rem']:
        stage_epochs = df[df['Description'] == f'Stage: {stage}']
        if len(stage_epochs) > 0:
            stage_stats[f'{stage}_epochs'] = len(stage_epochs)
            stage_stats[f'{stage}_pct'] = (len(stage_epochs) / total_epochs) * 100
    
    results = {
        'Total_Epochs': total_epochs,
        'Needs_Review': needs_review,
        'Auto_Reconciled': auto_reconciled,
        'Auto_Rate': (auto_reconciled/total_epochs*100) if total_epochs > 0 else 0,
        **stage_stats
    }
    
    # Debug print
    print(f"\nAnalyzing {subject_id}:")
    print(f"Total epochs: {total_epochs}")
    print(f"Needs review: {needs_review}")
    print(f"Auto reconciled: {auto_reconciled}")
    print(f"Stage stats: {stage_stats}")
    
    return results

def analyze_reconciliation_files():
    # Load demographics data
    demographics_df = pd.read_csv('../../output/demographics.csv')
    
    # Analyze both event and staging reconciliation
    directories = {
        'Flow': '../../output/flow_reconciliation_output/*.csv',
        'Arousal': '../../output/arousal_reconciliation_output/*.csv',
        'Staging': '../../output/staging_annotation/*_stage_annotations.csv'
    }
    
    for data_type, path_pattern in directories.items():
        csv_files = glob.glob(path_pattern)
        demographic_data = []
        
        print(f"\n{data_type.upper()} RECONCILIATION ANALYSIS")
        print("=" * 30)
        
        for file in csv_files:
            subject_id = os.path.basename(file).split('_')[0]
            
            if data_type == 'Staging':
                results = analyze_staging_reconciliation(subject_id)
            else:
                # Event reconciliation analysis
                df = pd.read_csv(file, delimiter='\t')
                review_mask = df['Description'].str.contains('Review: ', na=False)
                results = {
                    'Total_Events': len(df),
                    'Needs_Review': review_mask.sum(),
                    'Auto_Reconciled': (~review_mask).sum(),
                    'Auto_Rate': ((~review_mask).sum()/len(df)*100) if len(df) > 0 else 0
                }
            
            if results:
                # Get demographic information
                subject_demographics = demographics_df[demographics_df['ID'] == subject_id].iloc[0] if len(demographics_df[demographics_df['ID'] == subject_id]) > 0 else None
                
                if subject_demographics is not None:
                    demographic_data.append({
                        'ID': subject_id,
                        'Age': subject_demographics['Age during study'],
                        'Sex': subject_demographics['Sex'],
                        'Race': subject_demographics['Race'],
                        'Ethnicity': subject_demographics['Ethnicity'],
                        **results
                    })
                    
        
        if demographic_data:
            demo_df = pd.DataFrame(demographic_data)
            
            # Create visualizations
            plt.figure(figsize=(15, 10))
            
            # 1. Auto-reconciliation rate distribution
            plt.subplot(2, 2, 1)
            sns.histplot(data=demo_df, x='Auto_Rate', bins=20)
            plt.title(f'{data_type}: Auto-reconciliation Rate Distribution')
            
            # 2. Auto-reconciliation rate by age and sex
            plt.subplot(2, 2, 2)
            sns.scatterplot(data=demo_df, x='Age', y='Auto_Rate', 
                          hue='Sex', style='Sex', s=100)
            plt.title(f'{data_type}: Auto-reconciliation by Age and Sex')
            
            # 3. Auto-reconciliation rate by race
            plt.subplot(2, 2, 3)
            sns.boxplot(data=demo_df, x='Race', y='Auto_Rate')
            plt.xticks(rotation=45, ha='right')
            plt.title(f'{data_type}: Auto-reconciliation by Race')
            
            # 4. For staging data: reconciliation rate by sleep stage
            if data_type == 'Staging':
                plt.subplot(2, 2, 4)
                stage_data = demo_df[['Wake_pct', 'N1_pct', 'N2_pct', 'N3_pct', 'Rem_pct']].mean()
                plt.pie(stage_data, labels=stage_data.index, autopct='%1.1f%%')
                plt.title('Sleep Stage Distribution')
            
            plt.tight_layout()
            plt.savefig(f'{data_type.lower()}_reconciliation_analysis.png')
            plt.close()
            
            # Statistical analysis
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

if __name__ == "__main__":
    analyze_reconciliation_files()
