import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path
import re
from collections import Counter
import numpy as np
import seaborn as sns
from datetime import datetime, timedelta

def analyze_final_annotations(filename):
    """Analyze final reconciled annotations with focus on stage analytics"""
    try:
        df = pd.read_csv(filename)
        df['Onset'] = pd.to_datetime(df['Onset'])
        
        # Basic statistics
        stats = {
            'total_annotations': len(df),
            'duration_hours': (df['Onset'].max() - df['Onset'].min()).total_seconds() / 3600,
            'unique_annotation_types': df['Annotation'].nunique(),
            'start_time': df['Onset'].min(),
            'end_time': df['Onset'].max()
        }
        
        # Event type distribution
        event_distribution = df['Annotation'].value_counts()
        
        # Enhanced stage analysis (excluding artifacts)
        stage_df = df[df['Annotation'].str.contains('Stage:', na=False)]
        stage_analysis = analyze_stages(stage_df)
        
        return stats, event_distribution, stage_analysis, df
        
    except Exception as e:
        print(f"Error analyzing {filename}: {e}")
        return None, None, None, None

def analyze_stages(stage_df):
    """Detailed analysis of sleep stages excluding artifacts"""
    if stage_df.empty:
        return {}
    
    # Extract stage names
    stage_df = stage_df.copy()
    stage_df['stage'] = stage_df['Annotation'].str.extract(r'Stage: (.+)')
    
    # Standardize stage names
    stage_df['stage'] = stage_df['stage'].apply(standardize_stage_name)
    
    # Filter out artifacts
    stage_df = stage_df[~stage_df['stage'].isin(['Artifact'])]
    
    # Stage distribution
    stage_counts = stage_df['stage'].value_counts()
    
    # Calculate percentages
    total_epochs = len(stage_df)
    stage_percentages = (stage_counts / total_epochs * 100).round(2)
    
    return {
        'stage_counts': stage_counts,
        'stage_percentages': stage_percentages,
        'total_epochs': total_epochs
    }

def load_merged_data(filename):
    """Load and analyze merged/auto reconciled data"""
    try:
        df = pd.read_csv(filename)
        df['Onset'] = pd.to_datetime(df['Onset'])
        
        # Filter for stage annotations
        stage_df = df[df['Description'].str.contains('Stage:', na=False)]
        stage_df = stage_df.copy()
        stage_df['stage'] = stage_df['Description'].str.extract(r'Stage: (.+)')
        
        # Standardize stage names
        stage_df['stage'] = stage_df['stage'].apply(standardize_stage_name)
        
        # Filter out artifacts
        stage_df = stage_df[~stage_df['stage'].isin(['Artifact'])]
        
        return stage_df
    except Exception as e:
        print(f"Error loading merged data {filename}: {e}")
        return None

def parse_ai_timestamp(time_str, date_from_final):
    """Parse AI scored timestamp and align with final data date"""
    try:
        # Parse time (format: HH:MM:SS,mmm)
        time_part = time_str.split(',')[0]  # Remove milliseconds
        time_obj = datetime.strptime(time_part, '%H:%M:%S').time()
        
        # Combine with date from final data
        full_datetime = datetime.combine(date_from_final.date(), time_obj)
        
        # Handle day rollover (if time goes past midnight)
        if full_datetime < date_from_final:
            full_datetime += timedelta(days=1)
            
        return full_datetime
    except:
        return None

def load_ai_scored_data(filename, final_start_time=None):
    """Load and parse AI scored data with timestamp alignment"""
    try:
        with open(filename, 'r') as f:
            lines = f.readlines()
        
        # Find start of data (after header)
        data_start = 0
        for i, line in enumerate(lines):
            if 'Rate:' in line:
                data_start = i + 2  # Skip the Rate line and empty line
                break
        
        # Parse data
        data = []
        for line in lines[data_start:]:
            line = line.strip()
            if not line:
                continue
                
            parts = line.split(';')
            if len(parts) >= 2:
                time_str = parts[0].strip()
                stage = parts[1].strip()
                
                # Parse timestamp if final_start_time is provided
                parsed_time = None
                if final_start_time:
                    parsed_time = parse_ai_timestamp(time_str, final_start_time)
                
                data.append({
                    'time_str': time_str, 
                    'stage': stage,
                    'parsed_time': parsed_time
                })
        
        df = pd.DataFrame(data)
        
        # Map German stage names to English with consistent formatting
        stage_mapping = {
            'Wach': 'Wake',
            'Stadium 1': 'N1', 
            'Stadium 2': 'N2',
            'Stadium 3': 'N3',
            'Rem': 'REM',
            'REM': 'REM',  # Handle case where it's already in English
            'Wake': 'Wake',
            'N1': 'N1',
            'N2': 'N2', 
            'N3': 'N3'
        }
        
        df['stage_mapped'] = df['stage'].map(stage_mapping).fillna(df['stage'])
        
        # Filter out artifacts
        df = df[~df['stage_mapped'].isin(['Artefakt', 'Artifact', 'A'])]
        
        return df
    except Exception as e:
        print(f"Error loading AI scored data {filename}: {e}")
        return None

def standardize_stage_name(stage):
    """Standardize stage names to consistent format"""
    if pd.isna(stage):
        return stage
    
    stage_str = str(stage).strip()
    
    # Standardize common variations
    standardization_map = {
        'rem': 'REM',
        'Rem': 'REM', 
        'REM': 'REM',
        'wake': 'Wake',
        'Wake': 'Wake',
        'WAKE': 'Wake',
        'n1': 'N1',
        'N1': 'N1',
        'stage 1': 'N1',
        'Stage 1': 'N1',
        'n2': 'N2',
        'N2': 'N2', 
        'stage 2': 'N2',
        'Stage 2': 'N2',
        'n3': 'N3',
        'N3': 'N3',
        'stage 3': 'N3',
        'Stage 3': 'N3',
        'artifact': 'Artifact',
        'Artifact': 'Artifact',
        'artefakt': 'Artifact',
        'Artefakt': 'Artifact',
        'A': 'Artifact'
    }
    
    return standardization_map.get(stage_str, stage_str)

def align_and_compare_ai_final(final_df, ai_df):
    """Align AI scored data with final reconciled data and compare stages"""
    if final_df is None or ai_df is None:
        return {}
    
    # Get stage data from final reconciled
    final_stage_df = final_df[final_df['Annotation'].str.contains('Stage:', na=False)].copy()
    final_stage_df['stage'] = final_stage_df['Annotation'].str.extract(r'Stage: (.+)')[0]
    
    # Standardize stage names in final data
    final_stage_df['stage'] = final_stage_df['stage'].apply(standardize_stage_name)
    final_stage_df = final_stage_df[~final_stage_df['stage'].isin(['Artifact'])]
    
    # Standardize stage names in AI data
    ai_df = ai_df.copy()
    ai_df['stage_mapped'] = ai_df['stage_mapped'].apply(standardize_stage_name)
    ai_df = ai_df[~ai_df['stage_mapped'].isin(['Artifact'])]
    
    # If AI data has parsed timestamps, try to align temporally
    if 'parsed_time' in ai_df.columns and ai_df['parsed_time'].notna().any():
        return compare_with_temporal_alignment(final_stage_df, ai_df)
    else:
        return compare_epoch_by_epoch(final_stage_df, ai_df)

def compare_with_temporal_alignment(final_stage_df, ai_df):
    """Compare stages using temporal alignment"""
    try:
        # Create 30-second epochs for both datasets
        final_epochs = []
        ai_epochs = []
        
        # Process final data - already in 30s epochs
        for _, row in final_stage_df.iterrows():
            final_epochs.append({
                'timestamp': row['Onset'],
                'stage': row['stage']
            })
        
        # Process AI data - align to 30s epochs
        ai_df_clean = ai_df[ai_df['parsed_time'].notna()].copy()
        for _, row in ai_df_clean.iterrows():
            ai_epochs.append({
                'timestamp': row['parsed_time'],
                'stage': row['stage_mapped']
            })
        
        # Find overlapping time range
        if not final_epochs or not ai_epochs:
            return {}
            
        final_start = min(e['timestamp'] for e in final_epochs)
        final_end = max(e['timestamp'] for e in final_epochs)
        ai_start = min(e['timestamp'] for e in ai_epochs)
        ai_end = max(e['timestamp'] for e in ai_epochs)
        
        overlap_start = max(final_start, ai_start)
        overlap_end = min(final_end, ai_end)
        
        if overlap_start >= overlap_end:
            return {'error': 'No temporal overlap found'}
        
        # Align epochs within overlap period
        aligned_comparisons = []
        tolerance = timedelta(seconds=15)  # Allow 15s tolerance for alignment
        
        for final_epoch in final_epochs:
            if overlap_start <= final_epoch['timestamp'] <= overlap_end:
                # Find closest AI epoch
                closest_ai = None
                min_diff = timedelta.max
                
                for ai_epoch in ai_epochs:
                    if overlap_start <= ai_epoch['timestamp'] <= overlap_end:
                        diff = abs(final_epoch['timestamp'] - ai_epoch['timestamp'])
                        if diff < min_diff and diff <= tolerance:
                            min_diff = diff
                            closest_ai = ai_epoch
                
                if closest_ai:
                    aligned_comparisons.append({
                        'final_stage': final_epoch['stage'],
                        'ai_stage': closest_ai['stage'],
                        'timestamp': final_epoch['timestamp']
                    })
        
        return analyze_stage_differences(aligned_comparisons, method='temporal')
        
    except Exception as e:
        print(f"Error in temporal alignment: {e}")
        return compare_epoch_by_epoch(final_stage_df, ai_df)

def compare_epoch_by_epoch(final_stage_df, ai_df):
    """Compare stages epoch by epoch when temporal alignment isn't possible"""
    final_stages = final_stage_df['stage'].values
    ai_stages = ai_df['stage_mapped'].values
    
    min_length = min(len(final_stages), len(ai_stages))
    if min_length == 0:
        return {}
    
    comparisons = []
    for i in range(min_length):
        comparisons.append({
            'final_stage': final_stages[i],
            'ai_stage': ai_stages[i],
            'epoch': i
        })
    
    return analyze_stage_differences(comparisons, method='epoch')

def analyze_stage_differences(comparisons, method='temporal'):
    """Analyze differences between AI and final reconciled stages"""
    if not comparisons:
        return {}
    
    total_comparisons = len(comparisons)
    differences = []
    agreement_count = 0
    
    for comp in comparisons:
        final_stage = comp['final_stage']
        ai_stage = comp['ai_stage']
        
        if final_stage == ai_stage:
            agreement_count += 1
        else:
            differences.append({
                'final_stage': final_stage,
                'ai_stage': ai_stage,
                'change_type': f"AI:{ai_stage} -> Final:{final_stage}"
            })
    
    # Calculate statistics
    agreement_percentage = (agreement_count / total_comparisons * 100) if total_comparisons > 0 else 0
    disagreement_count = len(differences)
    disagreement_percentage = (disagreement_count / total_comparisons * 100) if total_comparisons > 0 else 0
    
    # Analyze disagreement patterns
    ai_stage_errors = Counter([d['ai_stage'] for d in differences])
    final_stage_corrections = Counter([d['final_stage'] for d in differences])
    change_types = Counter([d['change_type'] for d in differences])
    
    # Stage-specific disagreement rates
    all_stages = set()
    for comp in comparisons:
        all_stages.add(comp['final_stage'])
        all_stages.add(comp['ai_stage'])
    
    stage_disagreements = {}
    for stage in all_stages:
        stage_total = sum(1 for c in comparisons if c['ai_stage'] == stage)
        stage_errors = sum(1 for d in differences if d['ai_stage'] == stage)
        if stage_total > 0:
            stage_disagreements[stage] = {
                'total_ai_scored': stage_total,
                'disagreements': stage_errors,
                'error_rate': (stage_errors / stage_total * 100)
            }
    
    return {
        'total_comparisons': total_comparisons,
        'agreement_count': agreement_count,
        'disagreement_count': disagreement_count,
        'agreement_percentage': agreement_percentage,
        'disagreement_percentage': disagreement_percentage,
        'ai_stage_errors': dict(ai_stage_errors),
        'final_stage_corrections': dict(final_stage_corrections),
        'change_types': dict(change_types),
        'stage_disagreements': stage_disagreements,
        'alignment_method': method
    }

def compare_stage_distributions(final_analysis, merged_df, ai_df, subject_id):
    """Compare stage distributions across all three datasets"""
    comparison = {
        'subject_id': subject_id,
        'final_reconciled': {},
        'merged': {},
        'ai_scored': {}
    }
    
    # Final reconciled data
    if final_analysis and 'stage_counts' in final_analysis:
        comparison['final_reconciled'] = final_analysis['stage_counts'].to_dict()
    
    # Merged data
    if merged_df is not None and not merged_df.empty:
        merged_counts = merged_df['stage'].value_counts()
        comparison['merged'] = merged_counts.to_dict()
    
    # AI scored data
    if ai_df is not None and not ai_df.empty:
        ai_counts = ai_df['stage_mapped'].value_counts()
        comparison['ai_scored'] = ai_counts.to_dict()
    
    return comparison

def analyze_reconciliation_changes(merged_df, final_df):
    """Analyze what changed between merged and final reconciled data"""
    if merged_df is None or final_df is None:
        return {}
    
    # Get stage data from both datasets
    merged_stages = merged_df['stage'].values
    final_stage_df = final_df[final_df['Annotation'].str.contains('Stage:', na=False)]
    final_stages = final_stage_df['Annotation'].str.extract(r'Stage: (.+)')[0].values
    
    # Standardize stage names
    final_stages_standardized = [standardize_stage_name(stage) for stage in final_stages]
    
    # Filter out artifacts
    final_stages_clean = []
    for stage in final_stages_standardized:
        if stage not in ['Artifact']:
            final_stages_clean.append(stage)
    final_stages = np.array(final_stages_clean)
    
    # Find changes
    changes = []
    min_length = min(len(merged_stages), len(final_stages))
    
    for i in range(min_length):
        if merged_stages[i] != final_stages[i]:
            changes.append({
                'epoch': i,
                'merged_stage': merged_stages[i],
                'final_stage': final_stages[i]
            })
    
    change_summary = {
        'total_changes': len(changes),
        'change_percentage': (len(changes) / min_length * 100) if min_length > 0 else 0,
        'change_types': Counter([f"{c['merged_stage']} -> {c['final_stage']}" for c in changes]),
        'changes_by_merged_stage': Counter([c['merged_stage'] for c in changes]),
        'changes_by_final_stage': Counter([c['final_stage'] for c in changes])
    }
    
    return change_summary

def plot_reconciliation_analysis(comparisons, output_dir='plots'):
    """Create plots focused on reconciliation impact"""
    Path(output_dir).mkdir(exist_ok=True)
    
    # Collect all unique stages (excluding artifacts)
    all_stages = set()
    for comp in comparisons:
        for dataset in ['final_reconciled', 'merged', 'ai_scored']:
            stages = set(comp[dataset].keys())
            stages.discard('Artifact')  # Only need to discard standardized 'Artifact'
            all_stages.update(stages)
    
    all_stages = sorted(list(all_stages))
    
    # Create comprehensive analysis plots
    fig, axes = plt.subplots(2, 2, figsize=(16, 12))
    fig.suptitle('Reconciliation Impact Analysis', fontsize=16)
    
    # Plot 1: Stage distribution comparison (Final vs AI Scored)
    datasets = ['final_reconciled', 'ai_scored']
    stage_data = {stage: {dataset: [] for dataset in datasets} for stage in all_stages}
    
    for comp in comparisons:
        for stage in all_stages:
            for dataset in datasets:
                count = comp[dataset].get(stage, 0)
                stage_data[stage][dataset].append(count)
    
    # Average across subjects
    avg_data = {}
    for stage in all_stages:
        avg_data[stage] = {}
        for dataset in datasets:
            values = stage_data[stage][dataset]
            avg_data[stage][dataset] = np.mean(values) if values else 0
    
    # Bar plot comparison
    x = np.arange(len(all_stages))
    width = 0.35
    
    ax = axes[0, 0]
    final_values = [avg_data[stage]['final_reconciled'] for stage in all_stages]
    ai_values = [avg_data[stage]['ai_scored'] for stage in all_stages]
    
    ax.bar(x - width/2, final_values, width, label='Final Reconciled', alpha=0.8)
    ax.bar(x + width/2, ai_values, width, label='AI Scored', alpha=0.8)
    
    ax.set_xlabel('Sleep Stages')
    ax.set_ylabel('Average Epoch Count')
    ax.set_title('Final Reconciled vs AI Scored')
    ax.set_xticks(x)
    ax.set_xticklabels(all_stages, rotation=45)
    ax.legend()
    
    # Plot 2: AI vs Final disagreement rates by stage
    ax = axes[0, 1]
    ai_disagreements = {}
    
    for comp in comparisons:
        if 'ai_final_comparison' in comp:
            stage_disagree = comp['ai_final_comparison'].get('stage_disagreements', {})
            for stage, data in stage_disagree.items():
                if stage not in ai_disagreements:
                    ai_disagreements[stage] = []
                ai_disagreements[stage].append(data['error_rate'])
    
    if ai_disagreements:
        stages = list(ai_disagreements.keys())
        avg_error_rates = [np.mean(ai_disagreements[stage]) for stage in stages]
        
        ax.bar(stages, avg_error_rates, alpha=0.7, color='red')
        ax.set_xlabel('Sleep Stages')
        ax.set_ylabel('Average AI Error Rate (%)')
        ax.set_title('AI vs Final: Disagreement by Stage')
        ax.tick_params(axis='x', rotation=45)
    else:
        ax.text(0.5, 0.5, 'No AI comparison data available', 
                ha='center', va='center', transform=ax.transAxes)
    
    # Plot 3: Reconciliation changes by stage
    ax = axes[1, 0]
    total_changes_by_stage = Counter()
    
    for comp in comparisons:
        if 'reconciliation_changes' in comp:
            changes = comp['reconciliation_changes']
            total_changes_by_stage.update(changes.get('changes_by_merged_stage', {}))
    
    if total_changes_by_stage:
        stages_changed = list(total_changes_by_stage.keys())
        change_counts = [total_changes_by_stage[stage] for stage in stages_changed]
        
        ax.bar(stages_changed, change_counts, alpha=0.7, color='orange')
        ax.set_xlabel('Original Stage (Merged)')
        ax.set_ylabel('Number of Changes')
        ax.set_title('Stages Most Affected by Reconciliation')
        ax.tick_params(axis='x', rotation=45)
    else:
        ax.text(0.5, 0.5, 'No reconciliation changes detected', 
                ha='center', va='center', transform=ax.transAxes)
    
    # Plot 4: Subject-level AI agreement rates
    ax = axes[1, 1]
    subjects = [comp['subject_id'] for comp in comparisons[:15]]  # Show first 15
    agreement_rates = []
    
    for comp in comparisons[:15]:
        if 'ai_final_comparison' in comp:
            agreement_pct = comp['ai_final_comparison'].get('agreement_percentage', 0)
        else:
            agreement_pct = 0
        agreement_rates.append(agreement_pct)
    
    if subjects and agreement_rates:
        ax.bar(range(len(subjects)), agreement_rates, alpha=0.7, color='green')
        ax.set_xlabel('Subjects')
        ax.set_ylabel('AI-Final Agreement (%)')
        ax.set_title('AI vs Final Agreement by Subject')
        ax.set_xticks(range(len(subjects)))
        ax.set_xticklabels(subjects, rotation=45)
        ax.set_ylim(0, 100)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/reconciliation_analysis.png', dpi=300, bbox_inches='tight')
    plt.close()

def plot_stage_comparison(comparisons, output_dir='plots'):
    """Create simplified stage distribution comparison"""
    Path(output_dir).mkdir(exist_ok=True)
    
    # Collect all unique stages (excluding artifacts)
    all_stages = set()
    for comp in comparisons:
        for dataset in ['final_reconciled', 'merged', 'ai_scored']:
            stages = set(comp[dataset].keys())
            stages.discard('Artifact')  # Only need to discard standardized 'Artifact'
            all_stages.update(stages)
    
    all_stages = sorted(list(all_stages))
    
    # Create comparison plot
    fig, ax = plt.subplots(1, 1, figsize=(12, 8))
    
    datasets = ['final_reconciled', 'merged', 'ai_scored']
    stage_data = {stage: {dataset: [] for dataset in datasets} for stage in all_stages}
    
    for comp in comparisons:
        for stage in all_stages:
            for dataset in datasets:
                count = comp[dataset].get(stage, 0)
                stage_data[stage][dataset].append(count)
    
    # Average across subjects
    avg_data = {}
    for stage in all_stages:
        avg_data[stage] = {}
        for dataset in datasets:
            values = stage_data[stage][dataset]
            avg_data[stage][dataset] = np.mean(values) if values else 0
    
    # Bar plot comparison
    x = np.arange(len(all_stages))
    width = 0.25
    
    colors = ['#1f77b4', '#ff7f0e', '#2ca02c']  # Blue, Orange, Green
    for i, dataset in enumerate(datasets):
        values = [avg_data[stage][dataset] for stage in all_stages]
        ax.bar(x + i*width, values, width, label=dataset.replace('_', ' ').title(), 
               color=colors[i], alpha=0.8)
    
    ax.set_xlabel('Sleep Stages')
    ax.set_ylabel('Average Epoch Count')
    ax.set_title('Average Stage Distribution')
    ax.set_xticks(x + width)
    ax.set_xticklabels(all_stages)
    ax.legend()
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/stage_comparison.png', dpi=300, bbox_inches='tight')
    plt.close()

def analyze_all_files():
    """Analyze all available files and create comprehensive comparison"""
    final_dir = Path("data_reconciled/final")
    merged_dir = Path("output/merged")
    ai_scored_dir = Path("data_AI_scored")
    
    final_files = list(final_dir.glob("*.txt"))
    comparisons = []
    
    print("Analyzing all available files...")
    print(f"Found {len(final_files)} final reconciled files")
    
    for final_file in final_files:
        # Extract subject ID from filename
        subject_id = extract_subject_id(final_file.name)
        print(f"\nAnalyzing subject: {subject_id}")
        
        # Analyze final reconciled data
        stats, event_dist, stage_analysis, final_df = analyze_final_annotations(str(final_file))
        
        if stats is None:
            continue
            
        # Find corresponding merged file
        merged_file = merged_dir / f"{subject_id}_merged.csv"
        merged_df = load_merged_data(str(merged_file)) if merged_file.exists() else None
        
        # Find corresponding AI scored file
        ai_file = ai_scored_dir / f"{subject_id}.txt"
        ai_df = load_ai_scored_data(str(ai_file), stats.get('start_time')) if ai_file.exists() else None
        
        # Compare distributions
        comparison = compare_stage_distributions(stage_analysis, merged_df, ai_df, subject_id)
        comparisons.append(comparison)
        
        # Analyze reconciliation changes
        if merged_df is not None and final_df is not None:
            changes = analyze_reconciliation_changes(merged_df, final_df)
            comparison['reconciliation_changes'] = changes
            print(f"  Reconciliation changes: {changes.get('total_changes', 0)} ({changes.get('change_percentage', 0):.1f}%)")
        
        # Analyze AI vs Final comparison with detailed statistics
        if ai_df is not None and final_df is not None:
            ai_final_comparison = align_and_compare_ai_final(final_df, ai_df)
            comparison['ai_final_comparison'] = ai_final_comparison
            
            if 'agreement_percentage' in ai_final_comparison:
                print(f"  AI vs Final agreement: {ai_final_comparison['agreement_percentage']:.1f}%")
                print(f"  Total aligned epochs: {ai_final_comparison['total_comparisons']}")
                print(f"  Alignment method: {ai_final_comparison.get('alignment_method', 'unknown')}")
                
                # Show top disagreements
                if ai_final_comparison.get('change_types'):
                    top_changes = Counter(ai_final_comparison['change_types']).most_common(3)
                    print(f"  Top AI disagreements: {[f'{k}({v})' for k, v in top_changes]}")
        
        # Print summary for this subject
        print(f"  Final reconciled - Total epochs: {stage_analysis.get('total_epochs', 0)}")
        if merged_df is not None:
            print(f"  Merged data epochs: {len(merged_df)}")
        if ai_df is not None:
            print(f"  AI scored epochs: {len(ai_df)}")
    
    # Create comparison plots
    if comparisons:
        plot_stage_comparison(comparisons)
        plot_reconciliation_analysis(comparisons)
        print(f"\nGenerated analysis plots for {len(comparisons)} subjects")
    
    return comparisons

def extract_subject_id(filename):
    """Extract subject ID from filename"""
    # Handle different filename patterns
    patterns = [
        r'(AWV\d+)_',
        r'(AWV\d+)\.txt',
        r'(HYP\d+)_',
        r'(NAR\d+)_',
        r'(PED\d+)_',
        r'(CSA\d+)_',
        r'(RAN\d+)_',
        r'(RBD\d+)_',
        r'(\w+)_.*annotations'
    ]
    
    for pattern in patterns:
        match = re.search(pattern, filename)
        if match:
            return match.group(1)
    
    # Fallback: take everything before first underscore or period
    return filename.split('_')[0].split('.')[0]

if __name__ == "__main__":
    # Create plots directory
    Path("plots").mkdir(exist_ok=True)
    
    # Run comprehensive analysis
    comparisons = analyze_all_files()
    
    # Print detailed analysis summary
    if comparisons:
        print(f"\n{'='*70}")
        print("COMPREHENSIVE ANALYSIS SUMMARY")
        print(f"{'='*70}")
        
        # Calculate reconciliation statistics
        total_subjects = len(comparisons)
        subjects_with_changes = 0
        total_changes = 0
        all_change_types = Counter()
        stages_most_changed = Counter()
        
        # Calculate AI vs Final statistics
        subjects_with_ai_comparison = 0
        total_ai_agreements = []
        ai_disagreement_types = Counter()
        ai_stage_errors = Counter()
        
        for comp in comparisons:
            # Reconciliation analysis
            if 'reconciliation_changes' in comp:
                changes = comp['reconciliation_changes']
                subject_changes = changes.get('total_changes', 0)
                if subject_changes > 0:
                    subjects_with_changes += 1
                total_changes += subject_changes
                
                # Aggregate change types
                all_change_types.update(changes.get('change_types', {}))
                stages_most_changed.update(changes.get('changes_by_merged_stage', {}))
            
            # AI vs Final analysis
            if 'ai_final_comparison' in comp:
                ai_comp = comp['ai_final_comparison']
                if 'agreement_percentage' in ai_comp:
                    subjects_with_ai_comparison += 1
                    total_ai_agreements.append(ai_comp['agreement_percentage'])
                    ai_disagreement_types.update(ai_comp.get('change_types', {}))
                    ai_stage_errors.update(ai_comp.get('ai_stage_errors', {}))
        
        print(f"Subjects analyzed: {total_subjects}")
        print(f"Subjects with reconciliation changes: {subjects_with_changes} ({subjects_with_changes/total_subjects*100:.1f}%)")
        print(f"Total reconciliation changes: {total_changes}")
        print(f"Average changes per subject: {total_changes/total_subjects:.1f}")
        
        if stages_most_changed:
            print(f"\nStages most affected by reconciliation:")
            for stage, count in stages_most_changed.most_common(5):
                print(f"  {stage}: {count} changes")
        
        if all_change_types:
            print(f"\nMost common reconciliation changes:")
            for change_type, count in all_change_types.most_common(5):
                print(f"  {change_type}: {count} changes")
        
        # AI vs Final detailed analysis
        if subjects_with_ai_comparison > 0:
            print(f"\n{'='*40}")
            print("AI vs FINAL RECONCILED ANALYSIS")
            print(f"{'='*40}")
            print(f"Subjects with AI comparison: {subjects_with_ai_comparison}")
            print(f"Average AI-Final agreement: {np.mean(total_ai_agreements):.1f}% (±{np.std(total_ai_agreements):.1f})")
            print(f"Range: {min(total_ai_agreements):.1f}% - {max(total_ai_agreements):.1f}%")
            
            if ai_stage_errors:
                print(f"\nAI stages with most errors:")
                for stage, count in ai_stage_errors.most_common(5):
                    print(f"  {stage}: {count} disagreements")
            
            if ai_disagreement_types:
                print(f"\nMost common AI vs Final disagreements:")
                for disagreement, count in ai_disagreement_types.most_common(5):
                    print(f"  {disagreement}: {count} cases")
        
        print(f"\nDetailed analysis plots saved in 'plots/' directory")
        print("- stage_comparison.png: Final vs Merged vs AI scored distributions")
        print("- reconciliation_analysis.png: Detailed reconciliation impact + AI comparison analysis") 