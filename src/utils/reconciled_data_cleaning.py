import pandas as pd
import os
import glob

def clean_reconciled_data(input_file):
    """
    Clean reconciled data by removing rows with removal indicator "!"
    """
    # Read reconciled data from txt
    data = pd.read_csv(input_file, sep=',')
    
    # Remove all rows where Annotation contains "!"
    cleaned_data = data[~data['Annotation'].str.contains('!', na=False)]

    # Also remove rows where Annotation contains "Review" but print a warning and the number of rows removed
    # For AWV022_(1) a high number of rows are removed because it was the initial study (and the process was refined later)
    review_rows = cleaned_data[cleaned_data['Annotation'].str.contains('Review', na=False)]
    if len(review_rows) > 0:
        print(f"Warning: {len(review_rows)} rows where Annotation contains 'Review' were removed from {input_file}")
        cleaned_data = cleaned_data[~cleaned_data['Annotation'].str.contains('Review', na=False)]
            
    
    return cleaned_data

def main():
    # Create output directory if it doesn't exist
    output_dir = 'data_reconciled/final'
    os.makedirs(output_dir, exist_ok=True)
    
    # Get all files in the annotations directory
    input_files = glob.glob('data_reconciled/annotations/*.txt')
    
    for input_file in input_files:
        # Get the filename without path
        filename = os.path.basename(input_file)
        
        # Create output path
        output_file = os.path.join(output_dir, filename)
        
        try:
            # Clean the data
            print(f"Processing {filename}...")
            cleaned_data = clean_reconciled_data(input_file)

            # Save the cleaned data
            cleaned_data.to_csv(output_file, sep=',', index=False)
            print(f"Saved cleaned file to {output_file}")
            
            # Print statistics
            total_rows = len(pd.read_csv(input_file, sep=','))
            removed_rows = total_rows - len(cleaned_data)
            print(f"Removed {removed_rows} rows from {filename}")
            
        except Exception as e:
            print(f"Error processing {filename}: {str(e)}")

if __name__ == "__main__":
    main() 