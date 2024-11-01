import pandas as pd
import os

def add_stage_numbers(input_file, output_file):
    # Read the CSV file
    df = pd.read_csv(input_file, delimiter='\t')
    
    # Add ascending numbers to the Description column
    df['Description'] = [f"{i+1}. {desc}" for i, desc in enumerate(df['Description'])]
    
    # Save the modified data back to a CSV file
    df.to_csv(output_file, sep='\t', index=False)

if __name__ == "__main__":
    # Directory containing the staging annotation files
    input_dir = "output/staging_annotation"
    
    # Process all CSV files in the directory
    for filename in os.listdir(input_dir):
        if filename.endswith("_stage_annotations.csv"):
            input_file = os.path.join(input_dir, filename)
            
            # Create output filename by inserting "_numbered" before ".csv"
            base_name = filename[:-4]  # remove .csv
            output_filename = f"{base_name}_numbered.csv"
            output_file = os.path.join(input_dir, output_filename)
            
            add_stage_numbers(input_file, output_file)
            print(f"Created numbered annotations file: {output_file}")