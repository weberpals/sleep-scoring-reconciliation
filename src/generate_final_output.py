import os
from utils.combine_events import process_all_files as combine_events
from utils.merge_staging_events import main as merge_staging_events
from utils.add_stage_numbers import add_stage_numbers

def run_stage_numbering():
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

def generate_final_output():
    # Step 1: Run stage numbering
    print("Running stage numbering...")
    run_stage_numbering()
    
    # Step 2: Combine events
    print("Combining events...")
    combine_events()
    
    # Step 3: Merge staging and events
    print("Merging staging and events...")
    merge_staging_events()

if __name__ == "__main__":
    generate_final_output() 