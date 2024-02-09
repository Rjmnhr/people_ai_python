import pandas as pd
import os

directory = 'extracts/testing'

# Loop through each subdirectory in the main directory
for subdir, _, _ in os.walk(directory):
    # List all CSV files in the subdirectory
    csv_files = [f for f in os.listdir(subdir) if f.endswith('.csv')]

    # Check if there are any CSV files in the current subdirectory
    if not csv_files:
        continue

    # Initialize an empty list to hold DataFrames for the current subdirectory
    dfs = []
    
    # Read each CSV file and append its DataFrame to the list
    for csv_file in csv_files:
        df = pd.read_csv(os.path.join(subdir, csv_file))

        # Convert 'date_posted' to datetime and filter out rows where 'date_posted' is blank
        #df['date_posted'] = pd.to_datetime(df['date_posted'], format='%d/%m/%y', errors='coerce')
        df = df[df['date_posted'].notna()]

        dfs.append(df)

    # Concatenate all DataFrames from the current subdirectory into one
    merged_df = pd.concat(dfs, ignore_index=True)

    # Sort the DataFrame by 'date_posted'
    merged_df.sort_values(by='date_posted', inplace=True)

    # Drop duplicates based on the 'link' column
    merged_df = merged_df.drop_duplicates(subset='link')

    # Extract the name of the subdirectory and use it as the filename
    subdir_name = os.path.basename(subdir)
    merged_filename = f"{subdir_name}_.csv"

    # Ensure the 'merged' subfolder exists, if not create it
    merged_folder_path = os.path.join(subdir, "merged")
    if not os.path.exists(merged_folder_path):
        os.mkdir(merged_folder_path)

    # Save the merged DataFrame to the 'merged' subfolder in the current subdirectory
    merged_df.to_csv(os.path.join(merged_folder_path, merged_filename), index=False)

    print(f"CSV files in {subdir} merged and sorted successfully into {merged_filename} without duplicates based on the 'link' column and with blank 'date_posted' rows removed!")
