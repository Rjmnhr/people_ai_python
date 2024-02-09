import pandas as pd
import re
import statistics
import os

def postprocess(input_file, output_file):
    # Read the CSV file and preprocess the DataFrame
    df = pd.read_csv(input_file).drop(columns=['Unnamed: 0', 'Category'])
    df = df[~df['link'].str.contains('promoted')]
    df.reset_index(drop=True, inplace=True)

    # Function to extract contract information
    def intfinder(value):
        for i in value[::-1]:
            if any(char == '%' for char in i):
                continue
            if any(char.isdigit() for char in i):
                index = value.index(i)
                return ' '.join(value[index+1:])

    # Apply the contract extraction function
    df['contract'] = df['salary'].str.split(' ').apply(intfinder)

    # Define pattern for contract detection
    pattern = re.compile(r'(hour|day|month|p\.m\.|p\.h\.|p\.d\.|p/day|pd|daily|p.d|days|hourly|Mth)', flags=re.IGNORECASE)

    # Check for contract information and categorize
    for inde, tab in df.iterrows():
        if isinstance(tab['contract'], str) and re.search(pattern, tab['contract']):
            df.at[inde, 'contract'] = "yes"
        else:
            df.at[inde, 'contract'] = "no"

    # Function to remove duplicates and update salary columns
    def duplicate_remover(link):
        min_max = df[df['link'] == link]['min_salary'].to_list()
        min_max.extend(df[df['link'] == link]['max_salary'].to_list())
        min_max.sort()
        return min_max[0], min_max[-1], statistics.fmean([min_max[0], min_max[-1]])

    # Update salary columns
    for link in df['link'].unique():
        min_max = duplicate_remover(link)
        df.loc[df['link'] == link, ['min_salary', 'max_salary', 'average_salary']] = min_max[0], min_max[1], min_max[2]

    # Drop duplicates
    df.drop_duplicates(subset=['link'], keep='first', inplace=True)

    # Filter based on salary range
    df = df[(df['max_salary'] > 70000) & (df['max_salary'] < 360000)]
    df.reset_index(drop=True, inplace=True)

    # Write to CSV
    df.to_csv(output_file, index=False)

current_directory = os.getcwd()
files_to_process = [file for file in os.listdir(current_directory) if file.startswith('final_')]

def postprocessv2():    
    for file in files_to_process:    
        input_file = os.path.join(current_directory, file)
        output_file = os.path.join(current_directory, 'postprocessed_' + file)
        postprocess(input_file, output_file)