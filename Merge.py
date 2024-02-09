import glob
import pandas as pd
from datetime import datetime

# Assuming df is your DataFrame

#must be in this directory 
def merge_csv_files():
    path=r'output/**/*.csv'
    # list all csv files only
    csv_files = glob.glob(path)
    df = pd.DataFrame()

    # append the CSV files
    for file in csv_files:
        data = pd.read_csv(file)
        df = pd.concat([df, data], axis=0)
    # df.to_csv('merged_files3.csv', index=False)
    df.to_csv(f'final_{datetime.now().strftime("%Y%m%d")}.csv', index=False)
# merge_csv_files()