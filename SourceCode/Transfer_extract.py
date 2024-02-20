
import pandas as pd
from tqdm import tqdm

# Read Excel files
print("Reading Data ...")
df_a = pd.read_excel('Data/CleanedData/6207_20_BLU_points.xlsx')
df_b = pd.read_excel('Data/1207_TailGAS_January.xlsx')


# Copy all data from table A to a new table C
print("Copy Data ...")
df_c = df_a.copy()

# Convert 'RealTime' column to a consistent format for comparison
print("Converting TimeFormat if needed ...")
df_c['RealTime'] = pd.to_datetime(df_c['RealTime'], format='%m-%d-%Y %I:%M:%S %p')
df_b['RealTime'] = pd.to_datetime(df_b['RealTime'], format='%m-%d-%Y %I:%M:%S %p')

# Define a time range for comparison (adjust tolerance as needed)
time_range = pd.to_timedelta('530ms')  # Example: 100 milliseconds tolerance is too low

# Sort DataFrames by 'RealTime' for efficient merging
print("Sorting ...")
df_c = df_c.sort_values('RealTime')
df_b = df_b.sort_values('RealTime')

# Define a time range for comparison (adjust tolerance as needed)
time_range = pd.to_timedelta('530ms')  # Example: 100 milliseconds tolerance is too low

# Initialize tqdm with the total number of iterations (rows in df_c)
total_iterations = len(df_c)
with tqdm(total=total_iterations, desc="Processing", unit="row") as pbar:
    # Merge DataFrames based on the 'RealTime' column and add 'Transfer' column
    for _, row in df_c.iterrows():
        matched_row = df_b.iloc[(df_b['RealTime'] - row['RealTime']).abs().idxmin()]
        df_c.loc[row.name, 'Transfer'] = matched_row['Transfer']

        pbar.update(1)  # Update progress bar for each row

# Save the new DataFrame to an Excel file
name = '6207_With_transfer_METHANATION_.xlsx'
df_c.to_excel(f'Data/CleanedData/{name}', index=False)
print(f"Done  - {name}")
