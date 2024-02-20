
import sys
import matplotlib.pyplot as plt
import pandas as pd
from tqdm import tqdm


print("Reading BLU Data ...")
# BLU_raw = pd.read_excel('Data/6207_TailGAS_January.xlsx')  # Input BLU DATA
try:
    BLU_raw = pd.read_excel(str(sys.argv[1]))  # Input BLU DATA
except Exception as ex:
    print(f'{ex} \n Check the name!')
    exit()

print("Reading TC Data ...")
# TC_raw = pd.read_excel('Data/1207_TailGAS_January.xlsx')  # Input TC DATA
try:
    TC_raw = pd.read_excel(str(sys.argv[2]))  # Input TC DATA
except Exception as ex:
    print(f'{ex} \n Check the name!')
    exit()

# destination_name = '6207_With_transfer_METHANATION_.xlsx'
try:
    destination_name = str(sys.argv[3]) # Name for RESULT FILE
except Exception as ex:
    print(f'{ex} \n Check the name!')
    exit()


BLU_raw['switch_p'] = 0
data_raw = BLU_raw.rename(columns={'sensorid': "number_scenario"})

for i, row in tqdm(data_raw.iterrows(), total=len(data_raw), desc="Processing data"):
    if i < len(data_raw.index) - 3:
        if abs(data_raw.at[i, 'BMP aP'] - data_raw.at[i + 3, 'BMP aP']) > 950:
            data_raw.loc[i, 'switch_p'] = 1  # Use .loc to set value

data_raw.loc[len(data_raw) - 1, 'switch_p'] = 1  # Use .loc to set value

data_clean_list = []
old_index = 0
number_scenario = 1
for i, row in tqdm(data_raw.iterrows(), total=len(data_raw), desc="Cleaning data"):
    if (data_raw.at[i, 'switch_p'] == 1) & (i != 0):
        if len(data_raw.loc[old_index:i]) >= 30:
            data = data_raw.loc[i - 22:i - 3].copy()  # Use .loc to get a copy of the slice
            data['number_scenario'] = number_scenario
            data_clean_list.append(data)
            old_index = i
            number_scenario += 1

data_clean = pd.concat(data_clean_list, ignore_index=True)  # Concatenate the list of dataframes
data_clean = data_clean.drop(columns='switch_p')

fig1, ax = plt.subplots()
data_clean['BMP aT'].plot(ax=ax)

fig2, ax = plt.subplots()
data_clean['BMP aP'].plot(ax=ax)


# Copy all data from table A to a new table C
print("Copy Data ...")
df_c = data_clean.copy()

# Convert 'RealTime' column to a consistent format for comparison
print("Converting TimeFormat if needed ...")
df_c['RealTime'] = pd.to_datetime(df_c['RealTime'], format='%m-%d-%Y %I:%M:%S %p')
TC_raw['RealTime'] = pd.to_datetime(TC_raw['RealTime'], format='%m-%d-%Y %I:%M:%S %p')

# Define a time range for comparison (adjust tolerance as needed)
time_range = pd.to_timedelta('530ms')  # Example: 100 milliseconds tolerance is too low

# Sort DataFrames by 'RealTime' for efficient merging
print("Sorting ...")
df_c = df_c.sort_values('RealTime')
df_b = TC_raw.sort_values('RealTime')

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

df_c.to_excel(f'Data/CleanedData/{destination_name}', index=False)
print(f"Done  - {destination_name}")
plt.show()

if __name__ == '__main__':
    BLU_raw = sys.argv[1]
    TC_raw = sys.argv[2]
    destination_name = sys.argv[3]
