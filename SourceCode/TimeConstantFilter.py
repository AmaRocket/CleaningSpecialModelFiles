"""

Cleaning TimeConstant raws that contains < 0 or > 1 values

"""
import pandas as pd

number_scenario_id = "number_scenario"

def clean_blu_sensor_calibration_data(data_raw):
    data_raw = data_raw.dropna()
    print('Nb. of deleted rows in Calibration Data because Time Constant is <= 0.0 : ',
          len(data_raw[data_raw['TimeConstant'] <= 0.0]))
    data_raw = data_raw.drop(data_raw[data_raw['TimeConstant'] <= 0.0].index)
    print('Nb. of deleted rows in Calibration Data because Time Constant is >= 1.0 : ',
          len(data_raw[data_raw['TimeConstant'] >= 1.0]))
    data_raw = data_raw.drop(data_raw[data_raw['TimeConstant'] >= 1.0].index)
    # For future handle also TAO="NaN", or DPC all zeros or DPC mostly zeros but with some non-zero values

    data_raw = data_raw.reset_index(drop=True)
    data_raw['SHT °C'] = data_raw['SHT °C'] + 273.15
    data_raw['switch_p'] = 0
    data_raw[number_scenario_id] = None
    for i, row in data_raw.iterrows():
        if i < len(data_raw.index) - 5:
            if abs(data_raw.at[i, 'BMP aP'] - data_raw.at[i + 5, 'BMP aP']) > 950:
                data_raw.at[i, 'switch_p'] = 1
    data_raw.at[len(data_raw) - 1, 'switch_p'] = 1
    data_clean = pd.DataFrame(columns=data_raw.columns)
    old_index = 0
    number_scenario = 1
    for i, row in data_raw.iterrows():
        if (data_raw.at[i, 'switch_p'] == 1) & (i != 0):
            if len(data_raw.loc[old_index:i]) >= 30:
                data = data_raw.loc[i - 22:i - 3].copy()
                data[number_scenario_id] = number_scenario
                data_clean = pd.concat([data_clean, data])
                # data_clean = (data_clean.copy() if data.empty else data.copy() if data_clean.empty else pd.concat([data_clean, data]))
                old_index = i
                number_scenario += 1
    data_clean = data_clean.drop(columns='switch_p')
    data_clean = data_clean.reset_index(drop=True)

    return data_clean
