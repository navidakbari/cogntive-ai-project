# simulate_data.py
import pandas as pd
import numpy as np

# Set a seed for reproducibility
np.random.seed(42)

def create_clean_data(num_patients=100, day='1'):
    data = {
        'patient_ID': [f'P{i:03d}' for i in range(1, num_patients + 1)],
        'Timestamp': pd.to_datetime('2025-09-29') + pd.to_timedelta(np.arange(num_patients), unit='h'),
        'Age': np.random.randint(20, 70, num_patients),
        'Heart_Rate': np.random.normal(70, 5, num_patients).round(0), # BPM, clean
        'Sleep_Hours': np.random.uniform(5.5, 9.0, num_patients).round(1), # Hours, clean
        'Depression_Score': np.random.randint(0, 50, num_patients), # PHQ-9 style score
        'Anxiety_Score': np.random.randint(0, 40, num_patients)
    }
    df = pd.DataFrame(data)
    df.to_csv(f'raw_data_day_{day}.csv', index=False)
    print(f"Created clean raw_data_day_{day}.csv")
    return df

def create_corrupt_data(num_patients=100, day='2'):
    # Start with clean data
    data = {
        'patient_ID': [f'P{i:03d}' for i in range(1, num_patients + 1)],
        'Timestamp': pd.to_datetime('2025-09-30') + pd.to_timedelta(np.arange(num_patients), unit='h'),
        # Introduce Schema Drift: 'Age' is now 'Pat_Age'
        'Pat_Age': np.random.randint(20, 70, num_patients),
        'Heart_Rate': np.random.normal(70, 5, num_patients).round(0),
        'Sleep_Hours': np.random.uniform(5.5, 9.0, num_patients).round(1),
        'Depression_Score': np.random.randint(0, 50, num_patients),
        'Anxiety_Score': np.random.randint(0, 40, num_patients)
    }
    df = pd.DataFrame(data)

    # Challenge 1: Nulls in Heart_Rate (a critical column)
    df.loc[10:19, 'Heart_Rate'] = np.nan

    # Challenge 2: Outliers in Sleep_Hours
    df.loc[20, 'Sleep_Hours'] = 100.0 # Extreme outlier

    # Write the corrupt file
    df.to_csv(f'raw_data_day_{day}.csv', index=False)
    print(f"Created corrupt raw_data_day_{day}.csv with issues: Nulls, Outlier, Schema Drift (Pat_Age).")
    return df

# Create clean data for day 1
clean_df = create_clean_data(day='1')

# Create corrupt data for day 2
corrupt_df = create_corrupt_data(day='2')
