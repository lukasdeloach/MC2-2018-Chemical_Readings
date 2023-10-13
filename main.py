from datetime import timedelta

import pandas as pd

def load_and_sort_data(filename):
    # Load the data from the CSV file
    data = pd.read_csv(filename)

    # Sort the data by "measure" and "sample date"
    data.sort_values(by=['measure', 'sample date'], inplace=True)

    return data

def find_measures_after_date(data, year):
    # Initialize a list to store measures after the specified year
    measures_after_year = []

    # Iterate through unique measures in the dataset
    for measure in data['measure'].unique():
        measure_data = data[data['measure'] == measure]
        first_sample_date = pd.to_datetime(measure_data['sample date'], format='%d-%b-%y').min()

        if first_sample_date.year > year:
            measures_after_year.append(f"{measure}, {str(first_sample_date).split()[0]}")

    return measures_after_year

def find_abnormal_measure_changes(data, threshold):
    # Initialize a list to store abnormal measure changes
    abnormal_measure_changes = []

    # Iterate through unique measures in the dataset
    for measure in data['measure'].unique():
        measure_data = data[data['measure'] == measure]
        measure_values = measure_data['value'].values
        measure_changes = measure_values[1:] - measure_values[:-1]

        abnormal_changes_mask = abs(measure_changes) > threshold
        abnormal_changes = measure_changes[abnormal_changes_mask]
        sample_dates = pd.to_datetime(measure_data['sample date'], format='%d-%b-%y').values[1:]

        if any(abnormal_changes_mask):
            for change, date in zip(abnormal_changes, sample_dates):
                abnormal_measure_changes.append({
                    'Measure': measure,
                    'Value Change': change,
                    'Sample Date': str(date)
                })

    return abnormal_measure_changes

def find_anomalies_by_threshold(data, multiplier):
    # Initialize a list to store anomalies
    anomalies = []

    # Iterate through unique measures in the dataset
    for measure in data['measure'].unique():
        measure_data = data[data['measure'] == measure]

        # Calculate the mean and standard deviation of the measure
        mean = measure_data['value'].mean()
        std_dev = measure_data['value'].std()

        # Calculate the threshold based on the mean and standard deviation
        threshold_upper = mean + (multiplier * std_dev)
        threshold_lower = mean - (multiplier * std_dev)

        # Identify anomalies (values outside the threshold)
        anomalies_data = measure_data[
            (measure_data['value'] > threshold_upper) |
            (measure_data['value'] < threshold_lower)
        ]

        # Append anomalies to the list
        anomalies.extend(anomalies_data.to_dict(orient='records'))

    return anomalies

def find_chemicals_with_appearances_gaps(data, gap_threshold_days=1825):
    # Initialize a list to store chemicals with appearances gaps
    gaps = []
    formatted_gaps = []

    # Convert the 'sample date' column to a datetime object
    data['sample date'] = pd.to_datetime(data['sample date'], format='%d-%b-%y')

    # Sort the data by 'sample date'
    data = data.sort_values(by=['measure', 'sample date'])

    # Group the data by 'measure'
    grouped = data.groupby('measure')

    for measure, group in grouped:
        # Calculate the time gaps between consecutive samples for the current chemical
        group['time_gap'] = group['sample date'].diff()

        # Filter for gaps exceeding the threshold
        large_gaps = group[group['time_gap'] > pd.Timedelta(days=gap_threshold_days)]

        if not large_gaps.empty:
            # If there are large gaps for the current chemical, store it in the 'gaps' list
            large_gaps_info = large_gaps[['measure', 'sample date', 'value', 'time_gap']].copy()
            large_gaps_info['time_gap_days'] = large_gaps_info['time_gap'].dt.days
            large_gaps_info.drop(columns=['time_gap'], inplace=True)

            # Store the formatted data
            gaps.append({'measure': measure, 'large_gaps': large_gaps_info})

    for chemical in gaps:
        for _, row in chemical['large_gaps'].iterrows():
            last_day_after_gap = row['sample date'] - timedelta(days=row['time_gap_days'])
            formatted_gaps.append({(f"Chemical: {chemical['measure']} | Last Measure Before Gap: {last_day_after_gap} | First Measure After Gap: {row['sample date']} |Value: {row['value']} | Gap (Days): {row['time_gap_days']}")})
    return formatted_gaps

# Example usage:
data = load_and_sort_data('Boonsong Lekagul waterways readings.csv')
appearance_gaps = find_chemicals_with_appearances_gaps(data)
measures_after_2010 = find_measures_after_date(data, 2010)
abnormal_measure_changes = find_abnormal_measure_changes(data, 1000)
anomalies = find_anomalies_by_threshold(data, 20)
gap_threshold = 5  # Define the gap threshold (in years)
elapsed_threshold = 10  # Define the elapsed threshold (in years)

# Open the 'results' file for writing
with open('results.txt', 'w') as file:
    file.write("Chemicals with appearance gaps:\n")
    for chemical in find_chemicals_with_appearances_gaps(data):
        file.write(f"{chemical}")
        file.write('\n')
    file.write('\n')

    file.write("Measures after 2010:\n")
    for item in measures_after_2010:
        file.write(f"{item}\n")

    file.write("\nAbnormal measure changes:\n")
    for item in abnormal_measure_changes:
        file.write(f"{item}\n")

    file.write("\nAnomalies:\n")
    for anomaly in anomalies:
        file.write(f"Measure: {anomaly['measure']}, Sample Date: {anomaly['sample date']}, Value: {anomaly['value']}\n")



