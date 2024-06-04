import requests
import json
import os

# Function to read data from local storage
def read_local_data(file_path):
    if os.path.exists(file_path):
        with open(file_path, "r") as f:
            return json.load(f)
    else:
        return []

# Function to save data to local storage
def save_local_data(data, file_path):
    with open(file_path, "w") as f:
        json.load(data, f)

# Define the API endpoint
api_endpoint = "https://pm25.lass-net.org/API-1.0.0/device/08BEAC028C00/history/?format=JSON"

# Define the file path to local storage
file_path = "/Pyspark projects/Exercise-1.json"

# Read existing data from local storage
existing_data = read_local_data(file_path)

# Send a GET request to the API endpoint
response = requests.get(api_endpoint)

# Check if the request was successful (status code 200)
if response.status_code == 200:
    # Get the response content (JSON data)
    new_data = response.json()

    # Compare new data with existing data to identify new entries
    new_entries = [entry for entry in new_data if entry not in existing_data]

    if new_entries:
        # Update existing data with new entries
        updated_data = existing_data + new_entries

        # Save the updated data to local storage
        save_local_data(updated_data, file_path)

        print("New data inserted into local storage.")
    else:
        print("No new data available.")
else:
    print("Failed to fetch data from the API:", response.status_code)
