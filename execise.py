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

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
import requests

# Create a SparkSession
spark = SparkSession.builder \
    .appName("Fetch Schema from API") \
    .getOrCreate()

# Load JSON data into PySpark DataFrame
df = spark.read.option("multiline","true").option("inferschema", "true").json("response")

# Falttening the Nested Json
df1 = df.select("*",explode_outer("feeds").alias("new_feeds")).select("*",explode_outer("new_feeds.AirBox").alias("new_AirBox")).drop("new_feeds.AirBox").drop("feeds").drop("new_feeds")

df1.select("device_id","num_of_records","source","version","new_AirBox.*").printSchema()
df1.show(truncate=False)
