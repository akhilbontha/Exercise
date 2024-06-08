# Step 1: Fetch Data from the API
import requests
import json

# Replace with your API URL
api_url = "https://pm25.lass-net.org/API-1.0.0/device/08BEAC0AB6D6/history/" 
response = requests.get(api_url)

if response.status_code == 200:
    new_data = response.json()
    # Ensure new_data is a list
    if isinstance(new_data, dict):
        new_data = [new_data]
    elif not isinstance(new_data, list):
        raise Exception(f"Unexpected data format: {type(new_data)}")
else:
    raise Exception(f"Failed to fetch data. Status code: {response.status_code}")

# Read Existing Data from DBFS
from pyspark.sql import SparkSession
from pyspark.dbutils import DBUtils
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Initialize Spark session and DBUtils
spark = SparkSession.builder.appName("APIDataStaging").getOrCreate()
dbutils = DBUtils(spark)

# Define the path for the data in DBFS
existing_data_path = "/FileStore/tables/api_data7.json"

# Check if the file exists and read the existing data
try:
    existing_data_df = spark.read.json("dbfs:" + existing_data_path)
    existing_data = [row.asDict() for row in existing_data_df.collect()]
except Exception as e:
    # If the file does not exist, initialize an empty list
    existing_data = []

# Ensure existing_data is a list
if not isinstance(existing_data, list):
    existing_data = [existing_data]

# Combine existing data with new data
combined_data = existing_data + new_data

# Convert combined data to JSON string
combined_data_json = json.dumps(combined_data)

# Save combined JSON data to DBFS
dbutils.fs.put(existing_data_path, combined_data_json, overwrite=True)

# Read JSON data from DBFS
df = spark.read.json("dbfs:/FileStore/tables/api_data7.json")

# Show the DataFrame
#df.show()

# Print the schema
#df.printSchema()

# Doing first level flattening df1
df1 = df.select("*",explode_outer("feeds").alias("new_feeds")).select("*",explode_outer("new_feeds.AirBox").alias("new_AirBox")).drop("new_feeds.AirBox").drop("feeds").drop("new_feeds")

df_1 = df1.select("device_id","num_of_records","source","version")


df2 = df1.select("new_AirBox.*")

# Extract the dynamic timestamp keys
keys = df2.schema.fieldNames()

# Flatten the nested structure
flattened_df = df2.selectExpr("inline(array(" + ",".join([f"struct('{k}' as timestamp_key, `{k}`.*)" for k in keys]) + "))")

flattened_df1 = flattened_df.dropna(subset=['Date','time'])

# Show the resulting flattened DataFrame

df_inner = df_1.join(flattened_df1, on="device_id", how="inner").dropDuplicates()

#display(df_inner)

# Filter the DataFrame where s_d0 (PM2.5 level) is greater than 30
df_filtered = df_inner.filter(col("s_d0") > 30).select("date","timestamp", "s_d0")
display(df_filtered)

# Extract date from timestamp for daily statistics
df = df_inner.withColumn("date", date_format(col("timestamp"), "yyyy-MM-dd"))

# Compute daily maximum, minimum, and average pollution values
daily_stats = df.groupBy("date").agg(
    max("s_d0").alias("daily_max"),
    min("s_d0").alias("daily_min"),
    avg("s_d0").alias("daily_avg")
)

# Collect the filtered times and daily statistics
times_above_threshold = df_filtered.collect()
daily_stats_report = daily_stats.collect()

# Display the results
print("Times when PM2.5 level went above the threshold of 30:")
for row in times_above_threshold:
    print(f"Timestamp: {row['timestamp']}, PM2.5 Level: {row['s_d0']}")

print("\nDaily PM2.5 Statistics:")
print("Date       | Daily Max | Daily Min | Daily Avg")
for row in daily_stats_report:
    print(f"{row['date']} | {row['daily_max']}     | {row['daily_min']}     | {row['daily_avg']:.2f}")
