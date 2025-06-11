from pymongo import MongoClient
import pandas as pd

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["weather_db"]
collection = db["daily_weather_data"]

# Fetch a Sample of the Data
print("Fetching a sample of the data...")
cursor = collection.find({}, {"_id": 0}).limit(5)
sample_data = pd.DataFrame(list(cursor))

# Display the first 5 rows
print("\nSample Data (First 5 Rows):")
print(sample_data)

# Total Number of Records
total_records = collection.count_documents({})
print(f"\nTotal Number of Records: {total_records}")

# List All Column Names
print("\nColumn Names:")
columns = sample_data.columns.tolist()
print(columns)

# Get Basic Statistics
print("\nBasic Statistics for Numeric Columns:")
cursor_full = collection.find({}, {"_id": 0}).limit(5000)  # Fetch a sample subset
data = pd.DataFrame(list(cursor_full))
print(data.describe())
