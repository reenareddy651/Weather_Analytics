import pandas as pd
import os
import zipfile
from kaggle.api.kaggle_api_extended import KaggleApi
from pymongo import MongoClient

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["weather_db"]
collection = db["daily_weather_data"]

# Kaggle Dataset Details
kaggle_dataset = "balabaskar/historical-weather-data-of-all-country-capitals"  # Dataset path on Kaggle
kaggle_file = "daily_weather_data.csv"  # File within the dataset
local_zip_file = "daily_weather_data.csv.zip"  # Name for the downloaded zip file
local_csv_file = "weather_data/daily_weather_data.csv"  # Path for extracted CSV file

# Step 1: Download Kaggle Data
api = KaggleApi()
api.authenticate()

if not os.path.exists(local_zip_file):
    print("Downloading dataset from Kaggle...")
    api.dataset_download_file(kaggle_dataset, kaggle_file, path=".")
    print("Download complete.")

# Step 2: Unzip the File
if not os.path.exists(local_csv_file):
    print("Extracting CSV from the zip file...")
    with zipfile.ZipFile(local_zip_file, 'r') as zip_ref:
        zip_ref.extractall("weather_data")  # Extract to "weather_data" directory
    print("Extraction complete.")

# Step 3: Load Kaggle Data
if not os.path.exists(local_csv_file):
    raise FileNotFoundError(f"Extracted CSV file not found: {local_csv_file}")

print("Loading CSV data...")
df = pd.read_csv(local_csv_file)

# Step 4: Load Data into MongoDB
print("Ingesting data into MongoDB...")
data_dict = df.to_dict(orient="records")
collection.delete_many({})  # Clear existing data if any
collection.insert_many(data_dict)

# Verify insertion
record_count = collection.count_documents({})
print(f"Data ingestion complete! Total records inserted: {record_count}")
