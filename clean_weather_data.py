from pymongo import MongoClient
import pandas as pd

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["weather_db"]
collection = db["daily_weather_data"]
cleaned_collection = db["daily_weather_data_cleaned"]

# Fetch Data from MongoDB
print("Fetching data from MongoDB...")
data = pd.DataFrame(list(collection.find({}, {"_id": 0})))

# Clean the Data
print("Cleaning the data...")

# Drop rows with missing critical fields
data = data.dropna(subset=["date", "country", "city", "tavg", "tmin", "tmax"])

# Convert date to datetime format
data["date"] = pd.to_datetime(data["date"], errors="coerce")
data = data.dropna(subset=["date"])

# Standardize text fields
data["country"] = data["country"].str.lower().str.strip()
data["city"] = data["city"].str.lower().str.strip()

# Fill Missing Latitude and Longitude
print("Filling missing Latitude and Longitude...")
def fill_lat_long(row, reference_data):
    if pd.isnull(row["Latitude"]) or pd.isnull(row["Longitude"]):
        match = reference_data[
            (reference_data["country"] == row["country"]) &
            (reference_data["city"] == row["city"]) &
            (~reference_data["Latitude"].isnull()) &
            (~reference_data["Longitude"].isnull())
        ]
        if not match.empty:
            row["Latitude"] = match.iloc[0]["Latitude"]
            row["Longitude"] = match.iloc[0]["Longitude"]
    return row

data = data.apply(lambda row: fill_lat_long(row, data), axis=1)
data = data.dropna(subset=["Latitude", "Longitude"])  # Drop if still missing

# Fill Other Fields with Averages
print("Filling missing values with averages...")
numeric_fields = ["tavg", "tmin", "tmax", "wspd", "wdir"]

for field in numeric_fields:
    if field in data.columns:
        data[field] = data.groupby(["country", "city"])[field].transform(
            lambda x: x.fillna(x.mean())
        )

# Drop Remaining Duplicates
print("Removing duplicates...")
data = data.drop_duplicates()

# Insert Cleaned Data into MongoDB
print("Inserting cleaned data into MongoDB...")
cleaned_collection.delete_many({})  # Clear any previous data
cleaned_collection.insert_many(data.to_dict(orient="records"))

record_count = cleaned_collection.count_documents({})
print(f"Data cleaning complete! Total cleaned records: {record_count}")
