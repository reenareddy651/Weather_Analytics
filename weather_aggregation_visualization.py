import pandas as pd
import matplotlib.pyplot as plt
from pymongo import MongoClient

# MongoDB Setup
client = MongoClient("mongodb://localhost:27017/")
db = client["weather_db"]
collection = db["daily_weather_data_cleaned"]

# Fetch cleaned data from MongoDB
print("Fetching cleaned data from MongoDB...")
data = pd.DataFrame(list(collection.find({}, {"_id": 0})))

# Aggregations
print("Performing aggregations...")

# 1. Average Temperature Trends Over Time
temp_trends = data.groupby("date").agg({
    "tavg": "mean",
    "tmin": "mean",
    "tmax": "mean"
}).reset_index()

# 2. Top 10 Hottest and Coldest Cities
hottest_cities = data.groupby("city")["tavg"].mean().sort_values(ascending=False).head(10)
coldest_cities = data.groupby("city")["tavg"].mean().sort_values(ascending=True).head(10)

# 3. Average Wind Speed by City
wind_speed_by_city = data.groupby("city")["wspd"].mean().sort_values(ascending=False).head(10)

# 4. Temperature Variations by Country
temp_by_country = data.groupby("country").agg({
    "tmin": "mean",
    "tmax": "mean",
    "tavg": "mean"
}).reset_index()

# 5. Wind Direction Distribution
wind_direction = data["wdir"].dropna()

# Visualizations
print("Creating visualizations...")

# 1: Line Chart for Temperature Trends
plt.figure(figsize=(10, 6))
plt.plot(temp_trends["date"], temp_trends["tavg"], label="Average Temperature (°C)", color="orange")
plt.plot(temp_trends["date"], temp_trends["tmin"], label="Min Temperature (°C)", color="blue")
plt.plot(temp_trends["date"], temp_trends["tmax"], label="Max Temperature (°C)", color="red")
plt.title("Average Temperature Trends Over Time")
plt.xlabel("Date")
plt.ylabel("Temperature (°C)")
plt.legend()
plt.tight_layout()
plt.savefig("temperature_trends.png")
plt.show()

# 2: Bar Chart for Hottest and Coldest Cities
plt.figure(figsize=(10, 6))
hottest_cities.plot(kind="bar", color="red", title="Top 10 Hottest Cities")
plt.xlabel("City")
plt.ylabel("Average Temperature (°C)")
plt.tight_layout()
plt.savefig("hottest_cities.png")
plt.show()

plt.figure(figsize=(10, 6))
coldest_cities.plot(kind="bar", color="blue", title="Top 10 Coldest Cities")
plt.xlabel("City")
plt.ylabel("Average Temperature (°C)")
plt.tight_layout()
plt.savefig("coldest_cities.png")
plt.show()

# 3: Average Wind Speed by City
plt.figure(figsize=(10, 6))
wind_speed_by_city.plot(kind="bar", color="green", title="Top 10 Cities by Average Wind Speed")
plt.xlabel("City")
plt.ylabel("Wind Speed (km/h)")
plt.tight_layout()
plt.savefig("wind_speed_by_city.png")
plt.show()

# 4: Wind Direction Distribution
plt.figure(figsize=(10, 6))
plt.hist(wind_direction, bins=36, color="purple", edgecolor="black")
plt.title("Wind Direction Distribution")
plt.xlabel("Wind Direction (Degrees)")
plt.ylabel("Frequency")
plt.tight_layout()
plt.savefig("wind_direction_distribution.png")
plt.show()

print("Visualizations complete! All charts saved as PNG files.")
