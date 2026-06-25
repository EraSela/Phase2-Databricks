# Databricks notebook source
# MAGIC %md
# MAGIC ### OpenWeather API
# MAGIC
# MAGIC - Create an account [here](https://openweathermap.org/). You will use their API to get weather forecast data
# MAGIC - After you have created the free account, request an API key. The key enables you to connect to their API and pull weather data.
# MAGIC - Helpful video to understand API calls with Python [here](https://www.youtube.com/watch?v=_jBeFdqnNAU)

# COMMAND ----------

# Execute this cell
import pandas as pd
import numpy as np
import requests

# this cell includes the dataframes you'll use later
# creating the pandas dataframes
weather_error_logs = pd.DataFrame(columns=[
    'ErrorCode',
    'ErrorMessage',
    'GivenCity',
    'GivenLatitude',
    'GivenLongtitude'
    ])


# COMMAND ----------

# MAGIC %md
# MAGIC ### Weather Function
# MAGIC - Create a function which will make requests to the OpenWeather [Current Weather Data API](https://openweathermap.org/current) to get weather information using the latitude and longitude coordinates for a city. 
# MAGIC - Parse the response from the API request and extract the following information about a city: <code>id,name,country,main description,temperature,minimum temperature,maximum temperature,feels_like,humidity and wind speed<code>.
# MAGIC - The function should return a dictionary of the format:
# MAGIC   ```
# MAGIC     {
# MAGIC       "id": 802,
# MAGIC       "city": "Skopje",
# MAGIC       "latitude": 42,
# MAGIC       "longtitude": 21.4333,
# MAGIC       "country": 25,
# MAGIC       "description": "scattered clouds",
# MAGIC       "temp":290.97,
# MAGIC       "temp_min": 293.97,
# MAGIC       "temp_max": 293.97,
# MAGIC       "feels_like": 293.23,
# MAGIC       "humidity": 43,
# MAGIC       "wind_speed": 2.06   
# MAGIC      }
# MAGIC   ```
# MAGIC  - Design the function in a way that it should work for any given pair of values for latittude and longitute coordinates.

# COMMAND ----------

def get_weather_data(city, lat, long, weather_api_key):

    print("Weather function")

    url = "https://api.openweathermap.org/data/2.5/weather"

    params = {
        "lat": lat,
        "lon": long,
        "appid": weather_api_key
    }

    response = requests.get(url, params=params)
    data = response.json()

    # Handle API errors
    if response.status_code != 200:
        weather_error_logs.loc[len(weather_error_logs)] = [
            data.get("cod"),
            data.get("message"),
            city,
            lat,
            long
        ]
        return None

    weather_dict = {
        "id": data["weather"][0]["id"],
        "latitude": data["coord"]["lat"],
        "longtitude": data["coord"]["lon"],
        "city": data["name"],
        "country": data["sys"]["country"],
        "description": data["weather"][0]["description"],
        "temp": data["main"]["temp"],
        "temp_min": data["main"]["temp_min"],
        "temp_max": data["main"]["temp_max"],
        "feels_like": data["main"]["feels_like"],
        "humidity": data["main"]["humidity"],
        "wind_speed": data["wind"]["speed"]
    }

    return weather_dict

# COMMAND ----------

# Run this cell to check your function
city,lat,lon = 'Skopje', 42, 21.4333

answer = get_weather_data(city,lat,lon,weather_api_key)

# checking if you are returning a dictionary
assert isinstance(answer,dict), "You are not returning a dictionary"

# checking if the returned dictionary contains all the right keys
weather_keys = [ "id","latitude","longtitude", "city","country","description","temp","temp_min","temp_max","feels_like","humidity","wind_speed"]
assert all(key in answer for key in weather_keys),"You are missing keys"


# COMMAND ----------

# MAGIC %md
# MAGIC #### Handle failures in weather function 
# MAGIC
# MAGIC - A failure in this case as an example would be considered passing wrong values for latitude and longtitude. For this reason, modify the weather function you developed before to gracefully fail if wrong values for latitude and longtitude are passed. 
# MAGIC - The function should capture the response error message from the API call. In these cases, record this information as a new entry into the dataframe **weather_error_logs**. Explore the structure of this dataframe to see what information you need to capture and what columns you need to populate. If required, process the response further to populate the columns accordingly.

# COMMAND ----------

# clearing out the weather errors dataframe
weather_error_logs.drop(weather_error_logs.index,inplace=True)

# example of wrong values of latitude and longtitude 
city,lat,lon = 'Skopje',7955, 21.4333

# calling the function
get_weather_data(city,lat,lon,weather_api_key)

# Display first few rows of the weather_error_logs dataframe
weather_error_logs.head()

# COMMAND ----------

# MAGIC %md
# MAGIC #### Get weather forecast for new locations
# MAGIC Below you are given a pandas dataframe **weather_df** holding data for new cities and their latitude and longtitude values. 
# MAGIC - For each city in the dataframe, by using the function you developed before, get the weather information and add it to the dataframe. Explore the structure of the dataframe and populate the columns accordingly.

# COMMAND ----------

# creating the pandas dataframes
weather_df = pd.DataFrame(
    data=np.array([
        ["Skopje", 42, 21.4333],
        ["Tetovo", 7958, 21.4333],
        ["London", 51.5085, -0.1257],
        ["Tokyo", 35.6895, 139.6917],
        ["New York", 40.7143, -74.006],
        ["Berlin", 52.5244, 13.4105]
    ]),
    columns=["city", "latitude", "longtitude"]
)

for col in ["id", "country", "description", "temp", "temp_min", "temp_max", "feels_like", "humidity", "wind_speed"]:
    weather_df[col] = ""

print("Weather Dataframe Before:\n", weather_df.head())

# Get weather data for each row
for index, row in weather_df.iterrows():

    weather = get_weather_data(
        row["city"],
        float(row["latitude"]),
        float(row["longtitude"]),
        weather_api_key
    )

    if weather is not None:
        weather_df.loc[index, "id"] = weather["id"]
        weather_df.loc[index, "country"] = weather["country"]
        weather_df.loc[index, "description"] = weather["description"]
        weather_df.loc[index, "temp"] = weather["temp"]
        weather_df.loc[index, "temp_min"] = weather["temp_min"]
        weather_df.loc[index, "temp_max"] = weather["temp_max"]
        weather_df.loc[index, "feels_like"] = weather["feels_like"]
        weather_df.loc[index, "humidity"] = weather["humidity"]
        weather_df.loc[index, "wind_speed"] = weather["wind_speed"]

print("Weather Dataframe After:\n", weather_df.head())

# COMMAND ----------

# MAGIC %md
# MAGIC **Save the dataframes as tables**
# MAGIC
# MAGIC - First, convert the two pandas dataframes `weather_df, weather_error_logs` into Spark dataframes. This [guide](https://docs.databricks.com/pandas/pyspark-pandas-conversion.html) shows how you can achieve that.
# MAGIC - Save the newly created `weather_df, weather_error_logs` Spark DataFrames as managed tables using `.write.mode("overwrite").saveAsTable()`.
# MAGIC
# MAGIC > **💡 Tip:** When converting a Pandas DataFrame to a Spark DataFrame, make sure each column has a consistent data type (e.g., all numeric or all string). Mixed types in a column will cause an Arrow conversion error. Use `pd.to_numeric()` to cast columns that should be numeric.

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, DoubleType

numeric_cols = ["latitude", "longtitude", "id", "temp", "temp_min", "temp_max", "feels_like", "humidity", "wind_speed"]

for col in numeric_cols:
    weather_df[col] = pd.to_numeric(weather_df[col], errors="coerce")

weather_error_logs["ErrorCode"] = weather_error_logs["ErrorCode"].astype(str)
weather_error_logs["ErrorMessage"] = weather_error_logs["ErrorMessage"].astype(str)
weather_error_logs["GivenCity"] = weather_error_logs["GivenCity"].astype(str)
weather_error_logs["GivenLatitude"] = pd.to_numeric(weather_error_logs["GivenLatitude"], errors="coerce")
weather_error_logs["GivenLongtitude"] = pd.to_numeric(weather_error_logs["GivenLongtitude"], errors="coerce")

weather_spark_df = spark.createDataFrame(weather_df)

error_schema = StructType([
    StructField("ErrorCode", StringType(), True),
    StructField("ErrorMessage", StringType(), True),
    StructField("GivenCity", StringType(), True),
    StructField("GivenLatitude", DoubleType(), True),
    StructField("GivenLongtitude", DoubleType(), True)
])

weather_error_logs_spark_df = spark.createDataFrame(weather_error_logs, schema=error_schema)

spark.sql("DROP TABLE IF EXISTS weather_data")
spark.sql("DROP TABLE IF EXISTS weather_error_logs")

weather_spark_df.write.mode("overwrite").saveAsTable("weather_data")
weather_error_logs_spark_df.write.mode("overwrite").saveAsTable("weather_error_logs")

print("Tables saved successfully.")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Displaying the tables
# MAGIC -- select * from `weather_data`
# MAGIC select * from `weather_error_logs`