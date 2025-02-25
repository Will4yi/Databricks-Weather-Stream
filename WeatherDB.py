from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, current_timestamp, max as spark_max
import requests
import time

delta_table_path = "dbfs:/mnt/weather_data/delta"
filtered_delta_table_path = "dbfs:/mnt/weather_data/highest_temp"

# Weather API Key (Replace with your actual key)
API_KEY = "3497b4378bda48d2b0c222258240912"
LOCATIONS = ["California", "New York", "Texas"]
BASE_URL = "http://api.weatherapi.com/v1/current.json"

def fetch_weather_data(location):
    url = f"{BASE_URL}?key={API_KEY}&q={location}&aqi=no"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        return {
            "location": data["location"]["name"],
            "region": data["location"]["region"],
            "country": data["location"]["country"],
            "lat": data["location"]["lat"],
            "lon": data["location"]["lon"],
            "temp_c": data["current"]["temp_c"],
            "humidity": data["current"]["humidity"],
            "wind_kph": data["current"]["wind_kph"],
            "condition": data["current"]["condition"]["text"],
            "timestamp": data["location"]["localtime"]
        }
    else:
        print(f"Failed to fetch data for {location}: {response.status_code}, {response.text}")
        return None

weather_data_list = [fetch_weather_data(location) for location in LOCATIONS]

def append_to_delta():
    while True:
        all_weather_data = [fetch_weather_data(location) for location in LOCATIONS]
        valid_weather_data = [data for data in all_weather_data if data]

        if valid_weather_data:
            df = spark.createDataFrame(valid_weather_data)
            df.withColumn("ingestion_time", current_timestamp()) \
              .write.format("delta") \
              .mode("append") \
              .save(delta_table_path)

            print(f"Weather data appended at {time.strftime('%Y-%m-%d %H:%M:%S')}")
        time.sleep(300)
        
def write_stream_highest_temp():
    weather_stream = spark.readStream.format("delta").load(delta_table_path)
    
    highest_temp_df = (weather_stream
        .groupBy("location", "region", "country", "lat", "lon")
        .agg(
            spark_max("temp_c").alias("max_temp_c"),
            spark_max("timestamp").alias("latest_timestamp")
        )
    )

    query = (highest_temp_df
        .writeStream
        .format("delta")
        .outputMode("complete")  
        .option("checkpointLocation", "dbfs:/mnt/weather_data/checkpoints/highest_temp")
        .start(filtered_delta_table_path)
    )
    query.awaitTermination()

# df2 = spark.read.format("delta").load('dbfs:/mnt/weather_data/highest_temp')
# df2.show()
# display(df2)
