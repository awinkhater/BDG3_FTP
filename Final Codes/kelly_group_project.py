#All commented code maxes out java heap memory in local mode
#%%
import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, avg, desc
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import *
import matplotlib.pyplot as plt
import pandas as pd


#create session
spark = SparkSession.builder.appName("NYC_Taxi_Streaming").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

#define schema
schema = StructType([
    StructField("VendorID", IntegerType(), True),
    StructField("tpep_pickup_datetime", TimestampType(), True),
    StructField("tpep_dropoff_datetime", TimestampType(), True),
    StructField("passenger_count", IntegerType(), True),
    StructField("trip_distance", DoubleType(), True),
    StructField("pickup_longitude", DoubleType(), True),
    StructField("pickup_latitude", DoubleType(), True),
    StructField("RateCodeID", IntegerType(), True),
    StructField("store_and_fwd_flag", StringType(), True),
    StructField("dropoff_longitude", DoubleType(), True),
    StructField("dropoff_latitude", DoubleType(), True),
    StructField("payment_type", IntegerType(), True),
    StructField("fare_amount", DoubleType(), True),
    StructField("extra", DoubleType(), True),
    StructField("mta_tax", DoubleType(), True),
    StructField("tip_amount", DoubleType(), True),
    StructField("tolls_amount", DoubleType(), True),
    StructField("improvement_surcharge", DoubleType(), True),
    StructField("total_amount", DoubleType(), True)
])

#read/stream data
taxi_data = spark.readStream.schema(schema) .option("maxFilesPerTrigger", 1).csv("C:\\Users\\Sriniee\\Desktop\\SparkStreamProject\\SparkStreamProject\\Data").dropna()
file_path= "C:\\Users\\Sriniee\\Desktop\\SparkStreamProject\\SparkStreamProject\\Data\\yellow_tripdata_2015-01.csv"
csv_df = pd.read_csv(file_path)

#%%
# Start a streaming query to get the schema
#schema_query = taxi_data.writeStream.format("memory").queryName("schema_data").outputMode("append").start()

# Get the resulting DataFrame from the in-memory table
#schema_df = spark.sql("SELECT * FROM schema_data")

# Get the number of rows and columns
#num_rows = schema_df.count()
#num_cols = len(schema_df.columns)

# Print the DataFrame shape
#print(f"DataFrame shape: ({num_rows}, {num_cols})")

# Start a streaming query to get the descriptive statistics
#describe_query = taxi_data.writeStream.format("memory").queryName("describe_data").outputMode("append").start()

# Get the resulting DataFrame from the in-memory table
#describe_df = spark.sql("SELECT * FROM describe_data")

# Get the descriptive statistics
#describe_stats = describe_df.describe()

# Print the descriptive statistics
#describe_stats.show()

# Calculate averages for various columns
averages = taxi_data.select(
    avg("passenger_count").alias("Avg_Passenger_Count"),
    avg("trip_distance").alias("Avg_Trip_Distance"),
    avg("fare_amount").alias("Avg_Fare_Amount"),
    avg("extra").alias("Avg_Surcharge"),
    avg("mta_tax").alias("Avg_MTA_Tax"),
    avg("tip_amount").alias("Avg_Tip_Amount"),
    avg("tolls_amount").alias("Avg_Tolls_Amount"),
    avg("total_amount").alias("Avg_Total_Amount")
)
averages_query = averages.writeStream.format("console").outputMode("complete").start()

# Calculate averages
avg_passenger_count = csv_df['passenger_count'].mean()
avg_trip_distance = csv_df['trip_distance'].mean()
avg_fare_amount = csv_df['fare_amount'].mean()
avg_extra = csv_df['extra'].mean()
avg_mta_tax = csv_df['mta_tax'].mean()
avg_tip_amount = csv_df['tip_amount'].mean()
avg_tolls_amount = csv_df['tolls_amount'].mean()
avg_total_amount = csv_df['total_amount'].mean()
averages_df = pd.DataFrame({
    "Attribute": ["Passenger Count", "Trip Distance", "Fare Amount", "Extra", "MTA Tax", "Tip Amount", "Tolls Amount", "Total Amount"],
    "Average Value": [avg_passenger_count, avg_trip_distance, avg_fare_amount, avg_extra, avg_mta_tax, avg_tip_amount, avg_tolls_amount, avg_total_amount]
})
print(averages_df)
# Plotting the bar graph
plt.figure(figsize=(10, 6))
plt.barh(averages_df['Attribute'], averages_df['Average Value'], color='skyblue')
plt.xlabel('Average Value')
plt.ylabel('Attribute')
plt.title('Average Values of Taxi Data Attributes')
plt.show()





# Aggregate the data by trip_distance and count the occurrences
trip_distance_counts = taxi_data.groupBy("trip_distance").agg(count("*").alias("count"))

# Sort the aggregated DataFrame by trip_distance and count in ascending order
sorted_trip_distance_counts = trip_distance_counts.orderBy("trip_distance", "count")

# Get the 10 shortest trips by taking the first 10 rows of the sorted DataFrame
shortest_trips = sorted_trip_distance_counts.limit(10)

# Start a streaming query to show the shortest trips
shortest_trips_query = shortest_trips.writeStream.format("console").outputMode("complete").start()

# Sort the aggregated DataFrame by trip_distance and count in descending order
sorted_trip_distance_counts = trip_distance_counts.orderBy("trip_distance", "count", ascending=False)

# Get the 10 largest trips by taking the first 10 rows of the sorted DataFrame
largest_trips = sorted_trip_distance_counts.limit(10)

# Start a streaming query to show the largest trips
largest_trips_query = largest_trips.writeStream.format("console").outputMode("complete").start()

# Combine the 10 shortest and 10 largest trips into a single DataFrame
ls_df = shortest_trips.union(largest_trips)

#start streaming query for hourly average fare calculation
hourly_avg_fare = taxi_data.select(
    "VendorID",
    "passenger_count",
    "trip_distance",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "payment_type",
    "fare_amount",
    "total_amount",
    hour("tpep_pickup_datetime").alias("pickup_hour"),
    dayofweek("tpep_pickup_datetime").alias("pickup_dayofweek")
).groupBy("pickup_hour").agg(avg("fare_amount").alias("avg_fare"))
hourly_avg_fare_query = hourly_avg_fare.writeStream.format("console").outputMode("complete").start()




#Pandas
csv_df['tpep_pickup_datetime'] = pd.to_datetime(csv_df['tpep_pickup_datetime'])
csv_df['pickup_hour'] = csv_df['tpep_pickup_datetime'].dt.hour
hourly_avg_fare1 = csv_df.groupby('pickup_hour').agg({'fare_amount': 'mean'}).reset_index()
hourly_avg_fare1.columns = ['pickup_hour', 'avg_fare']
print(hourly_avg_fare1)


plt.figure(figsize=(10, 6))
plt.bar(hourly_avg_fare1['pickup_hour'], hourly_avg_fare1['avg_fare'], color='skyblue')
plt.xlabel('Pickup Hour')
plt.ylabel('Average Fare Amount')
plt.title('Hourly Average Fare Amount')
plt.xticks(hourly_avg_fare1['pickup_hour'])
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()



#start streaming query for daily average fare calculation
daily_avg_fare = taxi_data.select(
    "VendorID",
    "passenger_count",
    "trip_distance",
    "pickup_latitude",
    "pickup_longitude",
    "dropoff_latitude",
    "dropoff_longitude",
    "payment_type",
    "fare_amount",
    "total_amount",
    hour("tpep_pickup_datetime").alias("pickup_hour"),
    dayofweek("tpep_pickup_datetime").alias("pickup_dayofweek")
).groupBy("pickup_dayofweek").agg(avg("fare_amount").alias("avg_fare"))
daily_avg_fare_query = daily_avg_fare.writeStream.format("console").outputMode("complete").start()

csv_df['pickup_dayofweek'] = csv_df['tpep_pickup_datetime'].dt.dayofweek

daily_avg_fare1 = csv_df.groupby('pickup_dayofweek')['fare_amount'].mean().reset_index()
daily_avg_fare1.columns = ['pickup_dayofweek', 'avg_fare']
print(daily_avg_fare1)
# Plotting the bar graph
plt.figure(figsize=(10, 6))
plt.bar(daily_avg_fare1['pickup_dayofweek'], daily_avg_fare1['avg_fare'], color='skyblue')
plt.xlabel('Day of the Week')
plt.ylabel('Average Fare Amount')
plt.title('Daily Average Fare Amount')
plt.xticks(range(7), ['Monday', 'Tuesday', 'Wednesday', 'Thursday', 'Friday', 'Saturday', 'Sunday'])
plt.grid(axis='y', linestyle='--', alpha=0.7)
plt.show()

############ DON'T ATTEMPT TO MODEL OR PREDICT - NOT ENOUGH MEMORY IN LOCAL MODE
#read CSVs into static dataframe
#static_data = spark.read.schema(schema).csv("C:\\Users\\pjk\\Desktop\\big_data\\archive\\")

#create pickup_hour and pickup_dayofweek columns
#static_data = static_data.withColumn("pickup_hour", hour("tpep_pickup_datetime"))
#static_data = static_data.withColumn("pickup_dayofweek", dayofweek("tpep_pickup_datetime"))

#transform static data
#transformed_static_data = static_data.select(
#    "VendorID",
#    "passenger_count",
#    "trip_distance",
#    "pickup_latitude",
#    "pickup_longitude",
#    "dropoff_latitude",
#    "dropoff_longitude",
#    "payment_type",
#    "fare_amount",
#    "total_amount",
#    "pickup_hour",
#    "pickup_dayofweek"
#)

#prepare data for forecasting
#feature_cols = ["passenger_count", "trip_distance", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "payment_type", "pickup_hour", "pickup_dayofweek"]
#vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features", handleInvalid="skip")
#rf_regressor = RandomForestRegressor(featuresCol="features", labelCol="total_amount")

#create pipeline
#pipeline = Pipeline(stages=[vector_assembler, rf_regressor])

#train model on static data
#model = pipeline.fit(transformed_static_data)

#start streaming query and make fare amount predictions
#transformed_taxi_data = taxi_data.select(
#    "VendorID",
#    "passenger_count",
#    "trip_distance",
#    "pickup_latitude",
#    "pickup_longitude",
#    "dropoff_latitude",
#    "dropoff_longitude",
#    "payment_type",
#    "fare_amount",
#    "total_amount",
#    hour("tpep_pickup_datetime").alias("pickup_hour"),
#    dayofweek("tpep_pickup_datetime").alias("pickup_dayofweek")
#)

#predictions = model.transform(transformed_taxi_data)
#predictions_query = predictions.select("total_amount", "prediction").writeStream.format("console").outputMode("append").start()

#await termination for all streaming queries
#Vendor_frame_query.awaitTermination()
#schema_query.awaitTermination()
#describe_query.awaitTermination()




#Charan Code 


# %%
# Total Fare Amount Collected Over Time
total_fare_query = taxi_data \
    .withWatermark("tpep_pickup_datetime", "1 hour") \
    .groupBy(window("tpep_pickup_datetime", "1 hour")) \
    .agg(sum("fare_amount").alias("total_fare")) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .queryName("total_fare") \
    .start()

# %%
# Average Passenger Count Per Trip Type
avg_passenger_query = taxi_data \
    .groupBy("payment_type") \
    .agg(avg("passenger_count").alias("avg_passengers")) \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .queryName("avg_passenger_count") \
    .start()

#%%
# High Fare Ongoing Trips
high_fare_trips_query = taxi_data \
    .where("fare_amount > 50") \
    .select("VendorID", "tpep_pickup_datetime", "tpep_dropoff_datetime", "fare_amount", "trip_distance") \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .queryName("high_fare_trips") \
    .start()
# %%

from pyspark.sql.functions import window, sum

# streaming query for live number of passengers
live_passenger_count_query = taxi_data \
    .groupBy(window("tpep_pickup_datetime", "10 minutes")) \
    .agg(sum("passenger_count").alias("total_passengers")) \
    .selectExpr("window.start as start_time", "window.end as end_time", "total_passengers") \
    .writeStream \
    .outputMode("update") \
    .format("console") \
    .queryName("live_passenger_count") \
    .start()

shortest_trips_query.awaitTermination()
largest_trips_query.awaitTermination()
averages_query.awaitTermination()
hourly_avg_fare_query.awaitTermination()
daily_avg_fare_query.awaitTermination()
total_fare_query.awaitTermination()
avg_passenger_query.awaitTermination()
high_fare_trips_query.awaitTermination()
live_passenger_count_query.awaitTermination()


# %%
