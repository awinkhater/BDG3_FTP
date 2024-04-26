#!/usr/bin/env python
# coding: utf-8

# In[2]:


import pandas as pd
from pyspark.sql.functions import col, lit, expr, when, to_timestamp
from pyspark.sql.types import *
from datetime import datetime
import time
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import month, desc, hour, count, avg, mean


# In[3]:


from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

# Define our schema, as we have done in class
nyc_schema = StructType([
    StructField('Vendor', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('Passenger_Count', IntegerType(), True),
    StructField('Trip_Distance', DoubleType(), True),
    StructField('Pickup_Longitude', DoubleType(), True),
    StructField('Pickup_Latitude', DoubleType(), True),
    StructField('Rate_Code', StringType(), True),
    StructField('Store_And_Forward', StringType(), True),
    StructField('Dropoff_Longitude', DoubleType(), True),
    StructField('Dropoff_Latitude', DoubleType(), True),
    StructField('Payment_Type', StringType(), True),
    StructField('Fare_Amount', DoubleType(), True),
    StructField('Surcharge', DoubleType(), True),
    StructField('MTA_Tax', DoubleType(), True),
    StructField('Tip_Amount', DoubleType(), True),
    StructField('Tolls_Amount', DoubleType(), True),
    StructField('Total_Amount', DoubleType(), True)
])

# Create SparkSession
spark = SparkSession.builder \
    .appName("FTP_practice") \
    .getOrCreate()

# Load CSV file: This is the main dataset we used. The other 4 are in use too, but for the analysis in this code,
#We are just using this file.
#It is still 12 million rows
d1path=r"C:\Users\alexw\Desktop\BDFTP_DATA\yellow_tripdata_2015-01.csv"
rawDF = spark.read.format('csv').options(header=True).schema(nyc_schema).load(d1path)


# In[4]:


from pyspark.sql.functions import col, sum as spark_sum

#We are using a dictionary for null counts so we can make key-value pairs to get null count by column
null_counts = {}
for C in rawDF.columns:
    null_counts[C] = rawDF.where(col(C).isNull()).count()

# This just prints the null count for each column
for C, count in null_counts.items():
    print(f"Column '{C}': {count} null values")


# In[5]:


#Number of ROWS
num_rows = rawDF.count()

# Getting the number of columns
num_cols = len(rawDF.columns)

# Printing the shape of the DataFrame
print(f"This data frame has {num_rows} rows, and {num_cols} Columns")


# In[12]:


#Checking Columns
columns_list = rawDF.columns
print(f"Columns: {columns_list}")


# In[6]:


#This is an attempt to see the logical and physical spark plans. 
#The output is super long due to the extended arg
print(rawDF.explain(extended=True))


# In[7]:


columns_list = rawDF.columns
# Printing the list of columns
print(f"Columns of the DataFrame:{columns_list}")


# In[8]:


#We wanted to create a dataframe Grouped by Passenger Count, to show us how many ride
#Chat GPT helped format this one, as I kept getting syntax errors
Rider_frame = rawDF.groupBy("Passenger_Count").count().withColumnRenamed("count", "Ride Count").orderBy(col("Ride Count").desc())
Rider_frame.show()


# In[9]:


#This is a basic df describe command for EDA
rawDF.describe() 


# In[ ]:


averages = rawDF.select(
    avg("Passenger_Count").alias("Avg_Passenger_Count"),
    avg("Trip_Distance").alias("Avg_Trip_Distance"),
    avg("Fare_Amount").alias("Avg_Fare_Amount"),
    avg("Surcharge").alias("Avg_Surcharge"),
    avg("MTA_Tax").alias("Avg_MTA_Tax"),
    avg("Tip_Amount").alias("Avg_Tip_Amount"),
    avg("Tolls_Amount").alias("Avg_Tolls_Amount"),
    avg("Total_Amount").alias("Avg_Total_Amount")
)

# Show the averages of each column
averages.show()


# In[ ]:


#Condensned version for prez:
Caverages = rawDF.select(
    avg("Passenger_Count").alias("Avg_Passenger_Count"),
    avg("Trip_Distance").alias("Avg_Trip_Distance"),
    avg("Fare_Amount").alias("Avg_Fare_Amount"),
    avg("Tip_Amount").alias("Avg_Tip_Amount"),
    avg("Total_Amount").alias("Avg_Total_Amount")
)

# Show the averages of each column
Caverages.show()


# In[ ]:


from pyspark.sql.functions import min, max

# Query to calculate minimum values of each column
Cmin = rawDF.select(
    min("Passenger_Count").alias("Min_Passenger_Count"),
    min("Trip_Distance").alias("Min_Trip_Distance"),
    min("Fare_Amount").alias("Min_Fare_Amount"),
    min("Tip_Amount").alias("Min_Tip_Amount"),
    min("Total_Amount").alias("Min_Total_Amount")
)

# Query to calculate maximum values of each column
Cmax = rawDF.select(
    max("Passenger_Count").alias("Max_Passenger_Count"),
    max("Trip_Distance").alias("Max_Trip_Distance"),
    max("Fare_Amount").alias("Max_Fare_Amount"),
    max("Tip_Amount").alias("Max_Tip_Amount"),
    max("Total_Amount").alias("Max_Total_Amount")
)

# Show the minimum values of each column
Cmin.show()


# In[ ]:


# Show the maximum values of each column
Cmax.show()


# In[15]:


#Dropping problematic datetime columns, Pandas hate them i guess

problematic_datetime_columns = ["tpep_pickup_datetime", "tpep_dropoff_datetime"]
rawDF = rawDF.drop(*problematic_datetime_columns)

# # Taking a random sample (.001% in this case); for visualization

dfP = rawDF.sample(withReplacement=False, fraction=0.0001, seed=42)


# Convert the PySpark DataFrame to a Pandas DataFrame so I can use matplotlib/seaborn
df = dfP.toPandas()


# In[16]:


#SAMPLE INFO
print(df.head())
print(len(df))


# In[17]:


import matplotlib.pyplot as plt
import seaborn as sns
#HEATMAP
#THIS CHOOSES ALL THE NUMERICAL VARIABLES
ndf = df.select_dtypes(include=['number'])
CNDF=ndf.corr()

#CHOSE COOLWARM AS ITS THE BEST FOR PRESENTATION (THE GRAY DOESNT DRAW ATTENTION)
plt.figure(figsize=(10, 8))
sns.heatmap(CNDF, annot=True, cmap='coolwarm', fmt=".2f")
plt.title('Numerical Variable Correlation Heatmap')
plt.show()


# In[21]:


#Scatter Plot
plt.scatter(df['Trip_Distance'], df['Passenger_Count'])

#Axis labels and Title
plt.xlabel('Trip Distance')
plt.ylabel('Passenger Count')
plt.title('Scatter Plot of Trip Distance vs Passenger Count')
plt.grid()
plt.tight_layout()
plt.show()


# In[20]:


#Scatter Plot 2
plt.scatter(df['Tip_Amount'], df['Passenger_Count'])
plt.xlabel('Tip Amount')
plt.ylabel('Passenger Count')
plt.title('Scatter Plot of Tip Amount vs Passenger Count')
plt.grid()
plt.tight_layout()
plt.show()


# In[8]:


plt.scatter(df['Tip_Amount'], df['Fare_Amount'])
plt.xlabel('Tip Amount')
plt.ylabel('Fare Amount')
plt.title('Scatter Plot of Tip vs Fare in USD$')
plt.grid()
plt.show()


# In[22]:


#HISTOGRAM 1
plt.hist(df['Passenger_Count'])
plt.xlabel('Passenger Count')
plt.ylabel('Frequency')
plt.title('Histogram Plot of Passenger Count by Trip Distance')
plt.tight_layout()
# Rotate x-axis labels so it fits
plt.xticks(rotation=45)
plt.show()


# In[42]:


#============== WARNING MAKES EVERYTHING SUPER SUPER SLOW. COMMENT THIS OUT WHEN NOT IN USE
#============== SAVED IT AS HTML ALREADY, Just use that---> SEE GIT HUB
import folium
mymap = folium.Map(location=[df['Pickup_Latitude'].iloc[0], df['Pickup_Longitude'].iloc[0]], zoom_start=14)


#MAKING MAP/ CHATGPT helped with folium syntax
# Add markers for pickup locations
for index, row in df.iterrows():
    folium.Marker([row['Pickup_Latitude'], row['Pickup_Longitude']], popup='Pickup').add_to(mymap)

# Red Markers for dropoff locations
for index, row in df.iterrows():
    folium.Marker([row['Dropoff_Latitude'], row['Dropoff_Longitude']], popup='Dropoff',
                  icon=folium.Icon(color='red')).add_to(mymap)

#Blue lines connecting pickup and dropoff locations
for index, row in df.iterrows():
    folium.PolyLine(locations=[[row['Pickup_Latitude'], row['Pickup_Longitude']],
                               [row['Dropoff_Latitude'], row['Dropoff_Longitude']]], 
                    color='blue').add_to(mymap)

# Save the map to an HTML file
mymap.save("map_with_lines_and_colored_dropoff.html")

# Display the map
mymap


# In[ ]:


import folium

# Filter rawDF to contain only the 10 longest trip distance rows
top_10_longest_trips = rawDF.orderBy(rawDF['Trip_Distance'].desc()).limit(10)


#MAKING MAP/ CHATGPT helped with folium syntax
SLmap = folium.Map(location=[top_10_longest_trips.select('Pickup_Latitude').collect()[0][0],
                             top_10_longest_trips.select('Pickup_Longitude').collect()[0][0]],
                   zoom_start=13)

for row in top_10_longest_trips.collect():
    folium.Marker([row['Pickup_Latitude'], row['Pickup_Longitude']], popup='Pickup').add_to(SLmap)

for row in top_10_longest_trips.collect():
    folium.Marker([row['Dropoff_Latitude'], row['Dropoff_Longitude']], popup='Dropoff',
                  icon=folium.Icon(color='red')).add_to(SLmap)

for row in top_10_longest_trips.collect():
    folium.PolyLine(locations=[[row['Pickup_Latitude'], row['Pickup_Longitude']],
                               [row['Dropoff_Latitude'], row['Dropoff_Longitude']]], 
                    color='green').add_to(SLmap)

# # Save the map to an HTML file
# SLmap.save("Top_10_Longest_Trips_Map.html")

# Display the map
SLmap


# In[ ]:





# In[ ]:




