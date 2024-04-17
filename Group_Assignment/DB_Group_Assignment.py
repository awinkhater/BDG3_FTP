#%%
#Where we Import 
from pyspark.sql.functions import col, lit, expr, when, to_timestamp
from pyspark.sql.types import *
from datetime import datetime
import time
import pyspark
from pyspark.sql import SparkSession
import random
import string
import pandas as pd
from pyspark.sql.functions import avg
import plotly.express as px
import pandas as pd
import numpy as np
import time
# %%
#Q1
##+===========================================================SET DATASETLENGTH HERE!!!
DataLength=1000000
# %%
#Q1

#Reading the original Raw city dataframes, found from the internet.
#They contain names of cities and population.
OG_F_path=r"french_cities.csv"
OG_G_path=r"german_cities.csv"
dfF=pd.read_csv(OG_F_path)
dfG=pd.read_csv(OG_G_path)
#Renaming Cities to City
dfF.rename(columns={'cities': 'city'}, inplace=True)
# %%
#Q1
#Checking the original French set out


dfF.head()

# %%
#Checking the original German set out:
dfG.head()

# %%
#Q1
# # GRADER YOU WILL NEED TO UNCOMMENT THIS TO RUN CODE ON YOUR MACHINE!

# #GENERATING RANDOM CITY NAMES
# #French cities will end with 'ville'
# #German cities will end with 'burg'
# def generate_city_name(suffix):
#     return ''.join(np.random.choice(list('abcdefghijklmnopqrstuvwxyz'), np.random.randint(4, 7))) + suffix

# # This function makes random population within the range of existing df's population
# def generate_population():
#     return np.random.randint(0, 4000000)

# #I commented all this because it is unnecessary after the final dataset is made
# #It takes A LONG TIME to run

# # Adding French cities
# additional_data_F = pd.DataFrame({
#     'city': [generate_city_name('ville') for _ in range(DataLength)],
#     'population': [generate_population() for _ in range(DataLength)]
# })

# # Adding German cities
# additional_data_G = pd.DataFrame({
#     'city': [generate_city_name('burg') for _ in range(DataLength)],
#     'population': [generate_population() for _ in range(DataLength)]
# })

# # Concatenate the random data with the original data
# dfF_ext = pd.concat([dfF, additional_data_F], ignore_index=True)
# dfG_ext = pd.concat([dfG, additional_data_G], ignore_index=True)
# %%
#Q1
##!!GRADER YOU WILL NEED TO UNCOMMENT THIS TO RUN CODE ON YOUR MACHINE!
# ##This is generating the other columns.


# ##The areas are based on actual Km^2 data from the largest cities of the counties (Paris and Berlin)
# ##I just made up the cultural sites (:^|
# G_Areamin = 20
# G_Areamax = 900
# F_Areamax=110
# F_Areamin=20
# cultmin = 0
# cultmax = 100

# # Function to generate random CityID
# def generate_city_id():
#     return ''.join(str(np.random.randint(0, 10)) for _ in range(6))

# # Generate random values for dfF
# dfF_ext['Area'] = np.random.uniform(F_Areamin, F_Areamax, size=len(dfF_ext)).round(2)
# dfF_ext['Cultural Sites'] = np.random.randint(cultmin, cultmax, size=len(dfF_ext))
# dfF_ext['CityID'] = [generate_city_id() for _ in range(len(dfF_ext))]

# # Generate random values for dfG
# dfG_ext['Area'] = np.random.uniform(G_Areamin, G_Areamax, size=len(dfG_ext)).round(2)
# dfG_ext['Cultural Sites'] = np.random.randint(cultmin, cultmax, size=len(dfG_ext))
# dfG_ext['CityID'] = [generate_city_id() for _ in range(len(dfG_ext))]

# %%
#Q1
##!GRADER YOU WILL NEED TO UNCOMMENT THIS TO RUN CODE ON YOUR MACHINE!

# #Renaming some columns because I generated the data wrong
# #FRANCE
# dfF_ext.rename(columns={'city': 'CityName'}, inplace=True)
# dfF_ext.rename(columns={'population': 'Population'}, inplace=True)
# dfF_ext.rename(columns={'Cultural Sites':"CulturalSites"}, inplace=True)

# #Germany
# dfG_ext.rename(columns={'city': 'CityName'}, inplace=True)
# dfG_ext.rename(columns={'population': 'Population'}, inplace=True)
# dfG_ext.rename(columns={'Cultural Sites':"CulturalSites"}, inplace=True)

# #For some reasons the German population is double rather than integer so lets fix that
# dfG_ext['Population'].fillna(0, inplace=True)
# dfG_ext['Population'] = dfG_ext['Population'].astype(int)
# %%
#Q1
# # Save to CSV files: ===================== !GRADER YOU WILL NEED TO UNCOMMENT THIS TO RUN CODE ON YOUR MACHINE!

# dfG_ext.to_csv('german_cities.csv', index=False)
# dfF_ext.to_csv('french_cities.csv', index=False) 
# %%
#Q1
#Calling the paths to the saved Dataframes
frenchpath=r"french_cities.csv"
Germpath=r"german_cities.csv"
# %%
#Establishing A spark Session
spark = SparkSession.builder \
    .appName("Group_Assignment") \
    .getOrCreate()
# %%
#Q1
#This will set and apply a schema for the French Cities dataset
F_schema = StructType([
    StructField("CityName", StringType(), nullable=False),
    StructField("Population", IntegerType(), nullable=False),
    StructField("Area", DoubleType(), nullable=False),
    StructField("CulturalSites", IntegerType(), nullable=False),
    StructField("CityID", IntegerType(), nullable=False),
])

french_cities = spark.read.format("csv").load(frenchpath, schema=F_schema)
# %%
#Q1
#This will set a schema for the German Cities Dataset
G_schema = StructType([
    StructField("CityName", StringType(), nullable=False),
    StructField("Population", IntegerType(), nullable=False),
    StructField("Area", DoubleType(), nullable=False),
    StructField("CulturalSites", IntegerType(), nullable=False),
    StructField("CityID", IntegerType(), nullable=False),
])


german_cities = spark.read.format("csv").load(Germpath, schema=G_schema)
# %%
#Q1
#Double checking that it all worked
#This is just to drop the first row of each dataframe which for some reason is just the column names and null values
german_cities=german_cities.dropna()
french_cities=french_cities.dropna()
#Show german_cities
german_cities.show()
# %%
# Q2
#Pyspark Dataframe Operation
#Here we are selecting both data frames and ordering them by area. 
#We can see French cities are a bit larger by area

#set start time 
q2_pyspark_start_time = time.time()

germ_city_sizes = german_cities.select("CityName", "Area", "Population").orderBy(col("Area").desc()).show()
french_city_sizes = french_cities.select("CityName", "Area", "Population").orderBy(col("Area").desc()).show()
#============================================

#stop timer 
q2_pyspark_end_time = time.time()
#calculate query time 
q2_pyspark_time = q2_pyspark_end_time - q2_pyspark_start_time
print(f"PySpark execution time: {q2_pyspark_time} seconds")
# %%
#Q2
#SQL Version
#Making temp views so I can use SQL
german_cities.createOrReplaceTempView("german_cities")
french_cities.createOrReplaceTempView("french_cities")

#set start time 
q2_sql_start_time = time.time()

# German City Call
germ_city_sizes = spark.sql("""
    
    SELECT CityName, Area, Population 
    FROM german_cities 
    ORDER BY Area DESC

""")
germ_city_sizes.show()

#French City Call
french_city_sizes = spark.sql("""
    
    SELECT CityName, Area, Population 
    FROM french_cities 
    ORDER BY Area DESC

""")

#stop timer
q2_sql_end_time = time.time()
#calculate query time 
q2_sql_time = q2_sql_end_time - q2_sql_start_time
print(f"SQL execution time: {q2_sql_time} seconds")

# Show the result for French cities
french_city_sizes.show()

#Show difference in query execution times 
print(f"Performance difference: PySpark was {q2_pyspark_time / q2_sql_time:.2f} times {'slower' if q2_pyspark_time > q2_sql_time else 'faster'} than SQL")
# %%
#Q3

from pyspark.sql.functions import sum, avg, min, max, count

#pyspark solutions
print("PySpark Solutions:")

#set start time 
q3_pyspark_start_time = time.time()

#aggregate for german cities
germany_agg = german_cities.agg(sum("Population").alias("TotalPopulation"),
                                 avg("Area").alias("AverageArea"),
                                 min("Population").alias("MinPopulation"),
                                 max("Population").alias("MaxPopulation"),
                                 count("CityID").alias("CityCount"))
germany_agg.show()

#aggregate for french cities
france_agg = french_cities.agg(sum("Population").alias("TotalPopulation"),
                               avg("Area").alias("AverageArea"),
                               min("Population").alias("MinPopulation"),
                               max("Population").alias("MaxPopulation"),
                               count("CityID").alias("CityCount"))
france_agg.show()

#stop timer 
q3_pyspark_end_time = time.time()
#calculate query time 
q3_pyspark_time = q3_pyspark_end_time - q3_pyspark_start_time
print(f"PySpark execution time: {q3_pyspark_time} seconds")

#sql solutions
print("\nSQL Solutions:")

#set start time 
q3_sql_start_time = time.time()

#aggregate for german cities
germany_agg_sql = spark.sql("""
    SELECT SUM(Population) AS TotalPopulation,
           AVG(Area) AS AverageArea,
           MIN(Population) AS MinPopulation,
           MAX(Population) AS MaxPopulation,
           COUNT(CityID) AS CityCount
    FROM german_cities
""")
germany_agg_sql.show()

#aggregate for french cities
france_agg_sql = spark.sql("""
    SELECT SUM(Population) AS TotalPopulation,
           AVG(Area) AS AverageArea,
           MIN(Population) AS MinPopulation,
           MAX(Population) AS MaxPopulation,
           COUNT(CityID) AS CityCount
    FROM french_cities
""")

#stop timer 
q3_sql_end_time = time.time()
#calculate query time 
q3_sql_time = q3_sql_end_time - q3_sql_start_time
print(f"SQL execution time: {q3_sql_time} seconds")

france_agg_sql.show()

#Show difference in query execution times 
print(f"Performance difference: PySpark was {q3_pyspark_time / q3_sql_time:.2f} times {'slower' if q3_pyspark_time > q3_sql_time else 'faster'} than SQL")
# %%
#Q4

#set start time 
q4_pyspark_start_time = time.time()

germany_density = german_cities.withColumn("Density", col("Population") / col("Area"))
france_density = french_cities.withColumn("Density", col("Population") / col("Area"))
germany_density.select("CityName", "Density").show()
france_density.select("CityName", "Density").show()

#stop timer 
q4_pyspark_end_time = time.time()
#calculate query time 
q4_pyspark_time = q4_pyspark_end_time - q4_pyspark_start_time
print(f"PySpark execution time: {q4_pyspark_time} seconds")

# %%
#set start time
q4_sql_start_time = time.time()

# Germany Density Calculation
print("Germany")
density_german_stats = spark.sql("""
SELECT CityName, Population / Area AS Density
FROM german_cities

""")

density_german_stats.show()

# French Density Calculation
print("France")

density_french_stats = spark.sql("""
SELECT CityName, Population / Area AS Density
FROM french_cities

""")

density_french_stats.show()

#stop timer 
q4_sql_end_time = time.time()
#calculate query time 
q4_sql_time = q4_sql_end_time - q4_sql_start_time
print(f"SQL execution time: {q4_sql_time} seconds")

#Show difference in query execution times 
print(f"Performance difference: PySpark was {q4_pyspark_time / q4_sql_time:.2f} times {'slower' if q4_pyspark_time > q4_sql_time else 'faster'} than SQL")


# %%
#Q5

#set start time 
q5_pyspark_start_time = time.time()

full_outer_join_df = german_cities.join(french_cities, "CityID", "full_outer")\
                                   .select(german_cities["CityName"].alias("GermanCityName"),
                                           french_cities["CityName"].alias("FrenchCityName"),
                                           german_cities["Population"].alias("PopulationInGermany"),
                                           french_cities["Population"].alias("PopulationInFrance"))

# Show the combined DataFrame
full_outer_join_df.show()

#stop timer 
q5_pyspark_end_time = time.time()
#calculate query time 
q5_pyspark_time = q5_pyspark_end_time - q5_pyspark_start_time
print(f"PySpark execution time: {q5_pyspark_time} seconds")

# %%
#Q5 SQL

#set start time 
q5_sql_start_time = time.time()

# SQL Query to perform full outer join 
query = """
SELECT 
    g.CityName AS GermanCityName,
    f.CityName AS FrenchCityName,
    g.Population AS PopulationInGermany,
    f.Population AS PopulationInFrance
FROM german_cities g
FULL OUTER JOIN french_cities f ON g.CityID = f.CityID
"""

# Execute the SQL query using Spark SQL
full_outer_join_df = spark.sql(query)

# Show the results of the SQL query
full_outer_join_df.show()

#stop timer 
q5_sql_end_time = time.time()
#calculate query time 
q5_sql_time = q5_sql_end_time - q5_sql_start_time
print(f"SQL execution time: {q5_sql_time} seconds")

#Show difference in query execution times 
print(f"Performance difference: PySpark was {q5_pyspark_time / q5_sql_time:.2f} times {'slower' if q5_pyspark_time > q5_sql_time else 'faster'} than SQL")
# %%
#Q6
#Pyspark Dataframe operation

#set start time 
q6_pyspark_start_time = time.time()

#For this question I am using
print("Germany")
german_cities.select(
    avg("CityID").alias("Avg_CityID"),
    avg("Population").alias("Avg_Population"),
    avg("Area").alias("Avg_Area"),
    avg("CulturalSites").alias("Avg_CulturalSites")
).show()

print("France")
french_cities.select(
    avg("CityID").alias("Avg_CityID"),
    avg("Population").alias("Avg_Population"),
    avg("Area").alias("Avg_Area"),
    avg("CulturalSites").alias("Avg_CulturalSites")
).show()


#As we can see they are similar sizes, with Germany having slightly more cultural sites and population
#This makes sense that both would be similar as they are randomly generated via the same parameters

#stop timer 
q6_pyspark_end_time = time.time()
#calculate query time 
q6_pyspark_time = q6_pyspark_end_time - q6_pyspark_start_time
print(f"PySpark execution time: {q6_pyspark_time} seconds")

# %%
#Q6
#SQL Version

#set start time 
q6_sql_start_time = time.time()

#Using the previously created views in Q2
#German query
avg_german_stats = spark.sql("""
    
    SELECT AVG(CityID) AS Avg_CityID,
           AVG(Population) AS Avg_Population,
           AVG(Area) AS Avg_Area,
           AVG(CulturalSites) AS Avg_CulturalSites
    FROM german_cities
    
""")

avg_german_stats.show()

# French Query
avg_french_stats = spark.sql("""

    SELECT AVG(CityID) AS Avg_CityID,
           AVG(Population) AS Avg_Population,
           AVG(Area) AS Avg_Area,
           AVG(CulturalSites) AS Avg_CulturalSites
    FROM french_cities
    
""")
avg_french_stats.show()

#stop timer 
q6_sql_end_time = time.time()
#calculate query time 
q6_sql_time = q6_sql_end_time - q6_sql_start_time
print(f"SQL execution time: {q6_sql_time} seconds")

#Show difference in query execution times 
print(f"Performance difference: PySpark was {q6_pyspark_time / q6_sql_time:.2f} times {'slower' if q6_pyspark_time > q6_sql_time else 'faster'} than SQL")
# %%
# Q7
# Measure the start time of the process
q7_french_start_time = time.time()

# Order the dataset by population in descending order and select the top 20 cities
top_20_cities = french_cities.orderBy("Population", ascending=False).limit(20)
city_names = top_20_cities.select("CityName").collect()# Collect the names of these cities from the DataFrame
cultural_sites = top_20_cities.select("CulturalSites").collect()# Collect the number of cultural sites for each city from the DataFrame
city_names = [row.CityName for row in city_names]# Extract the city names into a list for easier manipulation later
cultural_sites = [row.CulturalSites for row in cultural_sites]# Extract the cultural sites data into a list

# Create a dictionary pairing city names with their corresponding cultural sites counts
data_dict = {'CityName': city_names, 'CulturalSites': cultural_sites}
df = pd.DataFrame(data_dict)# Convert the dictionary to a DataFrame for visualization

# Create a bar chart using Plotly Express to visualize the number of cultural sites in each city
fig = px.bar(df, x="CityName", y="CulturalSites",
             title="Cultural Sites in Top 20 Populous French Cities")
fig.update_layout(xaxis_title="City", yaxis_title="Number of Cultural Sites")# Create a bar chart using Plotly Express to visualize the number of cultural sites in each city
fig.show()# Display the bar chart


q7_french_end_time = time.time()# Measure the end time of the process
q7_french_time = q7_french_end_time - q7_french_start_time# Calculate the total execution time of the visualization process
print(f"frech Viz execution time: {q7_french_time} seconds")# Print the execution time

# %%
#Q7
q7_german_start_time = time.time()# Measure the start time of the process for performance analysis

# Order the German cities dataset by population in descending order and select the top 20 cities
top_20_cities_german = german_cities.orderBy("Population", ascending=False).limit(20)
city_names = top_20_cities_german.select("CityName").collect()# Collect the names of these top 20 cities from the DataFrame
cultural_sites = top_20_cities_german.select("CulturalSites").collect()# Collect the number of cultural sites for each of these cities from the DataFrame
city_names = [row.CityName for row in city_names]# Convert the collected city names into a list for further use
cultural_sites = [row.CulturalSites for row in cultural_sites]# Convert the collected number of cultural sites into a list
data_dict = {'CityName': city_names, 'CulturalSites': cultural_sites}# Create a dictionary that maps city names to their corresponding number of cultural sites
df = pd.DataFrame(data_dict)# Convert the dictionary into a DataFrame for easier visualization

# Create a bar chart using Plotly Express to show the number of cultural sites in each top city
fig = px.bar(df, x="CityName", y="CulturalSites",
             title="Cultural Sites in Top 20 Populous German Cities")
fig.update_layout(xaxis_title="City", yaxis_title="Number of Cultural Sites")# Update the chart layout to include more informative axis titles

fig.show()

q7_german_end_time = time.time()# Measure the end time of the process

q7_german_time = q7_german_end_time - q7_german_start_time# Calculate the total execution time of the visualization process

print(f"German Viz execution time: {q7_german_time} seconds")# Print the execution time to monitor performance

# %%
