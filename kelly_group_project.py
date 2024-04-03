import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from pyspark.sql.functions import *
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorAssembler
from pyspark.ml import Pipeline

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
taxi_data = spark.readStream.schema(schema).csv("C:\\Users\\pjk\\Desktop\\big_data\\archive\\")

#print streaming data to console
query = taxi_data.writeStream.format("console").outputMode("append").start()
query.awaitTermination()

#transform the data
transformed_data = taxi_data.select(
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
)

#basic eda/analysis
hourly_avg_fare = transformed_data.groupBy("pickup_hour").agg(avg("fare_amount").alias("avg_fare"))
hourly_avg_fare.writeStream.format("console").outputMode("complete").start().awaitTermination()

#####CONSOLE OUTPUT HANGS HERE

daily_avg_fare = transformed_data.groupBy("pickup_dayofweek").agg(avg("fare_amount").alias("avg_fare"))
daily_avg_fare.writeStream.format("console").outputMode("complete").start().awaitTermination()

#prepare data for forecasting
feature_cols = ["passenger_count", "trip_distance", "pickup_latitude", "pickup_longitude", "dropoff_latitude", "dropoff_longitude", "payment_type", "pickup_hour", "pickup_dayofweek"]
vector_assembler = VectorAssembler(inputCols=feature_cols, outputCol="features")

#random forest
rf_regressor = RandomForestRegressor(featuresCol="features", labelCol="total_amount")

#create pipeline
pipeline = Pipeline(stages=[vector_assembler, rf_regressor])

#train model
model = pipeline.fit(transformed_data)

#predictions
predictions = model.transform(transformed_data)
predictions.select("total_amount", "prediction").writeStream.format("console").outputMode("append").start().awaitTermination()