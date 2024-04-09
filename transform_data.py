import pandas as pd
import psycopg2
from psycopg2 import Error
from pyspark.sql import SparkSession
from pyspark.sql import SparkSession
from pyspark.sql import DataFrame as SparkDataFrame
from pyspark.sql.functions import col, regexp_replace, when, monotonically_increasing_id, row_number
from pyspark.sql import *
from pyspark.sql.window import Window   
from sqlalchemy import create_engine


host = 'localhost'
port = '5432'
database = 'makemytrip'
user = 'postgres'
password = '0409'

engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{database}')

query = "SELECT * FROM hotel_dataset"

df = pd.read_sql(query, engine)

spark = SparkSession.builder \
    .appName("DataTransformation") \
    .getOrCreate()

spark_df = spark.createDataFrame(df)

spark_df.cache()

spark_df = spark_df.fillna({'Tax': 0})
spark_df = spark_df.withColumn("Price", regexp_replace(col("Price"), ",", "").cast("integer"))
spark_df = spark_df.withColumn("Tax", regexp_replace(col("Tax"), ",", "").cast("integer"))
spark_df = spark_df.withColumn("Total Price", col("Price") + col("Tax"))

spark_df = spark_df.withColumn("Hotel Category", 
                   when(col("Star Rating") == 5, "Luxury")
                   .when(col("Star Rating") == 4, "Premium")
                   .otherwise("Standard"))

spark_df = spark_df.withColumn("Tax Applied", when(col("Tax") > 0, "Yes").otherwise("No"))

spark_df = spark_df.withColumn("Price Range Category", 
                   when(col("Total Price") < 3000, "Low")
                   .when((col("Total Price") >= 3000) & (col("Total Price") < 6000), "Medium")
                   .otherwise("High"))

spark_df = spark_df.withColumn("Review Sentiment", 
                   when(col("Rating Description").isin(["Excellent", "Very Good"]), "Positive")
                   .otherwise("Negative"))

window_spec = Window.orderBy(monotonically_increasing_id())
spark_df = spark_df.withColumn("Hotel_ID", (row_number().over(window_spec) - 1))

hotel_table = spark_df.select("Hotel_ID", "Hotel Name", "Rating", "Rating Description", "Reviews", 
                                "Star Rating", "Location", "Nearest Landmark", "Distance to Landmark", 
                                "Hotel Category", "Review Sentiment")

pricing_table = spark_df.select("Hotel_ID", "Hotel Name", "Price", "Tax", "Total Price", "Tax Applied", 
                          "Price Range Category")

hotel_details_df = hotel_table.toPandas()
pricing_df = pricing_table.toPandas()

hotel_details_df.to_sql(name='hotel_details', con=engine, if_exists='replace', index=False)
pricing_df.to_sql(name='pricing', con=engine, if_exists='replace', index=False)
engine.dispose()

print("transformation completed")
print("uploaded transformed files to database")

