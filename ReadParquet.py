import AppConfig as app
import os

from azure.storage.blob import ContainerClient
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

os.environ["HADOOP_HOME"] = "C:\\hadoop"

spark = SparkSession.builder \
    .appName("Join ID") \
    .config("spark.local.dir", "D:\\TempSpark\\") \
    .config("spark.executor.memory", "8g") \
    .config("spark.driver.memory", "8g") \
    .getOrCreate()

IndentityRelay42blob = "C:/Dev/Resource/IdentityRelay42/"
PageViewblob = "C:/Dev/Resource/pageview/"
Campaign  = "C:/Dev/Resource/Campaign/flexidrive.csv"

df1 = spark.read.load(PageViewblob)
df1 = df1.select("track_id")
df1 = df1.withColumnRenamed("track_id", "trackId")
distinct_df1 = df1.select("trackId").distinct()

df2 = spark.read.load(IndentityRelay42blob)
#df2 = df2.withColumnRenamed("Value", "trackId")
distinct_df2_Value = df2.select("UUID").distinct()

df3 = spark.read.csv(Campaign)
df3 = df3.select("_c0")
df3 = df3.withColumnRenamed("_c0", "trackId")

df = distinct_df1.join(df2, (distinct_df1.trackId == df2.Value) & (distinct_df1.trackId == distinct_df2_Value.UUID))
JoinAll = df.join(df3, (df3.trackId == df.trackId) & (df3.trackId == df.Value))

print("All", JoinAll.count())

#joined_Value_df = distinct_df1.join(distinct_df2_Value, on="trackId", how="inner")
#print(f"Pageview Count: {df1.count()} Disitnct: {distinct_df1.count()}")
#print(f"ID Count: {df2.count()} Distinct: {distinct_df2_Value.count()}")
#print(f"Join Disitnct Value: {joined_Value_df.count()}")
spark.stop()