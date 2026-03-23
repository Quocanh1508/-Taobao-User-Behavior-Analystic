import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, LongType
import pyspark.sql.functions as F

def get_spark_session(app_name="Taobao_Data_Cleansing"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def cleanse_data(spark, input_path, output_path):
    print(f"Reading raw data from: {input_path}")
    
    # 1. Define explicit schema to avoid inference overhead
    schema = StructType([
        StructField("user_id", IntegerType(), True),
        StructField("item_id", IntegerType(), True),
        StructField("category_id", IntegerType(), True),
        StructField("behavior_type", StringType(), True),
        StructField("event_ts", LongType(), True),
        StructField("ingested_at", StringType(), True),
    ])

    # 2. Read JSON
    raw_df = spark.read.schema(schema).json(input_path)

    # Cache if we were going to reuse, but since it's an ETL chain we just chain transformations.
    # Optional execution tracking
    # raw_df.cache()

    # 3. Filtering & Cleansing Logic
    critical_cols = ["user_id", "item_id", "behavior_type", "event_ts"]
    
    # a. Drop nulls
    clean_df = raw_df.dropna(subset=critical_cols)
    
    # b. Drop duplicates
    clean_df = clean_df.dropDuplicates(subset=critical_cols)
    
    # c. Remove invalid timestamps (Dataset should strictly be around 2017 Nov-Dec)
    # 1500000000 = 2017-07-14, 1550000000 = 2019-02-12
    clean_df = clean_df.filter((F.col("event_ts") > 1500000000) & (F.col("event_ts") < 1550000000))
    
    # d. Convert timestamp
    transformed_df = clean_df \
        .withColumn("event_time", F.to_timestamp(F.from_unixtime("event_ts"))) \
        .withColumn("event_date", F.to_date("event_time"))

    # Show execution plan and a few rows during dev/checkpoint showcase
    print("Sample Schema:")
    transformed_df.printSchema()
    
    # 4. Write to Parquet (Snappy enabled via Spark config)
    # Output will be partitioned by 'event_date'
    print(f"Writing cleansed Parquet data to: {output_path}")
    transformed_df \
        .write \
        .mode("overwrite") \
        .partitionBy("event_date") \
        .parquet(output_path)
    
    print("Cleansing Job Completed Successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="Path to raw JSON data for the specific ds e.g., Raw_Zone/Taobao/ingestion_date=.../*/*.json")
    parser.add_argument("--output-path", required=True, help="Base path for cleansed data output.")
    args = parser.parse_args()
    
    spark_session = get_spark_session()
    cleanse_data(spark_session, args.input_path, args.output_path)
    spark_session.stop()
