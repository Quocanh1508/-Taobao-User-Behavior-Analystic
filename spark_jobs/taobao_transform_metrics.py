import argparse
from pyspark.sql import SparkSession, Window
import pyspark.sql.functions as F

def get_spark_session(app_name="Taobao_Data_Transform_Metrics"):
    return SparkSession.builder \
        .appName(app_name) \
        .config("spark.sql.parquet.compression.codec", "snappy") \
        .config("spark.sql.shuffle.partitions", "200") \
        .getOrCreate()

def transform_metrics(spark, input_path, output_fact_path, output_metrics_path):
    # Enable Broadcast Join threshold config (showcase)
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760") # 10MB
    
    print(f"Reading cleansed parquet data from: {input_path}")
    base_df = spark.read.parquet(input_path)
    
    # Cache the dataframe since we will use it for Facts + Metrics
    base_df.cache()

    # 1. Sessionization (Gap < 30 mins)
    w_lag = Window.partitionBy("user_id").orderBy("event_time")
    
    # Calculate prev_time and the gap
    sess_df = base_df.withColumn("prev_time", F.lag("event_time").over(w_lag)) \
                     .withColumn("gap_seconds", F.unix_timestamp("event_time") - F.unix_timestamp("prev_time"))
                     
    # Flag new sessions: first event or gap > 1800s (30m)
    sess_df = sess_df.withColumn("is_new_session", 
        F.when(F.col("gap_seconds") > 1800, 1)
         .when(F.col("prev_time").isNull(), 1)
         .otherwise(0)
    )

    # Cumulative sum over user's events to generate a session index, then session_id
    w_sum = Window.partitionBy("user_id").orderBy("event_time").rowsBetween(Window.unboundedPreceding, Window.currentRow)
    
    fact_df = sess_df.withColumn("session_idx", F.sum("is_new_session").over(w_sum)) \
                     .withColumn("session_id", F.concat(F.col("user_id").cast("string"), F.lit("_"), F.col("session_idx").cast("string")))

    # Keep relevant fields for Processed_Zone/Taobao/Fact_Events/
    # Ensure optimal files
    fact_output = fact_df.select(
        "user_id", "item_id", "category_id", "behavior_type", 
        "event_time", "event_date", "session_id"
    ).repartition("event_date")
    
    print(f"Writing Fact Events to: {output_fact_path}")
    fact_output.write.mode("overwrite").partitionBy("event_date").parquet(output_fact_path)

    # 2. Funnel Analysis per session
    funnel_df = fact_output.groupBy("event_date", "session_id").agg(
        F.min(F.when(F.col("behavior_type") == "pv", F.col("event_time"))).alias("first_pv"),
        F.min(F.when(F.col("behavior_type") == "cart", F.col("event_time"))).alias("first_cart"),
        F.min(F.when(F.col("behavior_type") == "buy", F.col("event_time"))).alias("first_buy")
    )
    
    # Compute durations (if order strictly respects pv <= cart <= buy visually, neg filtering)
    funnel_df = funnel_df.withColumn("pv_to_cart_sec", F.unix_timestamp("first_cart") - F.unix_timestamp("first_pv")) \
                         .withColumn("cart_to_buy_sec", F.unix_timestamp("first_buy") - F.unix_timestamp("first_cart")) \
                         .withColumn("pv_to_buy_sec", F.unix_timestamp("first_buy") - F.unix_timestamp("first_pv"))
                         
    # Calculate daily aggregated metrics
    daily_funnel = funnel_df.groupBy("event_date").agg(
        F.avg(F.when(F.col("pv_to_cart_sec") > 0, F.col("pv_to_cart_sec"))).alias("avg_pv_to_cart_sec"),
        F.avg(F.when(F.col("cart_to_buy_sec") > 0, F.col("cart_to_buy_sec"))).alias("avg_cart_to_buy_sec"),
        F.avg(F.when(F.col("pv_to_buy_sec") > 0, F.col("pv_to_buy_sec"))).alias("avg_pv_to_buy_sec")
    )

    # 3. Metric Aggregation: DAU
    dau_df = base_df.groupBy("event_date").agg(F.countDistinct("user_id").alias("dau"))

    # 4. Metric Aggregation: Top 10 categories
    # Finding top 10 globally per day
    top_cat_df = base_df.filter(F.col("behavior_type") == "buy") \
        .groupBy("event_date", "category_id").agg(F.count("*").alias("purchase_count"))
        
    w_rank = Window.partitionBy("event_date").orderBy(F.col("purchase_count").desc())
    top_10_cats = top_cat_df.withColumn("rank", F.rank().over(w_rank)).filter(F.col("rank") <= 10)
    
    # Pack top 10 into an array per day
    top_10_daily = top_10_cats.groupBy("event_date").agg(F.collect_list("category_id").alias("top_10_categories_bought"))

    # 5. Join final metrics and Output
    # We join DAU and funnel metrics
    final_metrics = dau_df.join(daily_funnel, "event_date", "left") \
                          .join(top_10_daily, "event_date", "left") \
                          .coalesce(1) # We just want a single simple file per day for metrics
    
    print(f"Writing daily metrics to: {output_metrics_path}")
    final_metrics.write.mode("overwrite").partitionBy("event_date").parquet(output_metrics_path)
    
    base_df.unpersist()
    print("Metrics Job Completed Successfully!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--input-path", required=True, help="Path to cleansed Parquet data.")
    parser.add_argument("--output-fact", required=True, help="Base path for Fact Events output.")
    parser.add_argument("--output-metrics", required=True, help="Base path for Metrics Daily output.")
    args = parser.parse_args()
    
    spark_session = get_spark_session()
    transform_metrics(spark_session, args.input_path, args.output_fact, args.output_metrics)
    spark_session.stop()
