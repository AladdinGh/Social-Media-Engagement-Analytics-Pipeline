from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, trim, when, expr, spark_partition_id

spark = (
    SparkSession.builder
    .appName("SocialMediaPhase3OptimizationAndOutput")
    .master("local[*]")
    .getOrCreate()
)

# -------------------------
# DEBUG FUNCTION
# -------------------------
def debug_partitions(df, name, n=10):
    print("\n" + "="*60)
    print(f"📌 {name}")
    print("Partitions:", df.rdd.getNumPartitions())

    df.withColumn("partition_id", spark_partition_id()) \
      .select("partition_id", *df.columns) \
      .show(n, truncate=False)


# -------------------------
# LOAD DATA
# -------------------------
posts_df = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/posts.csv")
engagement_df = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/engagement.csv")
users_df = spark.read.option("header", True).option("inferSchema", True).csv("data/raw/users.csv")

posts_selected_df = posts_df.select("post_id","user_id","category","post_type","content_length","created_at","region")
engagement_selected_df = engagement_df.select("engagement_id","post_id","engagement_type","engagement_value","user_id","timestamp")
users_selected_df = users_df.select("user_id","username","country","account_type","followers_count","join_date")

users_renamed_df = users_selected_df.withColumnRenamed("user_id", "creator_user_id")

engagement_cast_df = engagement_selected_df.withColumn(
    "engagement_value",
    expr("try_cast(engagement_value as int)")
)

users_cast_df = users_renamed_df.withColumn(
    "followers_count",
    expr("try_cast(trim(followers_count) as int)")
)

posts_cast_df = posts_selected_df.withColumn("content_length", col("content_length").cast("int"))

posts_clean_df = posts_cast_df.dropna(subset=["post_id", "user_id", "category"])
engagement_clean_df = engagement_cast_df.dropna(subset=["engagement_id", "post_id", "engagement_type"])
users_clean_df = users_cast_df.dropna(subset=["creator_user_id", "username"])

posts_dedup_df = posts_clean_df.dropDuplicates(["post_id"])
engagement_dedup_df = engagement_clean_df.dropDuplicates(["engagement_id"])
users_dedup_df = users_clean_df.dropDuplicates(["creator_user_id"])

posts_standardized_df = (
    posts_dedup_df
    .withColumn("category", upper(trim(col("category"))))
    .withColumn("post_type", upper(trim(col("post_type"))))
    .withColumn("region", upper(trim(col("region"))))
)

engagement_standardized_df = engagement_dedup_df.withColumn(
    "engagement_type",
    upper(trim(col("engagement_type")))
)

engagement_scored_df = engagement_standardized_df.withColumn(
    "engagement_score",
    when(col("engagement_type") == "LIKE", 1)
    .when(col("engagement_type") == "COMMENT", 2)
    .when(col("engagement_type") == "SHARE", 3)
    .otherwise(0)
)

posts_engagement_df = posts_standardized_df.join(
    engagement_scored_df.drop("user_id"),
    on="post_id",
    how="inner"
)

full_social_media_df = posts_engagement_df.join(
    users_dedup_df,
    posts_engagement_df.user_id == users_dedup_df.creator_user_id,
    how="left"
)

filtered_social_media_df = full_social_media_df.filter(col("engagement_score") > 0).cache()
filtered_social_media_df.count()

# -------------------------
# 🔥 PARTITION DEBUG 1
# -------------------------
debug_partitions(filtered_social_media_df, "FILTERED SOCIAL MEDIA")

# -------------------------
# AGGREGATIONS
# -------------------------
category_engagement_df = filtered_social_media_df.groupBy("category").agg(
    {"engagement_score": "sum"}
).withColumnRenamed("sum(engagement_score)", "total_engagement_score")

creator_engagement_df = filtered_social_media_df.groupBy(
    "creator_user_id", "username"
).agg(
    {"engagement_score": "sum"}
).withColumnRenamed("sum(engagement_score)", "creator_total_engagement")

region_engagement_df = filtered_social_media_df.groupBy("region").agg(
    {"engagement_score": "sum"}
).withColumnRenamed("sum(engagement_score)", "region_total_engagement")

# -------------------------
# 🔥 PARTITION DEBUG 2 (BEFORE OPTIMIZATION)
# -------------------------
debug_partitions(category_engagement_df, "CATEGORY (ORIGINAL)")
debug_partitions(creator_engagement_df, "CREATOR (ORIGINAL)")
debug_partitions(region_engagement_df, "REGION (ORIGINAL)")

# -------------------------
# REPARTITION (shuffle)
# -------------------------
category_repart = category_engagement_df.repartition(2)
creator_repart = creator_engagement_df.repartition(2)
region_repart = region_engagement_df.repartition(2)

# -------------------------
# COALESCE (no shuffle)
# -------------------------
category_coal = category_engagement_df.coalesce(1)
region_coal = region_engagement_df.coalesce(1)

# -------------------------
# 🔥 PARTITION DEBUG 3 (AFTER OPTIMIZATION)
# -------------------------
debug_partitions(category_repart, "CATEGORY REPARTITION(2)")
debug_partitions(category_coal, "CATEGORY COALESCE(1)")

debug_partitions(region_repart, "REGION REPARTITION(2)")
debug_partitions(region_coal, "REGION COALESCE(1)")

# -------------------------
# WRITING OUTPUTS
# -------------------------
category_coal.write.mode("overwrite").csv("data/output/csv/category_engagement_summary", header=True)
creator_repart.write.mode("overwrite").csv("data/output/csv/creator_engagement_summary", header=True)
region_coal.write.mode("overwrite").csv("data/output/csv/region_engagement_summary", header=True)

category_repart.write.mode("overwrite").parquet("data/output/parquet/category_engagement_summary")
creator_repart.write.mode("overwrite").parquet("data/output/parquet/creator_engagement_summary")
region_repart.write.mode("overwrite").parquet("data/output/parquet/region_engagement_summary")

# -------------------------
# FINAL CHECK
# -------------------------
parquet_check_df = spark.read.parquet("data/output/parquet/category_engagement_summary")
parquet_check_df.show(truncate=False)
parquet_check_df.printSchema()

final_reporting_df = creator_engagement_df.select(
    "creator_user_id",
    "username",
    "creator_total_engagement"
)

final_reporting_df.show(truncate=False)

spark.stop()