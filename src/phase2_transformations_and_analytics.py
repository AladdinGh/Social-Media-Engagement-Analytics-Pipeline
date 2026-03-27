from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, upper, trim, when, udf, expr
)
from pyspark.sql.types import StringType


# =========================
# SIMPLE LOGGER
# =========================
def log(step, df):
    print("\n" + "="*60)
    print(f"📌 {step}")
    print(f"📊 ROWS: {df.count()}")
    df.show(10, False)
    print("="*60 + "\n")

# =========================
# SPARK SESSION
# =========================
spark = (
    SparkSession.builder
    .appName("SocialMediaPhase2DebugPipeline")
    .master("local[*]")
    .getOrCreate()
)

# =========================
# PATHS
# =========================
posts_path = "data/raw/posts.csv"
engagement_path = "data/raw/engagement.csv"
users_path = "data/raw/users.csv"

# =========================
# LOAD DATA
# =========================
posts_df = spark.read.option("header", True).csv(posts_path)
engagement_df = spark.read.option("header", True).csv(engagement_path)
users_df = spark.read.option("header", True).csv(users_path)

log("RAW POSTS", posts_df)
log("RAW ENGAGEMENT", engagement_df)
log("RAW USERS", users_df)

# =========================
# CLEAN NULLS
# =========================
def clean_nulls(df):
    for c in df.columns:
        df = df.withColumn(
            c,
            when(
                (col(c).isNull()) |
                (trim(col(c)) == "") |
                (upper(trim(col(c))) == "NULL"),
                None
            ).otherwise(col(c))
        )
    return df

posts_df = clean_nulls(posts_df)
engagement_df = clean_nulls(engagement_df)
users_df = clean_nulls(users_df)

log("AFTER NULL CLEANING - POSTS", posts_df)
log("AFTER NULL CLEANING - ENGAGEMENT", engagement_df)
log("AFTER NULL CLEANING - USERS", users_df)

# =========================
# SELECT
# =========================
posts_selected_df = posts_df.select(
    "post_id", "user_id", "category", "post_type",
    "content_length", "created_at", "region"
)

engagement_selected_df = engagement_df.select(
    "engagement_id", "post_id", "engagement_type",
    "engagement_value", "user_id", "timestamp"
)

users_selected_df = users_df.select(
    "user_id", "username", "country",
    "account_type", "followers_count", "join_date"
)

log("SELECTED POSTS", posts_selected_df)
log("SELECTED ENGAGEMENT", engagement_selected_df)
log("SELECTED USERS", users_selected_df)

# =========================
# RENAME
# =========================
users_selected_df = users_selected_df.withColumnRenamed("user_id", "creator_user_id")
posts_selected_df = posts_selected_df.withColumnRenamed("user_id", "creator_user_id")
engagement_selected_df = engagement_selected_df.withColumnRenamed("user_id", "engager_user_id")

log("RENAMED POSTS", posts_selected_df)
log("RENAMED ENGAGEMENT", engagement_selected_df)
log("RENAMED USERS", users_selected_df)

# =========================
# CAST
# =========================
posts_cast_df = posts_selected_df.withColumn(
    "content_length",
    expr("try_cast(content_length as int)")
)

engagement_cast_df = engagement_selected_df.withColumn(
    "engagement_value",
    expr("try_cast(engagement_value as int)")
)

users_cast_df = users_selected_df.withColumn(
    "followers_count",
    expr("try_cast(followers_count as int)")
)

log("CAST POSTS", posts_cast_df)
log("CAST ENGAGEMENT", engagement_cast_df)
log("CAST USERS", users_cast_df)

# =========================
# DROP NULLS
# =========================
posts_clean_df = posts_cast_df.dropna(subset=["post_id", "creator_user_id", "category"])
engagement_clean_df = engagement_cast_df.dropna(subset=["engagement_id", "post_id", "engagement_type"])
users_clean_df = users_cast_df.dropna(subset=["creator_user_id", "username"])

log("POSTS CLEAN", posts_clean_df)
log("ENGAGEMENT CLEAN", engagement_clean_df)
log("USERS CLEAN", users_clean_df)

# =========================
# DEDUP
# =========================
posts_dedup_df = posts_clean_df.dropDuplicates(["post_id"])
engagement_dedup_df = engagement_clean_df.dropDuplicates(["engagement_id"])
users_dedup_df = users_clean_df.dropDuplicates(["creator_user_id"])

log("POSTS DEDUP", posts_dedup_df)
log("ENGAGEMENT DEDUP", engagement_dedup_df)
log("USERS DEDUP", users_dedup_df)

# =========================
# STANDARDIZE
# =========================
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

log("STANDARDIZED POSTS", posts_standardized_df)
log("STANDARDIZED ENGAGEMENT", engagement_standardized_df)

# =========================
# SCORING
# =========================
engagement_scored_df = engagement_standardized_df.withColumn(
    "engagement_score",
    when(col("engagement_type").contains("LIKE"), 1)
    .when(col("engagement_type").contains("COMMENT"), 2)
    .when(col("engagement_type").contains("SHARE"), 3)
    .otherwise(0)
)

log("ENGAGEMENT SCORED", engagement_scored_df)

# =========================
# JOINS (CRITICAL DEBUG POINT)
# =========================
posts_engagement_df = posts_standardized_df.join(
    engagement_scored_df,
    on="post_id",
    how="inner"
)

log("AFTER POSTS x ENGAGEMENT JOIN", posts_engagement_df)

full_social_media_df = posts_engagement_df.join(
    users_dedup_df,
    on="creator_user_id",
    how="left"
)

log("AFTER USERS JOIN (FULL DATA)", full_social_media_df)

# =========================
# FILTER
# =========================
filtered_social_media_df = full_social_media_df.filter(
    col("engagement_score") > 0
)

log("FINAL FILTERED DATA", filtered_social_media_df)

# =========================
# ANALYTICS
# =========================
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

log("CATEGORY ANALYTICS", category_engagement_df)
log("CREATOR ANALYTICS", creator_engagement_df)
log("REGION ANALYTICS", region_engagement_df)




# =========================
filtered_social_media_df.createOrReplaceTempView("social_media_activity")

top_categories_sql_df = spark.sql("""
    SELECT
        category,
        SUM(engagement_score) AS total_engagement_score,
        COUNT(DISTINCT post_id) AS total_posts
    FROM social_media_activity
    GROUP BY category
    ORDER BY total_engagement_score DESC
""")
print ("TOP CATEGORIES - SQL ANALYTICS")
top_categories_sql_df.show(truncate=False)

def classify_creator_size(followers_count):
    if followers_count is None:
        return "UNKNOWN"
    elif followers_count < 1000:
        return "SMALL"
    elif followers_count < 10000:
        return "MEDIUM"
    else:
        return "LARGE"

classify_creator_size_udf = udf(classify_creator_size, StringType())


creator_classified_df = filtered_social_media_df.withColumn(
    "creator_size",
    classify_creator_size_udf(col("followers_count"))
)

print("CREATOR SIZE CLASSIFICATION Usinf UDF")
creator_classified_df.select(
    "creator_user_id",
    "username",
    "followers_count",
    "creator_size"
).show(10, truncate=False)


category_engagement_df.write.mode("overwrite").csv(
    "data/processed/category_engagement_summary",
    header=True
)

creator_engagement_df.write.mode("overwrite").csv(
    "data/processed/creator_engagement_summary",
    header=True
)

region_engagement_df.write.mode("overwrite").csv(
    "data/processed/region_engagement_summary",
    header=True
)



# =========================
# STOP
# =========================
spark.stop()