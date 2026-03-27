from pyspark.sql import SparkSession
from pyspark.sql.functions import length

# =========================================================
# 🚀 1. START SPARK SESSION
# =========================================================
print("\n🚀 Starting Spark Session...\n")

spark = (
    SparkSession.builder
    .appName("SocialMediaPhase1RawDataInspection")
    .master("local[*]")
    .getOrCreate()
)

print("✅ Spark Session started successfully!\n")


# =========================================================
# 📂 2. DEFINE FILE PATHS
# =========================================================
print("📂 Defining file paths...\n")

posts_path = "data/raw/posts.csv"
engagement_path = "data/raw/engagement.csv"
users_path = "data/raw/users.csv"

print(f"Posts path: {posts_path}")
print(f"Engagement path: {engagement_path}")
print(f"Users path: {users_path}\n")


# =========================================================
# 📥 3. LOAD DATA INTO DATAFRAMES
# =========================================================
print("📥 Loading raw data into Spark DataFrames...\n")

posts_df = spark.read.option("header", True).option("inferSchema", True).csv(posts_path)
engagement_df = spark.read.option("header", True).option("inferSchema", True).csv(engagement_path)
users_df = spark.read.option("header", True).option("inferSchema", True).csv(users_path)

print("✅ Data loaded successfully!\n")


# =========================================================
# 👀 4. DISPLAY SAMPLE DATA
# =========================================================
print("👀 Preview: POSTS DATA")
posts_df.show(5, truncate=False)

print("\n👀 Preview: ENGAGEMENT DATA")
engagement_df.show(5, truncate=False)

print("\n👀 Preview: USERS DATA")
users_df.show(5, truncate=False)


# =========================================================
# 🧱 5. SCHEMA INSPECTION
# =========================================================
print("\n🧱 POSTS SCHEMA")
posts_df.printSchema()

print("\n🧱 ENGAGEMENT SCHEMA")
engagement_df.printSchema()

print("\n🧱 USERS SCHEMA")
users_df.printSchema()


# =========================================================
# 🏷️ 6. COLUMN NAMES
# =========================================================
print("\n🏷️ COLUMN NAMES")

print(f"Posts Columns: {posts_df.columns}")
print(f"Engagement Columns: {engagement_df.columns}")
print(f"Users Columns: {users_df.columns}")


# =========================================================
# 🔢 7. RECORD COUNTS
# =========================================================
print("\n🔢 COUNTING RECORDS (this triggers Spark execution)")

print(f"Posts Count: {posts_df.count()}")
print(f"Engagement Count: {engagement_df.count()}")
print(f"Users Count: {users_df.count()}")


# =========================================================
# 🔍 8. BASIC TRANSFORMATIONS
# =========================================================

# --- Select important columns
print("\n🔍 Selecting key columns from POSTS dataset")

posts_selected_df = posts_df.select(
    "post_id",
    "user_id",
    "category",
    "post_type",
    "created_at",
    "region"
)

posts_selected_df.show(5, truncate=False)


# --- Filter by category
print("\n🔍 Filtering POSTS for category = 'Technology'")

technology_posts_df = posts_df.filter(posts_df.category == "Technology")
technology_posts_df.show(5, truncate=False)


# --- Derived column
print("\n🧠 Creating derived column: category_name_length")

posts_with_length_df = posts_df.withColumn(
    "category_name_length",
    length(posts_df.category)
)

posts_with_length_df.show(5, truncate=False)


# =========================================================
# 🛑 9. STOP SPARK SESSION
# =========================================================
print("\n🛑 Stopping Spark Session...")

spark.stop()

print("✅ Spark Session stopped. Phase 1 completed successfully!\n")