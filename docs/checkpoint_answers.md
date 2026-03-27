##################################################################################
PHASE 1
##################################################################################

Task 1
Run the script successfully and confirm that all three raw files are loaded into Spark. DONE

####################################

Log in WSL then navigate to project 
/mnt/c/Users/ghala/github-classroom/cloud-data-engineering-01/Social-Media-Engagement-Analytics-Pipeline


python3 -m venv .venv
source .venv/bin/activate
pip install pyspark
python3 src/phase1_raw_data_inspection.py
####################################

Task 2
Record the row counts for:

posts
engagement
users
############################
🔢 COUNTING RECORDS (this triggers Spark execution)
Posts Count: 228
Engagement Count: 440
Users Count: 233
############################

Task 3
Write down the schema observations:

Which columns are strings?
Which columns are numeric?
Are any columns inferred differently than you expected?

#################################
🧱 POSTS SCHEMA
root
 |-- post_id: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- category: string (nullable = true)
 |-- post_type: string (nullable = true)
 |-- content_length: integer (nullable = true)
 |-- created_at: string (nullable = true)
 |-- region: string (nullable = true)
 |-- title: string (nullable = true)


🧱 ENGAGEMENT SCHEMA
root
 |-- engagement_id: string (nullable = true)
 |-- post_id: string (nullable = true)
 |-- engagement_type: string (nullable = true)
 |-- engagement_value: string (nullable = true)
 |-- user_id: string (nullable = true)
 |-- timestamp: string (nullable = true)


🧱 USERS SCHEMA
root
 |-- user_id: string (nullable = true)
 |-- username: string (nullable = true)
 |-- country: string (nullable = true)
 |-- account_type: string (nullable = true)
 |-- followers_count: string (nullable = true)
 |-- join_date: string (nullable = true)
#################################

Task 4
Write down at least three important business columns from each dataset.

####################################
Posts Columns: ['post_id', 'user_id', 'category', 'post_type', 'content_length', 'created_at', 'region', 'title']
Engagement Columns: ['engagement_id', 'post_id', 'engagement_type', 'engagement_value', 'user_id', 'timestamp']
Users Columns: ['user_id', 'username', 'country', 'account_type', 'followers_count', 'join_date']
####################################
Task 5
Explain in your own words:

Spark DataFrame : A Spark DataFrame is like an Excel table, but it can process huge amounts of data efficiently using Spark.

Schema :  A schema defines the structure of a DataFrame.


what the difference is between a transformation and an action : A transformation is an operation that defines what you want to do with the data, but does NOT execute immediately.

An action triggers execution.


##################################################################################
PHASE 2
##################################################################################
 Task 1 — Column Selection

We selected only relevant columns to reduce noise, improve performance, and focus on analysis needs (IDs, categories, engagement, and user attributes). Irrelevant fields were removed to simplify joins and aggregations.

 Task 2 — Cleaning Steps
Null normalization → converted empty strings/"NULL" to proper nulls for consistency.
Type casting → ensured numeric fields are usable for calculations.
Dropping invalid rows → removed records missing key IDs to ensure correct joins.


 Task 3 — Duplicate Impact

Duplicates inflate engagement metrics, causing incorrect totals and misleading rankings of posts or creators, leading to wrong business decisions.

 Task 4 — engagement_score Meaning

A weighted metric for interaction strength:

Like = 1
Comment = 2
Share = 3
It reflects engagement quality, not just quantity.

 Task 5 — Grouped Summaries
Category engagement → identifies most engaging content types
Creator engagement → ranks influencers by engagement
Region engagement → shows engagement distribution by geography


 Task 6 — DataFrame API vs Spark SQL
DataFrame API: code-based transformations, flexible and step-by-step
Spark SQL: SQL queries for fast aggregation and reporting


 Task 7 — UDF Usage

Used to apply custom logic (classifying creators by follower count) that is not available in built-in Spark functions.


##################################################################################
PHASE 3 
##################################################################################

Task 1 — Why cache filtered_social_media_df?

Because it is reused multiple times (aggregations + partitions + actions). Caching avoids recomputing joins and transformations every time.

Task 2 — What caching does in Spark

Caching stores a DataFrame in memory after first computation so Spark can reuse it instead of recalculating it again.

Task 3 — Partitions observation

Example:

filtered_social_media_df: more partitions (from joins)
category_engagement_df: fewer partitions (after groupBy)

GroupBy and joins change partition distribution due to shuffle.

Task 4 — repartition vs coalesce
repartition() → full shuffle, can increase or decrease partitions, expensive
coalesce() → reduces partitions without full shuffle, cheaper


Task 5 — Small files problem

Too many small files slow down:

reading performance
storage metadata operations
downstream processing


Task 6 — What is Parquet?

Parquet is a column-based file format optimized for fast and efficient analytics.

Task 7 — CSV vs Parquet
Easier for humans: CSV
Better for analytics: Parquet

Parquet is faster, compressed, and reads only needed columns.

Task 8 — Why PostgreSQL is still useful

PostgreSQL is useful for transactional systems, real-time queries, and serving data to applications, while Spark is mainly for large-scale processing.