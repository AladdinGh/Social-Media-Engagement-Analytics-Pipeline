# Phase 1 — Spark Foundations and Raw Data Understanding

Welcome to **Phase 1** of the Social Media Engagement Analytics Pipeline project.

This phase is the foundation of the whole mini-project. Before a data engineering team can clean data, analyze it, optimize it, or load final outputs into a database, the team must first understand the raw data and the processing engine they are using.

That is the purpose of this phase.

In this phase, you will begin working as a junior data engineer on a social media analytics pipeline. You will start a Spark session, load raw datasets, inspect their structure, study the schema, and begin understanding how Spark works with data.

This phase is based on the learning from:

* **Week 3 — Day 1: Intro to Spark & Cluster Computing Concepts**

---

## Phase Objective

By the end of this phase, you should be able to:

* understand the role of Spark in data engineering
* start a Spark session
* load raw social media files into Spark DataFrames
* inspect rows and columns
* understand what a schema is
* explain why data types matter
* identify important fields in the dataset
* understand the difference between a **transformation** and an **action**
* begin thinking like a Spark engineer instead of only a Python programmer

---

## Why This Phase Matters in Real Work

In real companies, engineers do not immediately jump into transformations and analytics.

When raw data arrives from source systems, the first responsibilities are usually:

* confirm the files are readable
* inspect the shape of the data
* understand the columns
* verify data types
* spot obvious data quality problems
* decide what needs cleaning later

If this first step is skipped, the rest of the pipeline becomes risky.

For example:

* a date column may actually be stored as plain text
* a numeric column may contain invalid values
* a region field may have inconsistent spellings
* a file may contain duplicate or incomplete records

That is why this phase exists. It simulates the first realistic step in data engineering work: **understand the raw data before changing it**.

---

# What You Will Build in This Phase

In this phase, you will:

1. prepare your environment
2. start a Spark session
3. read raw social media files into Spark DataFrames
4. inspect the datasets
5. study the schema
6. identify important business columns
7. practice simple transformations and actions
8. save your observations for later phases

This is a guided phase.
You are not expected to invent the code on your own from scratch. The code is provided step by step, and each step includes explanation of:

* the theory
* the business reason
* the technical implementation

---

# Dataset Context for This Phase

In this project, you are working with raw exported data from a social media platform.

You may work with files such as:

* `posts.csv`
* `engagement.csv`
* `users.csv`

These files may contain fields such as:

## posts

* `post_id`
* `user_id`
* `category`
* `post_type`
* `content_length`
* `created_at`
* `region`

## engagement

* `engagement_id`
* `post_id`
* `engagement_type`
* `engagement_value`
* `user_id`
* `timestamp`

## users

* `user_id`
* `username`
* `country`
* `account_type`
* `followers_count`
* `join_date`

At this phase, your job is **not** to fully clean and analyze all of it yet.
Your job is to **understand what you received**.

---

# Before You Start

## Environment Notes

### If you are using Windows

You should use **WSL**.

That means:

* open your project inside WSL
* run Python and Spark commands from WSL
* use Linux-style paths such as:

  * `/home/your-user/project-folder/...`

### If you are using Linux

You can run everything normally in your terminal.

### Database note

In this phase, you do **not** need to use PostgreSQL yet. PostgreSQL will become more relevant later when we prepare business-ready outputs.

---

# Folder Reminder

At minimum, your project should include:

```text
social-media-engagement-analytics/
├── data/
│   └── raw/
├── src/
├── docs/
└── README_02_Phase1_Spark_Foundations_and_Raw_Data.md
```

Place your raw files inside:

```text
data/raw/
```

For example:

```text
data/raw/posts.csv
data/raw/engagement.csv
data/raw/users.csv
```

---

# Step 1 — Create the Phase 1 Script

## Task

Create a Python file named:

```python
src/phase1_raw_data_inspection.py
```

## Why this task matters

In real projects, engineers usually separate work by purpose.
A dedicated script for Phase 1 makes the project cleaner and easier to maintain.

It also helps other team members understand where raw data inspection happens.

---

# Step 2 — Start a Spark Session

## Theory

A **Spark session** is the main entry point for working with Spark.

Think of it as the starting object that allows you to:

* read data
* create DataFrames
* run Spark SQL
* work with the Spark engine

Without a Spark session, you cannot begin processing data with PySpark.

---

## Technical implementation

Add this code to `src/phase1_raw_data_inspection.py`:

```python
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SocialMediaPhase1RawDataInspection")
    .master("local[*]")
    .getOrCreate()
)
```

---

## Explanation of the code

### `SparkSession.builder`

This begins the process of creating a Spark session.

### `.appName("SocialMediaPhase1RawDataInspection")`

This gives the Spark application a readable name.
In real projects, naming your Spark job clearly helps with monitoring and debugging.

### `.master("local[*]")`

This tells Spark to run in **local mode** and use all available CPU cores on your machine.

This is useful for learning because you do not need a real Spark cluster yet, but you still use the same Spark programming model.

### `.getOrCreate()`

This creates the session if it does not already exist.

---

## Checkpoint

Run the script and make sure no error appears when Spark starts.

---

# Step 3 — Define the Raw Data Paths

## Theory

In real pipelines, hardcoding file paths in many places is messy and risky.

It is better to define paths once and reuse them.

This makes your script easier to update and easier to understand.

---

## Technical implementation

Add the following below your Spark session:

```python
posts_path = "data/raw/posts.csv"
engagement_path = "data/raw/engagement.csv"
users_path = "data/raw/users.csv"
```

---

## Explanation

These variables store the locations of the raw files.
Later, if the file location changes, you only update the path once instead of searching through the entire script.

---

# Step 4 — Read the Raw CSV Files into Spark DataFrames

## Theory

A **DataFrame** in Spark is a distributed table-like structure.

It has:

* rows
* columns
* data types

If you already know pandas, a Spark DataFrame may look similar at first, but Spark DataFrames are designed for scalable processing.

At this stage, we are reading raw files into Spark so that we can inspect them.

---

## Technical implementation

Add this code:

```python
posts_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(posts_path)
)

engagement_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(engagement_path)
)

users_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(users_path)
)
```

---

## Explanation of the code

### `.option("header", True)`

This tells Spark that the first row of the file contains column names.

Without this, Spark may treat the first row as normal data.

### `.option("inferSchema", True)`

This tells Spark to examine the data and guess the data types of each column.

For example, Spark may infer:

* integer
* double
* string
* timestamp

This is helpful in learning projects because it reduces manual setup.

However, in real production pipelines, engineers often define schema explicitly because automatic guessing can be wrong.

That is an important lesson:
**schema inference is useful, but it is not always safe enough for production systems.**

### `.csv(...)`

This reads the CSV file into a Spark DataFrame.

---

# Step 5 — Display Sample Rows

## Theory

Before you trust a dataset, you should look at actual records.

This helps you:

* confirm the file loaded correctly
* understand the type of values inside
* spot unexpected patterns early

In real work, this is often one of the first things engineers do after loading data.

---

## Technical implementation

Add:

```python
print("Posts Data")
posts_df.show(5, truncate=False)

print("Engagement Data")
engagement_df.show(5, truncate=False)

print("Users Data")
users_df.show(5, truncate=False)
```

---

## Explanation

### `.show(5, truncate=False)`

This displays the first 5 rows.

* `5` means show 5 records
* `truncate=False` means do not cut off long text values

This is useful because social media data may contain longer fields that you want to inspect fully.

---

## Business meaning

By looking at sample rows, the engineering team starts understanding:

* what a “post” record looks like
* what an “engagement” record looks like
* what user profile data looks like

This helps later when designing transformations and analytics.

---

# Step 6 — Inspect the Schema

## Theory

A **schema** describes:

* column names
* data types
* structure of the DataFrame

Example data types include:

* string
* integer
* double
* boolean
* timestamp

Schema matters because data engineering depends on correct types.

For example:

* if `followers_count` is stored as text instead of integer, calculations may fail
* if `created_at` is not handled properly, date-based reporting becomes harder
* if numeric values are read as strings, grouping and filtering may become incorrect

Schema is not just a technical detail.
It directly affects correctness and usability.

---

## Technical implementation

Add:

```python
print("Posts Schema")
posts_df.printSchema()

print("Engagement Schema")
engagement_df.printSchema()

print("Users Schema")
users_df.printSchema()
```

---

## Explanation

### `.printSchema()`

This prints the structure of the DataFrame in a tree format.

It helps you quickly understand:

* what columns exist
* what Spark thinks their types are

---

## Example reflection

When you inspect the schema, ask yourself:

* Did Spark infer all columns correctly?
* Are any numeric fields incorrectly stored as strings?
* Are timestamp fields still plain text?
* Which columns will matter most later?

---

# Step 7 — List the Column Names

## Theory

Before building analytics, you need to know which columns are available.

This is especially important in real work when:

* the dataset is new
* column names are unfamiliar
* multiple files must later be joined together

---

## Technical implementation

Add:

```python
print("Posts Columns:", posts_df.columns)
print("Engagement Columns:", engagement_df.columns)
print("Users Columns:", users_df.columns)
```

---

## Explanation

### `.columns`

This returns a list of column names.

This is useful for quick inspection and planning.

---

## Business meaning

At this step, the data engineering team begins identifying which fields might later help answer business questions such as:

* engagement by category
* top creators
* regional activity
* post performance

---

# Step 8 — Count the Records

## Theory

Counting rows is one of the simplest ways to understand the size of a dataset.

This is also a good moment to talk about an important Spark idea:

## Transformations vs Actions

In Spark, not every line of code immediately runs.

### Transformations

These describe work to be done later.

Examples:

* `select()`
* `filter()`
* `withColumn()`

### Actions

These actually trigger execution.

Examples:

* `show()`
* `count()`
* `collect()`
* `write()`

This is connected to **lazy evaluation**.

## Lazy evaluation

Spark often waits before doing the actual work.
Instead of executing every transformation immediately, it builds a plan.
Then, when an action happens, Spark executes the plan.

This allows Spark to optimize execution.

---

## Technical implementation

Add:

```python
print("Posts Count:", posts_df.count())
print("Engagement Count:", engagement_df.count())
print("Users Count:", users_df.count())
```

---

## Explanation

### `.count()`

This is an **action**.
It triggers Spark to actually process the DataFrame and count the rows.

This is a very important Spark concept.

---

## Why this matters in business

Knowing the size of the data helps teams understand:

* how much raw data they are receiving
* whether a file looks suspiciously small or too large
* how large future processing might become

---

# Step 9 — Perform a Few Simple Transformations

At this stage, we still do not want to deeply process the data yet.
But we do want to practice a few simple transformations so that you can connect Day 1 theory to real code.

---

## Task 1 — Select a Few Important Columns

### Theory

Real datasets often contain more columns than you need immediately.

Selecting only relevant columns can make the dataset easier to inspect and easier to reason about.

### Technical implementation

Add:

```python
posts_selected_df = posts_df.select(
    "post_id",
    "user_id",
    "category",
    "post_type",
    "created_at",
    "region"
)

posts_selected_df.show(5, truncate=False)
```

---

## Explanation

### `select(...)`

This is a **transformation**.
It creates a new DataFrame with only the chosen columns.

Spark still works lazily here.
The actual execution happens when `show()` is called.

---

## Task 2 — Filter a Small Subset

### Theory

Filtering helps narrow data to only the records you need.

In real work, engineers often filter data to:

* inspect specific categories
* isolate suspicious records
* focus on one business area

### Technical implementation

Add:

```python
technology_posts_df = posts_df.filter(posts_df.category == "Technology")
technology_posts_df.show(5, truncate=False)
```

---

## Explanation

### `filter(...)`

This is another transformation.

Here we are creating a new DataFrame containing only posts in the Technology category.

### Business meaning

This simulates a real business question such as:

* “Can we inspect only Technology content first?”
* “How much engagement do we get in one category?”

---

## Task 3 — Add a Simple Derived Column

### Theory

Data engineers often create new columns to make later analysis easier.

These are called **derived columns** because they are built from existing values.

### Technical implementation

Add:

```python
from pyspark.sql.functions import length

posts_with_title_length_df = posts_df.withColumn(
    "category_name_length",
    length(posts_df.category)
)

posts_with_title_length_df.show(5, truncate=False)
```

---

## Explanation

### `withColumn(...)`

This creates or replaces a column.

Here we are creating a new column called `category_name_length`.

This example is simple, but it shows the basic idea of deriving new information from raw data.

---

# Step 10 — Stop the Spark Session

## Theory

When your script finishes, it is good practice to stop Spark cleanly.

This helps release resources and keeps the workflow tidy.

---

## Technical implementation

Add this at the end of your script:

```python
spark.stop()
```

---

# Full Example Script for Phase 1

Below is the full guided script for this phase.

```python
from pyspark.sql import SparkSession
from pyspark.sql.functions import length

spark = (
    SparkSession.builder
    .appName("SocialMediaPhase1RawDataInspection")
    .master("local[*]")
    .getOrCreate()
)

posts_path = "data/raw/posts.csv"
engagement_path = "data/raw/engagement.csv"
users_path = "data/raw/users.csv"

posts_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(posts_path)
)

engagement_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(engagement_path)
)

users_df = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .csv(users_path)
)

print("Posts Data")
posts_df.show(5, truncate=False)

print("Engagement Data")
engagement_df.show(5, truncate=False)

print("Users Data")
users_df.show(5, truncate=False)

print("Posts Schema")
posts_df.printSchema()

print("Engagement Schema")
engagement_df.printSchema()

print("Users Schema")
users_df.printSchema()

print("Posts Columns:", posts_df.columns)
print("Engagement Columns:", engagement_df.columns)
print("Users Columns:", users_df.columns)

print("Posts Count:", posts_df.count())
print("Engagement Count:", engagement_df.count())
print("Users Count:", users_df.count())

posts_selected_df = posts_df.select(
    "post_id",
    "user_id",
    "category",
    "post_type",
    "created_at",
    "region"
)

posts_selected_df.show(5, truncate=False)

technology_posts_df = posts_df.filter(posts_df.category == "Technology")
technology_posts_df.show(5, truncate=False)

posts_with_title_length_df = posts_df.withColumn(
    "category_name_length",
    length(posts_df.category)
)

posts_with_title_length_df.show(5, truncate=False)

spark.stop()
```

---

# Phase 1 Checkpoint Tasks

Complete the following tasks after running the script.

## Task 1

Run the script successfully and confirm that all three raw files are loaded into Spark.

## Task 2

Record the row counts for:

* posts
* engagement
* users

## Task 3

Write down the schema observations:

* Which columns are strings?
* Which columns are numeric?
* Are any columns inferred differently than you expected?

## Task 4

Write down at least three important business columns from each dataset.

## Task 5

Explain in your own words:

* what a Spark DataFrame is
* what a schema is
* what the difference is between a transformation and an action

---

# What You Should Understand Before Moving to Phase 2

Before continuing, you should be comfortable with the following:

* Spark session creation
* reading CSV files into DataFrames
* using `show()`
* using `printSchema()`
* understanding column names
* understanding row counts
* identifying simple transformations
* understanding actions
* understanding why raw data inspection matters in data engineering

If any of these still feel unclear, review this phase again before moving on.

---

# Important Note About Parquet

You have **not** used Parquet yet in this phase, and that is intentional.

## What is Parquet?

Parquet is a **columnar file format** often used in analytics and big data systems.

That means it stores data in a way that is efficient for:

* reading selected columns
* analytics workloads
* compression
* performance optimization

### Why students need to know this later

Later in the project, when we begin thinking about better output formats, Parquet will become important.

### Why we are not using it yet

In Phase 1, the focus is on:

* raw data understanding
* Spark basics
* schema inspection

So we keep things simple first.

When Parquet appears later, it will be explained fully again in its own phase before you are asked to use it.

---

# End of Phase 1

At this point, you have completed the raw data inspection phase.

You now understand:

* how to begin a Spark project
* how to load raw data
* how to inspect schema and structure
* how Spark begins working with DataFrames
* how to connect basic Spark theory to practical code

This is the correct starting point before moving to real transformations and analytics.

---

# Next Step

Continue to:

**`README_03_Phase2_Data_Processing_and_Analytics.md`**

In the next phase, you will begin the main data engineering work: cleaning records, transforming columns, filtering data, grouping results, and building engagement analytics for the business.
