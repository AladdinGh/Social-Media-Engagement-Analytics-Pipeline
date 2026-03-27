

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