
-- ============================================
-- Creator Engagement Summary Table (PostgreSQL)
-- ============================================

-- Drop table if it already exists (optional)
DROP TABLE IF EXISTS creator_engagement_summary;

-- Create table
CREATE TABLE creator_engagement_summary (
    creator_user_id BIGINT PRIMARY KEY,
    username TEXT NOT NULL,
    creator_total_engagement INTEGER
);

-- Optional: index for faster queries
CREATE INDEX idx_creator_engagement
ON creator_engagement_summary (creator_total_engagement DESC);

-- ============================================
-- Example Queries (for reporting)
-- ============================================

-- Top 10 creators by engagement
SELECT *
FROM creator_engagement_summary
ORDER BY creator_total_engagement DESC
LIMIT 10;

-- Average engagement
SELECT AVG(creator_total_engagement) AS avg_engagement
FROM creator_engagement_summary;

-- Count creators
SELECT COUNT(*) AS total_creators
FROM creator_engagement_summary;