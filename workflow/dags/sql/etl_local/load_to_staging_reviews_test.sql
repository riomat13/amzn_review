-- Load data from files to PostgreSQL server
INSERT INTO staging_reviews
(
    reviewer_id
  , asin
  , reviewer
  , vote
  , rating
  , review_time
  , review_text
  , summary
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
;
