-- load data from staging tables for facto table
-- currently this causes duplicates and uses DISTNCT for it
-- TODO: reconstruct without DISTINCT
INSERT INTO reviews
(
    reviewer_id
  , asin
  , rating
  , vote
  , review_text
  , summary
  , review_time
)
(
  SELECT
      s.reviewer_id
    , s.asin
    , s.rating
    , s.vote
    , s.review_text
    , s.summary
    , s.review_time
  FROM
      staging_reviews s
  LEFT JOIN
      products p
  ON
      s.asin = p.asin
)
;
