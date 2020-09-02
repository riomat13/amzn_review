-- load data from staging tables for facto table
-- currently this causes duplicates and uses DISTNCT for it
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
    , CAST(REPLACE(s.vote, ',', '') AS INT)
    , s.review_text
    , s.summary
    , TIMESTAMP 'epoch' + s.review_time * INTERVAL '1 second'
  FROM
      staging_reviews s
  LEFT JOIN
      products p
  ON
      s.asin = p.asin
)
;
