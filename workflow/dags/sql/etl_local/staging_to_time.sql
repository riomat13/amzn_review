-- load data from staging table to dimension table (time)
INSERT INTO time
(
  SELECT
      review_time
    , EXTRACT(YEAR FROM review_time)
    , EXTRACT(MONTH FROM review_time)
    , EXTRACT(DAY FROM review_time)
    , EXTRACT(DOW FROM review_time)
    , EXTRACT(WEEK FROM review_time)
  FROM
      staging_reviews
)
ON CONFLICT
    (review_time)
DO NOTHING
;
