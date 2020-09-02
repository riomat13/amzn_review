-- load data from staging table to dimension table (time)
INSERT INTO time
(
  SELECT
      review_time
    , EXTRACT(year FROM review_time)
    , EXTRACT(month FROM review_time)
    , EXTRACT(day FROM review_time)
    , EXTRACT(dayofweek FROM review_time)
    , EXTRACT(week FROM review_time)
    , EXTRACT(dayofyear FROM review_time)
  FROM
  (
    SELECT
      TIMESTAMP 'epoch' + review_time * INTERVAL '1 second' AS review_time
    FROM
      staging_reviews
  )
)
;
