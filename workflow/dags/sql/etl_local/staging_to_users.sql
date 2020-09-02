-- load data from staging table to dimension table (users)
INSERT INTO users (user_id, name)
(
  SELECT
      reviewer_id
    , reviewer
  FROM
      staging_reviews
)
ON CONFLICT
    (user_id)
DO NOTHING
;
