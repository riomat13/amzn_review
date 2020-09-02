-- load data from staging table to dimension table (users)
BEGIN;

-- remove duplicated rows to update
DELETE FROM
  users
USING
  staging_reviews
WHERE
  users.user_id = staging_reviews.reviewer_id
;

INSERT INTO users (user_id, name)
(
  SELECT
      reviewer_id
    , reviewer
  FROM
      staging_reviews
)
;
