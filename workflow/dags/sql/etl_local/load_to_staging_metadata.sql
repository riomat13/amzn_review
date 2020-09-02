-- load data from metadata files to PostgreSQL
INSERT INTO staging_metadata
(
    asin
  , title
  , feature
  , description
  , price
  , related
  , also_bought
  , also_viewed
  , bought_together
  , buy_after_viewing
  , sales_rank
  , brand
  , categories
)
VALUES %s
ON CONFLICT
    (asin)
DO NOTHING
;
