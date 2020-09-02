-- staging table for review data
-- this will store data for each review
CREATE TABLE IF NOT EXISTS staging_reviews
(
    reviewer_id  VARCHAR(20) NOT NULL
  , asin         VARCHAR(10) NOT NULL
  , reviewer     TEXT
  , vote         INT         DEFAULT 0
  , rating       INT         NOT NULL
  , review_time  TIMESTAMP   NOT NULL
  , review_text  TEXT
  , summary      TEXT
);

-- staging table for product metadata
-- this will store all products and no update information
-- unloass new products are added
CREATE TABLE IF NOT EXISTS staging_metadata
(
    asin              CHAR(10) PRIMARY KEY
  , title             TEXT
  , feature           TEXT
  , description       TEXT
  , price             NUMERIC
  , related           TEXT
  , also_bought       TEXT
  , also_viewed       TEXT
  , bought_together   TEXT
  , buy_after_viewing TEXT
  , sales_rank        TEXT
  , brand             TEXT
  , categories        TEXT
);
