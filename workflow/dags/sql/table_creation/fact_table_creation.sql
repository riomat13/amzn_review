/* Fact table */
-- review data
CREATE TABLE IF NOT EXISTS reviews
(
    review_id    BIGINT         IDENTITY(1, 1) PRIMARY KEY
  , reviewer_id  VARCHAR(20)    NOT NULL REFERENCES users(user_id)
  , asin         CHAR(10)       NOT NULL REFERENCES products
  , rating       INT            NOT NULL
  , review_text  VARCHAR(65535)
  , summary      VARCHAR(1024)
  , vote         INT
  , review_time  TIMESTAMP               REFERENCES time
)
DISTKEY
  (asin)
SORTKEY
  (review_time)
;
