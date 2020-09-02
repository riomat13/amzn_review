/* Fact table */
-- review text can be empty when only ratings are provided
CREATE TABLE IF NOT EXISTS reviews
(
    review_id    SERIAL      PRIMARY KEY
  , reviewer_id  VARCHAR(20) NOT NULL REFERENCES users(user_id)
  , asin         CHAR(10)    NOT NULL REFERENCES products
  , rating       INT         NOT NULL
  , vote         INT         DEFAULT 0
  , review_text  TEXT
  , summary      TEXT
  , review_time  TIMESTAMP            REFERENCES time
);
