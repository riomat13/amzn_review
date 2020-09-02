/* Dimension tables
 *    - users
 *    - products
 */

CREATE TABLE IF NOT EXISTS users
(
    user_id      VARCHAR(20) PRIMARY KEY
  , name         TEXT
  , city         TEXT
  , state        TEXT
  , country      CHAR(2)
)
DISTKEY
  (country)
;

-- product is the largest dimension, so that disribute this table
CREATE TABLE IF NOT EXISTS products
(
    asin         CHAR(10)       PRIMARY KEY DISTKEY
  , product_name TEXT           NOT NULL
  , features     VARCHAR(1024)
  , description  VARCHAR(1024)
  , price        NUMERIC        NOT NULL
  , brand        TEXT           NOT NULL
  , category     TEXT           NOT NULL
)
;

/* Fact table */
CREATE TABLE IF NOT EXISTS reviews
(
    review_id    BIGINT      IDENTITY(1, 1) PRIMARY KEY
  , reviewer_id  VARCHAR(20) NOT NULL REFERENCES users(user_id)
  , asin         CHAR(10)    NOT NULL REFERENCES products
  , rating       INT         NOT NULL
  , review_text  VARCHAR(65535)
  , summary      VARCHAR(1024)
  , vote         INT
  , review_time  TIMESTAMP            REFERENCES time(datetime_ts)
)
DISTKEY
  (asin)
SORTKEY
  (review_time)
;

