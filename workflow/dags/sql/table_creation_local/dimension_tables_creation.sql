/* Dimension tables
 *    - users
 *    - products
 *    - time
 */

CREATE TABLE IF NOT EXISTS users
(
    user_id      VARCHAR(20) PRIMARY KEY
  , name         TEXT
);

CREATE TABLE IF NOT EXISTS products
(
    asin         CHAR(10)    PRIMARY KEY
  , product_name TEXT        NOT NULL
  , features     TEXT
  , description  TEXT
  , price        NUMERIC     NOT NULL
  , brand        TEXT        NOT NULL
  , category     TEXT        NOT NULL
);

CREATE TABLE IF NOT EXISTS time
(
    review_time  TIMESTAMP   PRIMARY KEY
  , year         INT         NOT NULL
  , month        INT         NOT NULL
  , day          INT         NOT NULL
  , day_of_week  INT         NOT NULL
  , week         INT         NOT NULL
);
