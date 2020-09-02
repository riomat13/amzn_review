-- staging table for review data
-- this will store data for each review
CREATE TABLE IF NOT EXISTS staging_reviews
(
    reviewer_id  VARCHAR(20) NOT NULL
  , asin         CHAR(10)    NOT NULL
  , reviewer     TEXT
  , vote         VARCHAR(20) DEFAULT '0'
  , rating       SMALLINT    NOT NULL
  , review_time  BIGINT
  , review_text  VARCHAR(65535)
  , summary      VARCHAR(1024)
)
;

-- staging table for product metadata
-- this will store all products and no update information
-- unloass new products are added
CREATE TABLE IF NOT EXISTS staging_product_metadata
(
    asin              CHAR(10)    PRIMARY KEY
  , title             TEXT        NOT NULL
  , features          VARCHAR(1024)
  , description       VARCHAR(1024)
  , price             NUMERIC
  , related           TEXT
  , sales_rank        TEXT
  , also_bought       VARCHAR(1024)
  , also_viewed       VARCHAR(1024)
  , bought_together   VARCHAR(1024)
  , buy_after_viewing VARCHAR(1024)
  , brand             TEXT
  , categories        TEXT
)
;

-- staging table for climate
CREATE TABLE IF NOT EXISTS staging_climate
(
    station_id        CHAR(11) NOT NULL
  , name              TEXT     NOT NULL
  , date_str          TEXT     NOT NULL
  , awnd              NUMERIC
  , prcp              NUMERIC
  , snow              NUMERIC
  , snwd              NUMERIC
  , tavg              NUMERIC
  , tmax              NUMERIC
  , tmin              NUMERIC
  , wt01              BOOL     DEFAULT false
  , wt02              BOOL     DEFAULT false
  , wt03              BOOL     DEFAULT false
  , wt04              BOOL     DEFAULT false
  , wt05              BOOL     DEFAULT false
  , wt06              BOOL     DEFAULT false
  , wt07              BOOL     DEFAULT false
  , wt08              BOOL     DEFAULT false
  , wt09              BOOL     DEFAULT false
  , wt10              BOOL     DEFAULT false
  , wt11              BOOL     DEFAULT false
  , wt12              BOOL     DEFAULT false
  , wt13              BOOL     DEFAULT false
  , wt14              BOOL     DEFAULT false
  , wt15              BOOL     DEFAULT false
  , wt16              BOOL     DEFAULT false
  , wt17              BOOL     DEFAULT false
  , wt18              BOOL     DEFAULT false
  , wt19              BOOL     DEFAULT false
  , wt21              BOOL     DEFAULT false
  , wt22              BOOL     DEFAULT false
)
;

-- staging table for climate observing station
CREATE TABLE IF NOT EXISTS staging_stations
(
    station_id        TEXT
  , longitude         NUMERIC
  , latitude          NUMERIC
  , elevation         NUMERIC
  , name              TEXT
  , code1             VARCHAR(10)
  , code2             CHAR(5)
)
;
