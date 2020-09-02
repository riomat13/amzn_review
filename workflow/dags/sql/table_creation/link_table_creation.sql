CREATE TABLE IF NOT EXISTS time
(
    datetime_ts  TIMESTAMP   PRIMARY KEY
  , year         INT         NOT NULL
  , month        INT         NOT NULL
  , day          INT         NOT NULL
  , day_of_week  INT         NOT NULL
  , week         INT         NOT NULL
  , day_of_year  INT         NOT NULL
)
DISTKEY
  (year)
SORTKEY
  (datetime_ts)
;
