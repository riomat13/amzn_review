-- load data from staging table to dimension table (time)
INSERT INTO climate
(
  SELECT
      TO_TIMESTAMP(date_str, 'YYYY-MM-DD') AS datetime_ts
    , station_id
    , awnd
    , prcp
    , snow
    , snwd
    , tavg
    , tmax
    , tmin
    , wt01
    , wt02
    , wt03
    , wt04
    , wt05
    , wt06
    , wt07
    , wt08
    , wt09
    , wt10
    , wt11
    , wt12
    , wt13
    , wt14
    , wt15
    , wt16
    , wt17
    , wt18
    , wt19
    , wt21
    , wt22
  FROM
      staging_climate
)
;
