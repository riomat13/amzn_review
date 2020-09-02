-- load data from staging table to dimension table (time)
INSERT INTO stations
(
  SELECT
      station_id
    , name
    , longitude
    , latitude
    , elevation
  FROM
      staging_stations
)
;
