-- load data from staging table to link dimension table (time) from climate
INSERT INTO time
(
  SELECT
      datetime_ts
    , EXTRACT(year FROM datetime_ts)
    , EXTRACT(month FROM datetime_ts)
    , EXTRACT(day FROM datetime_ts)
    , EXTRACT(dayofweek FROM datetime_ts)
    , EXTRACT(week FROM datetime_ts)
    , EXTRACT(dayofyear FROM datetime_ts)
  FROM
  (
    SELECT
      TO_TIMESTAMP(date_str, 'YYYY-MM-DD') AS datetime_ts
    FROM
      staging_climate
  )
)
;
