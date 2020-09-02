/* Dimension tables
 *    - station
 */

/* all weather obsevating stations
 *
 *  country: country code with 2 alphabetic characters based on ISO-3166-2
 *
 *  Set initial country as `united states`
 *  because the target data is all from united states.
 */
CREATE TABLE IF NOT EXISTS stations
(
    station_id    CHAR(11)   PRIMARY KEY
  , station_name  TEXT
  , longitude     NUMERIC
  , latitude      NUMERIC
  , city          TEXT
  , state         TEXT
  , country       CHAR(2)    DEFAULT 'US'
)
;

--CREATE TABLE IF NOT EXISTS weather_type
--(
--    type_id        SMALLINT   NOT NULL PRIMARY KEY
--  , description    TEXT
--)
--DITSTYLE
--  ALL
--;

/* Fact tablw
 *  - climate
 */
CREATE TABLE IF NOT EXISTS climate
(
    datetime_ts    TIMESTAMP  NOT NULL REFERENCES time(datetime_ts)
  , station_id     CHAR(11)   NOT NULL REFERENCES stations
  , ave_wind       NUMERIC
  , percepitation  NUMERIC
  , snow_falll     NUMERIC
  , snow_depth     NUMERIC
  , ave_temp       NUMERIC
  , min_temp       NUMERIC
  , max_temp       NUMERIC
  , wt01           BOOL
  , wt02           BOOL
  , wt03           BOOL
  , wt04           BOOL
  , wt05           BOOL
  , wt06           BOOL
  , wt07           BOOL
  , wt08           BOOL
  , wt09           BOOL
  , wt10           BOOL
  , wt11           BOOL
  , wt12           BOOL
  , wt13           BOOL
  , wt14           BOOL
  , wt15           BOOL
  , wt16           BOOL
  , wt17           BOOL
  , wt18           BOOL
  , wt19           BOOL
  , wt21           BOOL
  , wt22           BOOL
)
DISTKEY
  (station_id)
;
