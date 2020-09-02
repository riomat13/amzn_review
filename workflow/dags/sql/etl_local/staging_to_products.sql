-- load data from staging data to dimension table (products)
INSERT INTO products
(
    asin
  , product_name
  , features
  , description
  , price
  , brand
  , category
)
(
  SELECT
      asin
    , title
    , feature
    , description
    , price
    , brand
    , categories::jsonb -> 0
  FROM
    staging_metadata
)
ON CONFLICT
    (asin)
DO UPDATE SET
    product_name=EXCLUDED.product_name
  , features=EXCLUDED.features
  , description=EXCLUDED.description
  , price=EXCLUDED.price
  , brand=EXCLUDED.brand
  , category=EXCLUDED.category
;
