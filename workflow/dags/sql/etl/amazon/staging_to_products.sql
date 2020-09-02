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
    , features
    , description
    , price
    , brand
    , COALESCE(NULLIF(JSON_EXTRACT_ARRAY_ELEMENT_TEXT(categories, 0, true), ''), NULLIF(categories, ''), 'none')
  FROM
    staging_product_metadata
)
;
