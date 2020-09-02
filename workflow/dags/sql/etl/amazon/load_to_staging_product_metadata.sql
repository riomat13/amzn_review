-- load data from Amazon product metadata files to PostgreSQL
COPY
  staging_product_metadata
FROM
  's3://{{ var.value.get("s3_bucket") }}/uploaded/metadata/products/'
ACCESS_KEY_ID
  '{{ var.value.get("access_key_id") }}'
SECRET_ACCESS_KEY
  '{{ var.value.get("secret_access_key") }}'
JSON
  's3://{{ var.value.get("s3_bucket") }}/metadata/{{ var.value.get("metadata_jsonpath") }}'
GZIP
ACCEPTINVCHARS
REGION
  '{{ var.value.get("aws_region") }}'
;
