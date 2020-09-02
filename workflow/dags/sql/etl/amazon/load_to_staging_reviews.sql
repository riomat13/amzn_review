-- Load data from files to PostgreSQL server
COPY
  staging_reviews
FROM
  's3://{{ var.value.get("s3_bucket") }}/uploaded/reviews/'
ACCESS_KEY_ID
  '{{ var.value.get("access_key_id") }}'
SECRET_ACCESS_KEY
  '{{ var.value.get("secret_access_key") }}'
JSON
  's3://{{ var.value.get("s3_bucket") }}/metadata/{{ var.value.get("review_jsonpath") }}'
GZIP
ACCEPTINVCHARS
REGION
  '{{ var.value.get("aws_region") }}'
;
