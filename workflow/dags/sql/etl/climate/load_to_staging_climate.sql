-- load data from metadata files to PostgreSQL
COPY
  staging_climate
FROM
  's3://{{ var.value.get("s3_bucket") }}/uploaded/climate/'
ACCESS_KEY_ID
  '{{ var.value.get("access_key_id") }}'
SECRET_ACCESS_KEY
  '{{ var.value.get("secret_access_key") }}'
CSV
IGNOREHEADER 1
REGION
  '{{ var.value.get("aws_region") }}'
;
