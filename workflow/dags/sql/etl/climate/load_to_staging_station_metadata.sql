-- load data from climate observating station metadata files to PostgreSQL
COPY
  staging_stations
FROM
  's3://{{ var.value.get("s3_bucket") }}/uploaded/metadata/climate/ghcnd-stations.txt'
ACCESS_KEY_ID
  '{{ var.value.get("access_key_id") }}'
SECRET_ACCESS_KEY
  '{{ var.value.get("secret_access_key") }}'
DELIMITER
  '\t'
REGION
  '{{ var.value.get("aws_region") }}'
;
