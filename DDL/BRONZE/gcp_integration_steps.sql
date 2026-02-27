CREATE OR REPLACE STORAGE INTEGRATION gcs_integration_elt
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'GCS'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('gcs://snowflake-etl-bucket');

DESC STORAGE INTEGRATION gcs_integration_elt;


CREATE OR REPLACE STAGE gcp_stg_erp
  URL = 'gcs://snowflake-etl-bucket'
  STORAGE_INTEGRATION = gcs_integration_elt
  FILE_FORMAT = csv_format;

