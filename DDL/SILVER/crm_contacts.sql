CREATE OR REPLACE TABLE crm_contacts (
    contact_id INT,
    nome STRING,
    email STRING,
    telefone STRING,
    cargo STRING,
    org_id INT,      -- Chave estrangeira para ligar com Organizations
    owner_name STRING,
    filename STRING,
    ingestion_at TIMESTAMP_NTZ 
);

INSERT INTO crm_contacts
SELECT 
    value:id::INT,
    value:name::STRING,
    value:email::STRING,
    value:phone::STRING,
    value:job_title::STRING,
    value:org_id::INT,
    value:owner_name::STRING,
    filename,
    CURRENT_TIMESTAMP() as ingestion_at
FROM ELT_B2B_SNOWFLAKE.BRONZE.crm_raw,
LATERAL FLATTEN(input => dados_brutos:data)
WHERE filename LIKE '%contacts%';