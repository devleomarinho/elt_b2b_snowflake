CREATE OR REPLACE TABLE crm_activities (
    activity_id STRING,
    tipo_atividade STRING, 
    assunto STRING,
    foi_realizado BOOLEAN, 
    data_prazo DATE,
    deal_id STRING,        
    owner_name STRING,
    observacao STRING,
    filename STRING,
    ingestion_at TIMESTAMP_NTZ 
);

INSERT INTO crm_activities
SELECT 
    value:id::STRING,
    value:type::STRING,
    value:subject::STRING,
    value:done::BOOLEAN,
    TRY_TO_DATE(value:due_date::STRING),
    value:deal_id::STRING,
    value:owner_name::STRING,
    value:note::STRING,
    filename,
    CURRENT_TIMESTAMP() as ingestion_at
FROM elt_b2b_snowflake.BRONZE.crm_raw,
LATERAL FLATTEN(input => dados_brutos:data)
WHERE filename LIKE '%activities%';