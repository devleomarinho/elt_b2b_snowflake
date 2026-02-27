CREATE OR REPLACE TABLE crm_organizations (
    org_id INT,
    nome_empresa STRING,
    setor STRING,
    cidade STRING,
    estado STRING,
    funcionarios STRING, -- Faixa de tamanho (ex: "50-100")
    receita_anual NUMBER(15, 2),
    owner_name STRING,
    filename STRING,
    ingestion_at TIMESTAMP_NTZ 
);

INSERT INTO crm_organizations
SELECT 
    value:id::INT,
    value:name::STRING,
    value:industry::STRING,
    value:city::STRING,
    value:state::STRING,
    value:employees::STRING,
    value:annual_revenue::NUMBER(15, 2),
    value:owner_name::STRING,
    filename,
    CURRENT_TIMESTAMP() as ingestion_at
FROM ELT_B2B_SNOWFLAKE.BRONZE.crm_raw,
LATERAL FLATTEN(input => dados_brutos:data)
WHERE filename LIKE '%organizations%';