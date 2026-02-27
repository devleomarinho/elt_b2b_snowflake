SELECT * FROM ELT_B2B_SNOWFLAKE.SILVER.CRM_ORGANIZATIONS;
CREATE OR REPLACE TABLE DIM_CLIENTES (
    ID_CLIENTE INT,
    NOME_EMPRESA STRING,
    SETOR STRING,
    CIDADE STRING,
    ESTADO STRING,
    SEGMENTO_TAMANHO STRING,
    RECEITA_ANUAL NUMBER(15, 2), 
    HASH_DIFF STRING
);

MERGE INTO DIM_CLIENTES g
USING (
    WITH silver_data AS (
        SELECT 
            org_id as ID_CLIENTE,
            nome_empresa,
            setor,
            cidade,
            estado,
            CASE 
                WHEN receita_anual > 10000000 THEN 'Enterprise'
                WHEN receita_anual > 1000000 THEN 'Mid-Market'
                ELSE 'SMB' 
            END as SEGMENTO_TAMANHO,
            receita_anual
        FROM elt_b2b_snowflake.silver.crm_organizations
    )
    SELECT *, MD5(OBJECT_CONSTRUCT(*)::VARCHAR) AS hash_diff FROM silver_data
) s
ON g.ID_CLIENTE = s.ID_CLIENTE
WHEN MATCHED AND g.HASH_DIFF <> s.hash_diff THEN
    UPDATE SET 
        g.NOME_EMPRESA = s.nome_empresa,
        g.SETOR = s.setor,
        g.CIDADE = s.cidade,
        g.ESTADO = s.estado,
        g.SEGMENTO_TAMANHO = s.SEGMENTO_TAMANHO,
        g.RECEITA_ANUAL = s.RECEITA_ANUAL,
        g.HASH_DIFF = s.hash_diff
WHEN NOT MATCHED THEN
    INSERT (ID_CLIENTE, NOME_EMPRESA, SETOR, CIDADE, ESTADO, SEGMENTO_TAMANHO, RECEITA_ANUAL, HASH_DIFF)
    VALUES (s.ID_CLIENTE, s.nome_empresa, s.setor, s.cidade, s.estado, s.SEGMENTO_TAMANHO, s.RECEITA_ANUAL, s.hash_diff);