CREATE OR REPLACE TABLE DIM_VENDEDORES (
    ID_VENDEDOR STRING, 
    NOME_VENDEDOR STRING,
    HASH_DIFF STRING
);

MERGE INTO DIM_VENDEDORES g
USING (
    WITH silver_data AS (
        SELECT DISTINCT 
            vendedor as NOME_VENDEDOR,
            -- Chave primária baseada no MD5 do nome para ser imutável
            MD5(vendedor) as ID_VENDEDOR 
        FROM ELT_B2B_SNOWFLAKE.SILVER.metas_vendedores
    )
    SELECT *, MD5(OBJECT_CONSTRUCT(*)::VARCHAR) AS hash_diff FROM silver_data
) s
ON g.ID_VENDEDOR = s.ID_VENDEDOR
WHEN MATCHED AND g.HASH_DIFF <> s.hash_diff THEN
    UPDATE SET 
        g.NOME_VENDEDOR = s.NOME_VENDEDOR,
        g.HASH_DIFF = s.hash_diff
WHEN NOT MATCHED THEN
    INSERT (ID_VENDEDOR, NOME_VENDEDOR, HASH_DIFF)
    VALUES (s.ID_VENDEDOR, s.NOME_VENDEDOR, s.hash_diff);