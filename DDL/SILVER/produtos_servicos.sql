CREATE OR REPLACE TABLE produtos_servicos(
    produto_id STRING,
    nome STRING,
    categoria STRING,
    preco_base NUMBER(10,2),
    margem_minima NUMBER(5,2),
    desconto_max NUMBER(5,2),
    filename STRING,
    ingest_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);
USE SCHEMA elt_b2b_snowflake.silver;
TRUNCATE TABLE produtos_servicos;

INSERT INTO produtos_servicos (produto_id, nome, categoria, preco_base, margem_minima, desconto_max, filename)
SELECT 
    produto_id,
    nome,
    categoria,
    TRY_CAST(REPLACE(REGEXP_REPLACE(preco_base, '[^0-9,]', ''), ',', '.') AS NUMBER(10, 2)),
    TRY_CAST(REPLACE(margem_minima, '%', '') AS NUMBER(5, 2)),
    TRY_CAST(REPLACE(desconto_max, '%', '') AS NUMBER(5, 2)),
    filename
FROM elt_b2b_snowflake.BRONZE.produtos_servicos

QUALIFY ROW_NUMBER() OVER(
    PARTITION BY produto_id 
    ORDER BY load_timestamp DESC, categoria DESC, nome DESC
) = 1;
