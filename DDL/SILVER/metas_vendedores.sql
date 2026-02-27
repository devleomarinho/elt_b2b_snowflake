CREATE OR REPLACE TABLE metas_vendedores (
    vendedor STRING,
    data_meta DATE,
    meta_receita NUMBER(12, 2),
    meta_deals_fechados INT,
    comissao_percentual NUMBER(5, 2),
    filename STRING,
    ingestion_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO metas_vendedores(vendedor, data_meta, meta_receita, meta_deals_fechados, comissao_percentual, filename)
SELECT 
    vendedor,
    TRY_TO_DATE(mes, 'YYYY-MM-DD') as data_meta,
    TRY_CAST(meta_receita AS NUMBER(12, 2)),
    TRY_CAST(meta_deals_fechados AS INT),
    ROUND(TRY_CAST(comissao_percentual AS NUMBER(10, 6)), 2), 
    filename
FROM elt_b2b_snowflake.bronze.metas_vendedores;
