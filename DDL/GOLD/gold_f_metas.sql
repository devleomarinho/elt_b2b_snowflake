CREATE OR REPLACE TABLE F_METAS AS
SELECT 
    v.ID_VENDEDOR,
    m.data_meta, 
    m.meta_receita,
    m.meta_deals_fechados
FROM elt_b2b_snowflake.silver.metas_vendedores m
LEFT JOIN elt_b2b_snowflake.GOLD.DIM_VENDEDORES v ON m.vendedor = v.NOME_VENDEDOR;