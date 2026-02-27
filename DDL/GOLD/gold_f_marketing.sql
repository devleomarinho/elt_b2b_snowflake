CREATE OR REPLACE TABLE F_MARKETING AS
SELECT 
    data_referencia as DATA,
    origem_lead as CANAL,
    custo_total,
    qtd_leads,
    custo_por_lead
FROM elt_b2b_snowflake.SILVER.custos_aquisicao;