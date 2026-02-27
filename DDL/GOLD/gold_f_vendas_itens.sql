CREATE OR REPLACE TABLE F_VENDAS_ITENS_BRIDGE AS
SELECT 
    d.deal_id as ID_NEGOCIO,
    flat.value::STRING as ID_PRODUTO 
FROM elt_b2b_snowflake.silver.crm_deals d,
LATERAL FLATTEN(input => d.produtos_interesse) flat;