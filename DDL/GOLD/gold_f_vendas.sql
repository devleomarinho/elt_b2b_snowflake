CREATE OR REPLACE TABLE F_VENDAS (
    ID_NEGOCIO STRING,
    TITULO STRING,
    ID_CLIENTE INT,
    ID_VENDEDOR STRING,
    DATA_FECHAMENTO DATE,
    VALOR_TOTAL NUMBER(12, 2),
    PROBABILIDADE INT,
    VALOR_PONDERADO NUMBER(12, 2),
    ESTAGIO STRING,
    STATUS STRING,
    ORIGEM_LEAD STRING,
    HASH_DIFF STRING
);

MERGE INTO F_VENDAS g
USING (
    WITH silver_data AS (
        SELECT 
            d.deal_id as ID_NEGOCIO,
            d.titulo,
            org.ID_CLIENTE,
            -- Ligamos com o novo ID do Vendedor baseado em Hash
            MD5(d.owner_name) as ID_VENDEDOR, 
            d.data_fechamento_estimada as DATA_FECHAMENTO,
            d.valor as VALOR_TOTAL,
            d.probabilidade,
            d.valor * (d.probabilidade/100) as VALOR_PONDERADO,
            d.estagio,
            d.status,
            d.origem_lead
        FROM elt_b2b_snowflake.silver.crm_deals d
        LEFT JOIN  elt_b2b_snowflake.gold.dim_clientes org 
        ON d.org_name = org.NOME_EMPRESA
    )
    SELECT *, MD5(OBJECT_CONSTRUCT(*)::VARCHAR) AS hash_diff FROM silver_data
) s
ON g.ID_NEGOCIO = s.ID_NEGOCIO
WHEN MATCHED AND g.HASH_DIFF <> s.hash_diff THEN
    UPDATE SET 
        g.TITULO = s.titulo,
        g.ID_CLIENTE = s.ID_CLIENTE,
        g.ID_VENDEDOR = s.ID_VENDEDOR,
        g.DATA_FECHAMENTO = s.DATA_FECHAMENTO,
        g.VALOR_TOTAL = s.VALOR_TOTAL,
        g.PROBABILIDADE = s.probabilidade,
        g.VALOR_PONDERADO = s.VALOR_PONDERADO,
        g.ESTAGIO = s.estagio,
        g.STATUS = s.status,
        g.ORIGEM_LEAD = s.origem_lead,
        g.HASH_DIFF = s.hash_diff
WHEN NOT MATCHED THEN
    INSERT (ID_NEGOCIO, TITULO, ID_CLIENTE, ID_VENDEDOR, DATA_FECHAMENTO, VALOR_TOTAL, PROBABILIDADE, VALOR_PONDERADO, ESTAGIO, STATUS, ORIGEM_LEAD, HASH_DIFF)
    VALUES (s.ID_NEGOCIO, s.titulo, s.ID_CLIENTE, s.ID_VENDEDOR, s.DATA_FECHAMENTO, s.VALOR_TOTAL, s.probabilidade, s.VALOR_PONDERADO, s.estagio, s.status, s.origem_lead, s.hash_diff);