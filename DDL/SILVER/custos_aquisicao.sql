CREATE OR REPLACE TABLE custos_aquisicao (
    data_referencia DATE,          
    origem_lead STRING,
    custo_total NUMBER(10, 2),
    qtd_leads INT,                 
    custo_por_lead NUMBER(10, 2),  
    observacoes STRING,
    filename STRING,
    ingestion_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO custos_aquisicao(data_referencia, origem_lead, custo_total, qtd_leads, custo_por_lead, observacoes, filename)
SELECT
    COALESCE(
        TRY_TO_DATE(data_referencia, 'YYYY-MM-DD'),  
        TRY_TO_DATE(data_referencia, 'DD/MM/YYYY'),  
        TRY_TO_DATE(data_referencia, 'YYYY/MM/DD'), 
        TRY_TO_DATE(data_referencia, 'DD-MM-YYYY')  
    ) as data_referencia,
      
    origem_lead,
    TRY_CAST(custo AS NUMBER(10,2)),
    TRY_CAST(qtd_leads AS INT),
    CASE 
        WHEN TRY_CAST(qtd_leads AS INT) > 0 
        THEN TRY_CAST(custo AS NUMBER(10, 2)) / TRY_CAST(qtd_leads AS INT)
        ELSE 0 
    END as custo_por_lead,

    observacoes,
    filename
FROM elt_b2b_snowflake.bronze.custos_aquisicao
WHERE data_referencia IS NOT NULL;
