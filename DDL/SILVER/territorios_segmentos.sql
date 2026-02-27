CREATE OR REPLACE TABLE territorios_segmentos (
    territorio STRING,
    estados_lista ARRAY,          
    segmento_foco STRING,
    vendedor_responsavel STRING,
    potencial_anual NUMBER(12, 2),
    filename STRING,
    ingestion_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

INSERT INTO territorios_segmentos(territorio, estados_lista, segmento_foco, vendedor_responsavel, potencial_anual, filename)
SELECT 
    territorio,
    SPLIT(
        REPLACE(REPLACE(estado, ';', ','), ', ', ','), 
        ','
    ) as estados_lista,    
    segmento_foco,
    vendedor_responsavel,
    TRY_CAST(potencial_anual AS NUMBER(12, 2)),
    filename
FROM ELT_B2B_SNOWFLAKE.BRONZE.territorios_segmentos;

SELECT *
FROM territorios_segmentos;