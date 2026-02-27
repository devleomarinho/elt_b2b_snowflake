CREATE OR REPLACE TABLE custos_aquisicao (
    data_referencia STRING,
    origem_lead STRING,
    custo STRING,
    qtd_leads STRING,
    observacoes STRING,
    filename STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO custos_aquisicao (data_referencia, origem_lead, custo, qtd_leads, observacoes, filename) 
FROM(
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        metadata$filename
    FROM @gcp_stg_erp/csvs/custos_aquisicao_v2.csv
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format' skip_header = 1);

