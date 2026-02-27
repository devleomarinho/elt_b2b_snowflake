CREATE OR REPLACE TABLE metas_vendedores (
    vendedor STRING,
    mes STRING,
    meta_receita STRING,
    meta_deals_fechados STRING,
    comissao_percentual STRING,
    filename STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO metas_vendedores(vendedor, mes, meta_receita, meta_deals_fechados, comissao_percentual, filename)
FROM(
    SELECT
        $1,
        $2,
        $3,
        $4,
        $5,
        metadata$filename
    FROM @gcp_stg_erp/csvs/metas_vendedores_v2.csv
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

