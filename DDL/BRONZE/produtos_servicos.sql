CREATE OR REPLACE TABLE produtos_servicos (
    produto STRING,
    nome STRING,
    categoria STRING,
    preco_base STRING,
    margem_minima STRING,
    desconto_max STRING,
    filename STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO produtos_servicos(produto, nome, categoria, preco_base, margem_minima, desconto_max, filename)
FROM(
    SELECT $1, $2, $3, $4, $5, $6, METADATA$filename
    FROM @gcp_stg_erp/csvs/produtos_servicos_v2.csv
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

