CREATE OR REPLACE TABLE territorios_segmentos (
    territorio STRING,
    estado STRING,
    segmento_foco STRING,
    vendedor_responsavel STRING,
    potencial_anual STRING,
    filename STRING,
    load_timestamp TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO territorios_segmentos (territorio, estado, segmento_foco, vendedor_responsavel, potencial_anual, filename)
FROM (
    SELECT $1, $2, $3, $4, $5, metadata$filename
    FROM @gcp_stg_erp/csvs/territorios_segmentos.csv
)
FILE_FORMAT = (FORMAT_NAME = 'csv_format');
