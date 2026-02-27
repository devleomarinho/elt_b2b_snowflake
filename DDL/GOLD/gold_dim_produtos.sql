TRUNCATE TABLE elt_b2b_snowflake.gold.dim_produtos;
CREATE OR REPLACE TABLE DIM_PRODUTOS(
    produto_id STRING,
    nome STRING,
    categoria STRING,
    preco_base NUMBER(10,2),
    margem_minima NUMBER(5,2),
    desconto_max NUMBER(5,2),
    hash_diff STRING
);

MERGE INTO DIM_PRODUTOS d
USING(
    WITH silver_produtos AS(
        SELECT
        produto_id,
        nome,
        categoria,
        preco_base,
        margem_minima,
        desconto_max
    FROM ELT_B2B_SNOWFLAKE.SILVER.produtos_servicos  
    )
    SELECT 
        *,
        MD5(OBJECT_CONSTRUCT(*)::VARCHAR) AS hash_diff
        FROM silver_produtos
) s
ON d.produto_id = s.produto_id
WHEN MATCHED AND d.hash_diff <> s.hash_diff THEN
    UPDATE SET
        d.nome = s.nome,
        d.categoria = s.categoria,
        d.preco_base = s.preco_base,
        d.margem_minima = s.margem_minima,
        d.desconto_max = s.desconto_max,
        d.hash_diff = s.hash_diff

WHEN NOT MATCHED THEN
    INSERT(produto_id, nome, categoria, preco_base, margem_minima, desconto_max, hash_diff)
    VALUES(s.produto_id, s.nome, s.categoria, s.preco_base, s.margem_minima, s.desconto_max, s.hash_diff);

SELECT *
FROM dim_produtos;
