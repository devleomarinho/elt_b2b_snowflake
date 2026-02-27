*Leia em outros idiomas: [English](README.md), [Português](README.pt-br.md).*

---
# Pipeline de ELT para dados de ERP e CRM com Snowflake e GCP

![GCP](https://img.shields.io/badge/Google_Cloud-4285F4?style=for-the-badge&logo=google-cloud&logoColor=white)
![Snowflake](https://img.shields.io/badge/Snowflake-29B5E8?style=for-the-badge&logo=snowflake&logoColor=white)

Este repositório contém um projeto end-to-end de Engenharia de Dados focado em Inteligência Comercial e Revenue Operations (RevOps) B2B. O pipeline implementa uma arquitetura de Data Lakehouse utilizando o Snowflake como plataforma central, projetada para processar dados estruturados e semi-estruturados de forma escalável e com otimização de custos de computação.

## Visão Geral do Projeto

Empresas B2B frequentemente enfrentam o desafio de cruzar dados transacionais tradicionais (metas, custos, catálogos de produtos) provenientes de ERPs com dados altamente aninhados gerados por CRMs modernos. Tradicionalmente, processar arquivos JSON complexos exige infraestruturas separadas (como clusters Spark ou scripts Python pesados), o que aumenta a latência dos dados e a complexidade da manutenção. Além disso, estratégias de ingestão que reprocessam todo o volume de dados a cada ciclo geram custos computacionais excessivos e lentidão na entrega de relatórios.

### A Solução e Benefícios
Este projeto resolve esses problemas construindo um Data Warehouse 100% cloud e gerenciado. Os principais benefícios alcançados são:

**Redução de Complexidade**: Extração e "achatamento" (flattening) de dados JSON diretamente na camada do banco de dados utilizando SQL puro.

**Otimização de Custos**: O pipeline utiliza o free tier da GCP para o serviço de storage, com custo zero de armazenamento. Além disso, no Snowflake foi implementada uma arquitetura de carga incremental idempotente. O banco de dados identifica e atualiza apenas os registros que sofreram alterações reais, economizando créditos de processamento.

**Confiabilidade**: Regras rigorosas de tipagem e deduplicação aplicadas nativamente, garantindo que os painéis de Business Intelligence consumam informações íntegras e validadas.

**Facilidade de Manutenção**: O pipeline foi construído no Snowflake com a linguagem SQL, que é universalmente utilizada pelos profissionais de dados, o que facilita a manutenção e escalabilidade do projeto.

### Destaques Técnicos da Engenharia

Para atingir os objetivos propostos, o projeto faz uso intensivo de recursos avançados de SQL e do motor computacional do Snowflake:

**Change Data Capture (CDC) com Hash Diff (MD5)**: A atualização da camada analítica (Gold) foi construída para ser totalmente incremental e idempotente. Em vez de comparar colunas individualmente ou fazer cargas completas, o projeto utiliza a instrução MERGE combinada com a função MD5(OBJECT_CONSTRUCT(*)::VARCHAR). Essa abordagem empacota toda a linha de origem em um objeto JSON e gera um hash único. Se o hash da origem for diferente do hash no destino, o registro é atualizado; caso contrário, é ignorado. Isso representa o padrão-ouro em modelagem de dimensões de alteração lenta (SCD).

**Processamento Nativo de JSON (Semi-estruturados)**:
Os dados brutos do CRM entram no banco de dados em colunas do tipo VARIANT. Na camada de transformação, a função LATERAL FLATTEN é utilizada para iterar sobre arrays e explodir objetos JSON em linhas e colunas relacionais. Isso permitiu extrair campos customizados e listas de produtos que estavam profundamente aninhados na estrutura do arquivo, eliminando ferramentas externas de pré-processamento.

**Garantia de Qualidade e Deduplicação**:
Para evitar a propagação de duplicatas e inconsistências, o pipeline utiliza a instrução QUALIFY ROW_NUMBER() OVER(...) = 1 diretamente nos scripts de transformação, garantindo que apenas a versão mais recente de um evento avance para as camadas finais, mesmo em casos de falhas sistêmicas na origem.

## Arquitetura da Solução

<img width="6235" height="1815" alt="diagram" src="https://github.com/user-attachments/assets/35982cbc-9623-4065-856a-a37c551603a0" />


O projeto segue a Arquitetura Medalhão, adaptada para o ambiente Snowflake:

**Ingestão**: Arquivos transacionais em CSV (ERP) e respostas de API em JSON (CRM) são armazenados no Google Cloud Storage (GCS).

**Integração Segura**: O Snowflake estabelece uma conexão trust-based via IAM Roles com o GCP, permitindo a leitura dos arquivos sem a exposição de chaves de acesso estáticas.

**Camada Bronze**: Tabelas brutas alimentadas via COPY INTO. Os dados permanecem em seu formato original (STRING ou VARIANT), preservando o histórico de extração e metadados estruturais.

**Camada Silver**: Conversão de tipos via TRY_CAST, limpeza de strings com REGEXP_REPLACE, tratamento de listas, flattening de JSON e deduplicação determinística.

**Camada Gold**: Modelagem dimensional baseada no modelo Star Schema. Tabelas Fato e Dimensões atualizadas de forma incremental, prontas para ferramentas de visualização.

### Tecnologias Utilizadas

- Cloud Storage: Google Cloud Storage (Data Lake)

- Data Platform / Data Warehouse: Snowflake

- Linguagem de Transformação: SQL (Dialeto Snowflake)

- Modelagem: Esquema estrela, arquitetura medalhão

## Modelagem de Dados

O Data Mart final entrega as seguintes tabelas principais para consumo:

| Tabela | Tipo | Descrição |
| :--- | :--- | :--- |
| F_VENDAS | Fato | Transações de oportunidades do CRM (Pipeline). |
| F_METAS | Fato | Metas mensais de vendas por vendedor. |
| F_MARKETING | Fato | Custos de aquisição de leads por canal. |
| F_VENDAS_ITENS | Bridge | Tabela de ligação para análise de múltiplos produtos por venda. |
| D_CLIENTES | Dimensão | Dados demográficos das empresas (Setor, Tamanho, Local). |
| D_PRODUTOS | Dimensão | Catálogo de produtos e serviços com precificação. |
| D_VENDEDORES | Dimensão | Equipe de vendas (SCD Tipo 1). |

## Contact
Projeto desenvolvido por Leonardo Marinho. 
[LinkedIn](https://www.linkedin.com/in/devleomarinho/) | [Email](mailto:dev.leomarinho@gmail.com)


