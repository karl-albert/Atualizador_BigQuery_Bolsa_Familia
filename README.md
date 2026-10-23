# Atualizador Mensal Bolsa Família — Google BigQuery

Repositório oficial para automação do pipeline mensal do **Bolsa Família Analytics** no **Google Cloud BigQuery**.

## Arquitetura da Solução
* **Projeto GCP**: `bolsa-familia-brasil-507002`
* **Dataset**: `bolsa_familia` (Região: US)
* **Tabelas Analíticas**:
  1. `fato_bolsa_familia_com_genero` (Otimizada para análise demográfica) 

  2. `fato_bolsa_familia_consolidada` (Otimizada para granularidade SIAFI e ticket médio)
* **Motor Local**: DuckDB In-Memory (redução de 93% no tráfego de dados e carga ZSTD instantânea).

## Como Executar
Basta dar duplo clique em `Subir_Mes_BigQuery.bat` ou executar via terminal:
```bash
python atualizador_mensal_bigquery.py
```
