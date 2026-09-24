#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
==================================================================================================
PROJETO: BOLSA FAMÍLIA ANALYTICS — ATUALIZADOR MENSAL GOOGLE BIGQUERY
==================================================================================================
Destino: Google Cloud BigQuery
Projeto: bolsa-familia-brasil-507002
Dataset: bolsa_familia
Tabelas:
  1. fato_bolsa_familia_com_genero (Agregada por Município + Gênero + Mês)
  2. fato_bolsa_familia_consolidada (Agregada por Município SIAFI + Mês)
Autor: Karl / Antigravity Assistant
Data: Setembro/2026
==================================================================================================
"""

import os
import sys
import glob
import time
import re
import duckdb
from google.cloud import bigquery
from google.oauth2 import service_account

if hasattr(sys.stdout, 'reconfigure'): sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'): sys.stderr.reconfigure(encoding='utf-8', errors='replace')

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
BASE_DIR = os.path.dirname(SCRIPT_DIR)
DETALHADO_DIR = os.path.join(BASE_DIR, "0_Bases_de_Dados", "detalhado")
KEY_PATH = os.path.join(BASE_DIR, "04_Chave_do_GCP_Google_Cloud_Platafform", "bolsa-familia-brasil-507002-eb76546af1b5.json")
TEMP_DIR = os.path.join(SCRIPT_DIR, "temp_bigquery_upload")

PROJECT_ID = "bolsa-familia-brasil-507002"
DATASET_ID = f"{PROJECT_ID}.bolsa_familia"

def extract_yyyymm(filename):
    m = re.search(r'(\d{6})', filename)
    return int(m.group(1)) if m else None

def get_bigquery_client():
    if not os.path.exists(KEY_PATH):
        raise FileNotFoundError(f"Chave de serviço JSON não encontrada em: {KEY_PATH}")
    creds = service_account.Credentials.from_service_account_file(KEY_PATH)
    return bigquery.Client(project=PROJECT_ID, credentials=creds)

def obter_max_mes(client, table_name):
    table_id = f"{DATASET_ID}.{table_name}"
    try:
        query = f"SELECT MAX(mes_competencia) AS max_mes FROM `{table_id}`"
        query_job = client.query(query, location="US")
        results = [row.max_mes for row in query_job]
        return results[0] if results and results[0] is not None else 0
    except Exception as e:
        print(f"[AVISO] Tabela {table_name} consulta: {e}")
        return 0

def processar_e_subir_mes(client, ym, fpath, temp_dir=TEMP_DIR):
    os.makedirs(temp_dir, exist_ok=True)
    con = duckdb.connect()
    sql_in = fpath.replace("\\", "/")

    print(f"\n--- Ingestão BigQuery para o Mês {ym} ---")

    # 1. Tabela fato_bolsa_familia_com_genero
    print(f"[*] Gerando slice com Gênero via DuckDB...")
    gen_file = os.path.join(temp_dir, f"{ym}_gen.parquet")
    sql_gen_out = gen_file.replace("\\", "/")

    t0 = time.time()
    con.execute(f"""
        COPY (
            WITH raw_data AS (
                SELECT 
                    mes_competencia,
                    uf,
                    nome_municipio,
                    cod_municipio_siafi,
                    SPLIT_PART(nome_favorecido, ' ', 1) AS primeiro_nome,
                    nis_favorecido,
                    valor_parcela
                FROM read_parquet('{sql_in}')
            )
            SELECT 
                r.mes_competencia::INTEGER AS mes_competencia,
                r.uf::VARCHAR AS uf,
                r.nome_municipio::VARCHAR AS nome_municipio,
                IF(UPPER(TRIM(r.primeiro_nome)) LIKE '%A', 'Feminino', 'Masculino')::VARCHAR AS Genero,
                COUNT(r.nis_favorecido)::INTEGER AS total_beneficiarios,
                ROUND(SUM(r.valor_parcela), 2)::DECIMAL(38,9) AS valor_total_pago
            FROM raw_data r
            GROUP BY r.mes_competencia, r.uf, r.nome_municipio, 4
        ) TO '{sql_gen_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    # Idempotência no BigQuery
    tbl_gen_id = f"{DATASET_ID}.fato_bolsa_familia_com_genero"
    print(f"[*] Limpando mês {ym} em {tbl_gen_id}...")
    client.query(f"DELETE FROM `{tbl_gen_id}` WHERE mes_competencia = {ym}", location="US").result()

    print(f"[*] Carregando {gen_file} no BigQuery...")
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition="WRITE_APPEND"
    )
    with open(gen_file, "rb") as sf:
        client.load_table_from_file(sf, tbl_gen_id, job_config=job_config).result()
    print(f"[OK] fato_bolsa_familia_com_genero atualizada em {time.time()-t0:.2f}s!")

    # 2. Tabela fato_bolsa_familia_consolidada
    print(f"[*] Gerando slice consolidado SIAFI via DuckDB...")
    cons_file = os.path.join(temp_dir, f"{ym}_cons.parquet")
    sql_cons_out = cons_file.replace("\\", "/")

    t1 = time.time()
    con.execute(f"""
        COPY (
            SELECT 
                mes_competencia::INTEGER AS mes_competencia,
                uf::VARCHAR AS uf,
                cod_municipio_siafi::VARCHAR AS cod_municipio_siafi,
                nome_municipio::VARCHAR AS nome_municipio,
                COUNT(nis_favorecido)::INTEGER AS total_beneficiarios,
                ROUND(SUM(valor_parcela), 2)::DECIMAL(38,9) AS valor_total_beneficio,
                ROUND(AVG(valor_parcela), 2)::DECIMAL(38,9) AS valor_medio_parcela
            FROM read_parquet('{sql_in}')
            GROUP BY 1, 2, 3, 4
        ) TO '{sql_cons_out}' (FORMAT PARQUET, COMPRESSION ZSTD);
    """)

    tbl_cons_id = f"{DATASET_ID}.fato_bolsa_familia_consolidada"
    print(f"[*] Limpando mês {ym} em {tbl_cons_id}...")
    client.query(f"DELETE FROM `{tbl_cons_id}` WHERE mes_competencia = {ym}", location="US").result()

    with open(cons_file, "rb") as sf_cons:
        client.load_table_from_file(sf_cons, tbl_cons_id, job_config=job_config).result()
    print(f"[OK] fato_bolsa_familia_consolidada atualizada em {time.time()-t1:.2f}s!")

    con.close()

    # Limpar temp files
    for f in [gen_file, cons_file]:
        if os.path.exists(f):
            try: os.remove(f)
            except: pass

def main():
    print("=" * 80)
    print(" >>> PIPELINE DE ATUALIZAÇÃO MENSAL INCREMENTAL -> GOOGLE BIGQUERY <<<")
    print("=" * 80)

    client = get_bigquery_client()
    max_mes_bq = obter_max_mes(client, "fato_bolsa_familia_com_genero")
    print(f"[OK] Último mês gravado no BigQuery (com gênero): {max_mes_bq}")

    arquivos = sorted(glob.glob(os.path.join(DETALHADO_DIR, "*_NovoBolsaFamilia_fato.parquet")))
    if not arquivos:
        print(f"[ERRO] Nenhum arquivo em {DETALHADO_DIR}")
        return

    pendentes = []
    for f in arquivos:
        ym = extract_yyyymm(f)
        if ym and ym > max_mes_bq:
            pendentes.append((ym, f))

    if not pendentes:
        print(f"\n[TUDO ATUALIZADO] BigQuery já possui todos os meses carregados até {max_mes_bq}!")
        resposta = input("Deseja forçar a recarga do último mês disponível? (s/N): ").strip().lower()
        if resposta == 's':
            ultimo_f = arquivos[-1]
            pendentes.append((extract_yyyymm(ultimo_f), ultimo_f))
        else:
            return

    for ym, fpath in pendentes:
        processar_e_subir_mes(client, ym, fpath)

    print("\n" + "=" * 80)
    print("[SUCESSO] Ingestão mensal no Google BigQuery concluída com sucesso!")
    print("=" * 80)

if __name__ == "__main__":
    main()
