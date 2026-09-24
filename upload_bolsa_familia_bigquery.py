#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
========================================================================================
PIPELINE DE CARGA BOLSA FAMÍLIA -> GOOGLE BIGQUERY (FATO CONSOLIDADA & DIMENSÕES)
========================================================================================
Projeto GCP: bolsa-familia-brasil-507002
Dataset: bolsa_familia
Localização: US
========================================================================================
Carrega de forma incremental e leve:
  1. fato_bolsa_familia_consolidada (~12 MB total, respostas instantâneas)
  2. dim_regional (27 UFs e agrupamentos regionais)
  3. Populacao_Municipios_UFs (5.570 municípios com população IBGE)
  4. dim_municipios (Cadastro SIAFI)
========================================================================================
"""

import os
import sys
import glob
import time
import shutil
import re
import duckdb
import pandas as pd
from google.cloud import bigquery
from google.oauth2 import service_account

# Forçar UTF-8 no Windows
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')
if hasattr(sys.stderr, 'reconfigure'):
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')

# Configurações de Caminho
BASE_DIR_C = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Bolsa_Familia\0_Bases_de_Dados"
BASE_DIR_D = r"D:\1_Bases_Parquet"
ROOT_PROJ = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Bolsa_Familia"
KEY_PATH = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Bolsa_Familia\04_Chave_do_GCP_Google_Cloud_Platafform\bolsa-familia-brasil-507002-eb76546af1b5.json"

PROJECT_ID = "bolsa-familia-brasil-507002"
DATASET_ID = f"{PROJECT_ID}.bolsa_familia"

def format_bytes(size):
    for unit in ['B', 'KB', 'MB', 'GB', 'TB']:
        if size < 1024.0:
            return f"{size:.2f} {unit}"
        size /= 1024.0
    return f"{size:.2f} PB"

def extract_yyyymm(filename):
    m = re.search(r'(\d{6})', filename)
    return int(m.group(1)) if m else None

def obter_meses_carregados(client, table_id):
    """Consulta os meses de competência já gravados no BigQuery para não duplicar."""
    try:
        query = f"SELECT DISTINCT mes_competencia FROM `{table_id}`"
        query_job = client.query(query, location="US")
        results = [row.mes_competencia for row in query_job]
        return set(results)
    except Exception as e:
        print(f" [INFO] Tabela ainda não existe ou vazia ({e}).")
        return set()

def sincronizar_consolidados():
    """Sincroniza os parquets consolidados de D: para C: e retorna a lista ordenada."""
    c_cons = os.path.join(BASE_DIR_C, "consolidado")
    d_cons = os.path.join(BASE_DIR_D, "consolidado")
    os.makedirs(c_cons, exist_ok=True)

    if os.path.exists(d_cons):
        for f in glob.glob(os.path.join(d_cons, "*.parquet")):
            fname = os.path.basename(f)
            dest_path = os.path.join(c_cons, fname)
            if not os.path.exists(dest_path) or os.path.getsize(dest_path) != os.path.getsize(f):
                print(f" [SYNC] Sincronizando {fname} -> {c_cons}...")
                shutil.copy2(f, dest_path)

    return sorted(glob.glob(os.path.join(c_cons, "*.parquet")))

def sincronizar_dimensoes():
    """Sincroniza dimensões de D: para C:."""
    c_dim = os.path.join(BASE_DIR_C, "dimensoes")
    d_dim = os.path.join(BASE_DIR_D, "dimensoes")
    os.makedirs(c_dim, exist_ok=True)

    if os.path.exists(d_dim):
        for f in glob.glob(os.path.join(d_dim, "*.parquet")):
            fname = os.path.basename(f)
            dest_path = os.path.join(c_dim, fname)
            if not os.path.exists(dest_path) or os.path.getsize(dest_path) != os.path.getsize(f):
                print(f" [SYNC] Sincronizando dimensão {fname}...")
                shutil.copy2(f, dest_path)
    return c_dim

def sincronizar_detalhados():
    """Sincroniza parquets detalhados entre D: e C: para manter o histórico local."""
    c_det = os.path.join(BASE_DIR_C, "detalhado")
    d_det = os.path.join(BASE_DIR_D, "detalhado")
    os.makedirs(c_det, exist_ok=True)

    if os.path.exists(d_det):
        for f in glob.glob(os.path.join(d_det, "*.parquet")):
            fname = os.path.basename(f)
            ym = extract_yyyymm(fname)
            dest_name = f"{ym}_NovoBolsaFamilia.parquet" if ym else fname
            dest_path = os.path.join(c_det, dest_name)
            if not os.path.exists(dest_path) or os.path.getsize(dest_path) != os.path.getsize(f):
                print(f" [SYNC LOCAL] Sincronizando {fname} -> {dest_name}...")
                shutil.copy2(f, dest_path)

def main():
    print("\n" + "=" * 80)
    print(" >>> PIPELINE DE CARGA CONSOLIDADA BOLSA FAMÍLIA -> BIGQUERY 🚀")
    print(f" [PROJETO]: {PROJECT_ID}")
    print(f" [DATASET]: bolsa_familia")
    print("=" * 80)

    # 1. Validar Credenciais
    if not os.path.exists(KEY_PATH):
        print(f"\n❌ [ERRO] Chave de serviço JSON não encontrada em:\n   {KEY_PATH}")
        sys.exit(1)

    creds = service_account.Credentials.from_service_account_file(KEY_PATH)
    client = bigquery.Client(project=PROJECT_ID, credentials=creds)

    dataset = bigquery.Dataset(DATASET_ID)
    dataset.location = "US"
    dataset = client.create_dataset(dataset, exists_ok=True)
    print(" [OK] Dataset 'bolsa_familia' conectado com sucesso!\n")

    # Sincronização prévia dos arquivos locais
    print(" [*] Sincronizando arquivos locais entre Disco D: e C:...")
    sincronizar_detalhados()
    dim_dir = sincronizar_dimensoes()
    cons_files = sincronizar_consolidados()

    # ==========================================================================
    # 1. TABELA: dim_regional
    # ==========================================================================
    table_regional_id = f"{DATASET_ID}.dim_regional"
    try:
        client.get_table(table_regional_id)
        print(" [OK] Tabela dim_regional já existe!")
    except Exception:
        print(" [1/3] Criando Tabela: dim_regional...")
        df_regional = pd.DataFrame([
            ('SP', 'SAO PAULO', 'Sudeste'), ('RS', 'SUL', 'Sul'), ('AC', 'NORTE', 'Norte'), ('DF', 'CENTRO-OESTE', 'Centro-Oeste'),
            ('GO', 'CENTRO-OESTE', 'Centro-Oeste'), ('MS', 'CENTRO-OESTE', 'Centro-Oeste'), ('MT', 'CENTRO-OESTE', 'Centro-Oeste'), 
            ('RO', 'NORTE', 'Norte'), ('TO', 'NORTE', 'Norte'), ('MG', 'MG', 'Sudeste'), ('AL', 'NORDESTE', 'Nordeste'), 
            ('CE', 'NORDESTE', 'Nordeste'), ('PB', 'NORDESTE', 'Nordeste'), ('PE', 'NORDESTE', 'Nordeste'), ('PI', 'NORDESTE', 'Nordeste'), 
            ('RN', 'NORDESTE', 'Nordeste'), ('PR', 'SUL', 'Sul'), ('SC', 'SUL', 'Sul'), ('BA', 'BA+SE', 'Nordeste'), 
            ('SE', 'BA+SE', 'Nordeste'), ('ES', 'RJ+ES', 'Sudeste'), ('RJ', 'RJ+ES', 'Sudeste'), ('AM', 'NORTE', 'Norte'), 
            ('AP', 'NORTE', 'Norte'), ('MA', 'NORTE', 'Nordeste'), ('PA', 'NORTE', 'Norte'), ('RR', 'NORTE', 'Norte')
        ], columns=['uf', 'regional', 'regiao_macro'])
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
        job = client.load_table_from_dataframe(df_regional, table_regional_id, job_config=job_config)
        job.result()
        print(" [OK] Tabela dim_regional criada com 27 UFs e Regiões!")

    # ==========================================================================
    # 2. TABELA: Populacao_Municipios_UFs (IBGE)
    # ==========================================================================
    table_pop_id = f"{DATASET_ID}.Populacao_Municipios_UFs"
    try:
        tbl_pop = client.get_table(table_pop_id)
        print(f" [OK] Tabela Populacao_Municipios_UFs já existe ({tbl_pop.num_rows:,} municípios IBGE)!")
    except Exception:
        pop_excel = os.path.join(ROOT_PROJ, "Populacao_Municipios_UFs.xlsx")
        if os.path.exists(pop_excel):
            print(" [2/3] Carregando Tabela de População IBGE...")
            df_pop = pd.read_excel(pop_excel)
            job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
            job = client.load_table_from_dataframe(df_pop, table_pop_id, job_config=job_config)
            job.result()
            print(f" [OK] Tabela Populacao_Municipios_UFs carregada com {len(df_pop):,} registros!")

    # ==========================================================================
    # 3. TABELA: dim_municipios
    # ==========================================================================
    table_mun_id = f"{DATASET_ID}.dim_municipios"
    dim_mun_file = os.path.join(dim_dir, "dim_municipios.parquet")
    if os.path.exists(dim_mun_file):
        try:
            client.get_table(table_mun_id)
            print(" [OK] Tabela dim_municipios já existe!")
        except Exception:
            print(" [3/3] Carregando Tabela: dim_municipios...")
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_TRUNCATE"
            )
            with open(dim_mun_file, "rb") as sf:
                job = client.load_table_from_file(sf, table_mun_id, job_config=job_config)
                job.result()
            print(" [OK] Tabela dim_municipios criada com sucesso!")

    # ==========================================================================
    # 4. TABELA FATO: fato_bolsa_familia_consolidada (Carga Incremental via Parquet)
    # ==========================================================================
    print("\n" + "-" * 80)
    print(" [*] Processando Fato Consolidada Analítica...")
    table_fato_id = f"{DATASET_ID}.fato_bolsa_familia_consolidada"
    
    meses_carregados = obter_meses_carregados(client, table_fato_id)
    if meses_carregados:
        print(f" [INFO] Meses já gravados no BigQuery ({len(meses_carregados)} meses): {sorted(list(meses_carregados))}")

    arquivos_pendentes = []
    for fpath in cons_files:
        fname = os.path.basename(fpath)
        ym = extract_yyyymm(fname)
        if ym and ym in meses_carregados:
            continue
        arquivos_pendentes.append((ym, fpath))

    if not arquivos_pendentes:
        print("\n✨ [TUDO ATUALIZADO] Todos os arquivos consolidados já estão gravados no BigQuery!")
    else:
        print(f"\n📦 Arquivos consolidados pendentes para subir: {len(arquivos_pendentes)}")
        con = duckdb.connect()
        temp_dir = os.path.join(ROOT_PROJ, "0_Bases_de_Dados", "temp")
        os.makedirs(temp_dir, exist_ok=True)

        for idx, (ym, fpath) in enumerate(arquivos_pendentes, start=1):
            fname = os.path.basename(fpath)
            fsize = os.path.getsize(fpath)
            t_start = time.time()
            print(f" [{idx:02d}/{len(arquivos_pendentes):02d}] Subindo {fname} ({format_bytes(fsize)})...", end="", flush=True)

            # Padronizar schema exatamente para DECIMAL(38,9) / NUMERIC do BigQuery
            clean_temp = os.path.join(temp_dir, f"{ym}_clean.parquet")
            con.execute(f"""
                COPY (
                    SELECT 
                        mes_competencia::INTEGER AS mes_competencia,
                        uf::VARCHAR AS uf,
                        cod_municipio_siafi::VARCHAR AS cod_municipio_siafi,
                        nome_municipio::VARCHAR AS nome_municipio,
                        total_beneficiarios::INTEGER AS total_beneficiarios,
                        valor_total_pago::DECIMAL(38,9) AS valor_total_beneficio,
                        valor_medio_beneficio::DECIMAL(38,9) AS valor_medio_parcela
                    FROM '{fpath.replace(chr(92), "/")}'
                ) TO '{clean_temp.replace(chr(92), "/")}' (FORMAT PARQUET, COMPRESSION ZSTD);
            """)

            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.PARQUET,
                write_disposition="WRITE_APPEND"
            )
            with open(clean_temp, "rb") as sf:
                job = client.load_table_from_file(sf, table_fato_id, job_config=job_config)
                job.result()

            if os.path.exists(clean_temp):
                try: os.remove(clean_temp)
                except: pass

            print(f" OK em {time.time()-t_start:.1f}s!")

    # Resumo Final
    tbl_final = client.get_table(table_fato_id)
    print("\n" + "=" * 80)
    print(" >>> CARGA FINALIZADA COM SUCESSO TOTAL NO BIGQUERY! 🚀")
    print(f" 📊 Tabela: fato_bolsa_familia_consolidada")
    print(f" 📋 Total de Linhas: {tbl_final.num_rows:,}")
    print(f" 💾 Tamanho em Nuvem: {tbl_final.num_bytes / (1024*1024):.2f} MB")
    print("=" * 80)
    print("\n👉 Próximo Passo:")
    print("   Abra o seu Dashboard no Power BI Desktop e clique em 'Atualizar'!")
    print("=" * 80)

if __name__ == "__main__":
    main()
