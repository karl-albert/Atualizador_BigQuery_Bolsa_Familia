"""
========================================================================================
Projeto: Dashboard Bolsa Família (Power BI / GCP BigQuery)
Aplicativo: Carga Bolsa Família para Google BigQuery (Desktop GUI Moderna)
Autor: Karl / Antigravity Assistant
========================================================================================
Recursos:
  • Seletor de Modo: Fato Consolidada Econômica (100% Gratuito) vs Fato Detalhada Completa
  • Carga Incremental Otimizada da Fato Consolidada (~12 MB total, sem estourar cotas)
  • Sincronização Automática das Dimensões (Regional, População IBGE e Municípios)
  • Verificação Inteligente de Meses Existentes e Detecção de Pendências
  • Terminal de Logs Escuro Integrado e Não-Bloqueante (Multithread)
========================================================================================
"""

import sys
import os
import glob
import time
import shutil
import re
import threading
import tkinter as tk
from tkinter import ttk, filedialog, messagebox

# Auto-instalação de dependências caso necessário
try:
    import duckdb
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import service_account
except ImportError:
    import subprocess
    subprocess.check_call([sys.executable, "-m", "pip", "install", "duckdb", "pyarrow", "pandas", "google-cloud-bigquery", "openpyxl"])
    import duckdb
    import pandas as pd
    from google.cloud import bigquery
    from google.oauth2 import service_account

# Configurações Padrão
DEFAULT_BASE_C = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Bolsa_Familia\0_Bases_de_Dados"
DEFAULT_BASE_D = r"D:\1_Bases_Parquet"
DEFAULT_ROOT = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Bolsa_Familia"
DEFAULT_KEY = r"C:\000 - Karl\116 - Dashbords\Projetos_Power_BI\Bolsa_Familia\04_Chave_do_GCP_Google_Cloud_Platafform\bolsa-familia-brasil-507002-eb76546af1b5.json"
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

def auto_detect_parquet_dir(mode="consolidada"):
    if mode == "consolidada":
        candidates = [
            os.path.join(DEFAULT_BASE_D, "consolidado"),
            DEFAULT_BASE_D,
            os.path.join(DEFAULT_BASE_C, "consolidado"),
            DEFAULT_BASE_C
        ]
    else:
        candidates = [
            os.path.join(DEFAULT_BASE_D, "detalhado"),
            os.path.join(DEFAULT_BASE_C, "detalhado"),
            DEFAULT_BASE_D,
            DEFAULT_BASE_C
        ]
    for c in candidates:
        if os.path.exists(c) and glob.glob(os.path.join(c, "*.parquet")):
            return c
    return os.path.join(DEFAULT_BASE_D, "consolidado" if mode == "consolidada" else "detalhado")

class AppUploadBigQuery(tk.Tk):
    def __init__(self):
        super().__init__()
        self.title("☁️ Carga Bolsa Família para Google BigQuery (Analytics)")
        self.geometry("940x870")
        self.minsize(880, 750)
        self.configure(bg="#f4f6f9")
        
        self.center_window()
        
        # Variáveis Globais
        self.var_modo_carga = tk.StringVar(value="consolidada") # 'consolidada' ou 'detalhada'
        self.var_key_path = tk.StringVar(value=DEFAULT_KEY)
        self.var_parquet_dir = tk.StringVar(value=auto_detect_parquet_dir("consolidada"))
        self.var_project_id = tk.StringVar(value=PROJECT_ID)
        self.var_dataset_id = tk.StringVar(value="bolsa_familia")
        
        self.var_sync_fato = tk.BooleanVar(value=True)
        self.var_sync_regional = tk.BooleanVar(value=True)
        self.var_sync_pop = tk.BooleanVar(value=True)
        self.var_sync_mun = tk.BooleanVar(value=True)
        
        self.is_running = False
        
        self.create_widgets()
        self.check_connection_async()

    def center_window(self):
        self.update_idletasks()
        w = 940
        h = 870
        x = max(0, (self.winfo_screenwidth() // 2) - (w // 2))
        y = max(0, (self.winfo_screenheight() // 2) - (h // 2) - 20)
        self.geometry(f"{w}x{h}+{x}+{y}")

    # =========================================================================
    # THREAD-SAFE UI UPDATES
    # =========================================================================
    def safe_log(self, msg):
        def _update():
            self.txt_log.insert("end", msg + "\n")
            self.txt_log.see("end")
        self.after(0, _update)

    def safe_status(self, text, progress=None):
        def _update():
            self.lbl_status.config(text=text)
            if progress is not None:
                self.progress_bar["value"] = progress
        self.after(0, _update)

    def safe_enable_buttons(self):
        def _update():
            self.is_running = False
            self.btn_iniciar.config(state="normal", bg="#1e40af", text="🚀 INICIAR CARGA NO BIGQUERY")
            self.btn_test_conn.config(state="normal")
        self.after(0, _update)

    # =========================================================================
    # CRIAÇÃO DA INTERFACE MODERNA
    # =========================================================================
    def create_widgets(self):
        # Cabeçalho Superior
        header = tk.Frame(self, bg="#0f172a", height=80)
        header.pack(fill="x")
        
        lbl_title = tk.Label(
            header,
            text="☁️ Carga Google BigQuery: Bolsa Família (Analytics)",
            font=("Segoe UI", 15, "bold"),
            fg="#ffffff",
            bg="#0f172a"
        )
        lbl_title.pack(anchor="w", padx=25, pady=(12, 2))
        
        lbl_sub = tk.Label(
            header,
            text="Pipeline Incremental | Google Cloud Platform | Modelo Estrela Otimizado para Power BI",
            font=("Segoe UI", 9),
            fg="#38bdf8",
            bg="#0f172a"
        )
        lbl_sub.pack(anchor="w", padx=25, pady=(0, 12))

        # Container Principal
        container = tk.Frame(self, bg="#f4f6f9", padx=20, pady=10)
        container.pack(fill="both", expand=True)

        # ---------------------------------------------------------------------
        # SEÇÃO 1: MODO DE CARGA & ECONOMIA DE COTAS
        # ---------------------------------------------------------------------
        sec_modo = tk.LabelFrame(
            container,
            text=" 🎯 1. Selecione o Modo de Carga & Economia de Cotas ",
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#1e40af",
            padx=15,
            pady=8,
            relief="groove"
        )
        sec_modo.pack(fill="x", pady=(0, 8))

        # Opção 1: Fato Consolidada (Econômica)
        rb_m1 = tk.Radiobutton(
            sec_modo,
            text="⚡ Modo Econômico (Recomendado para Power BI): Subir Apenas Fato Consolidada + Dimensões",
            variable=self.var_modo_carga,
            value="consolidada",
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#15803d",
            activebackground="#ffffff",
            command=self.on_mode_change
        )
        rb_m1.pack(anchor="w")

        lbl_desc1 = tk.Label(
            sec_modo,
            text="   • Carrega apenas a tabela agregada por Município e Mês (~12 MB total) e as dimensões de UF/Cidades.\n   • 100% dentro da faixa GRATUITA do BigQuery (sem risco de exceder cotas do Google Cloud).",
            font=("Segoe UI", 8),
            fg="#475569",
            bg="#ffffff",
            justify="left"
        )
        lbl_desc1.pack(anchor="w", padx=(10, 0), pady=(0, 6))

        # Opção 2: Fato Detalhada (Completa / Pesada)
        rb_m2 = tk.Radiobutton(
            sec_modo,
            text="📦 Modo Detalhado Completo: Subir Fato Detalhada com CPFs/NIS nominais (~60 GB)",
            variable=self.var_modo_carga,
            value="detalhada",
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#b45309",
            activebackground="#ffffff",
            command=self.on_mode_change
        )
        rb_m2.pack(anchor="w")

        lbl_desc2 = tk.Label(
            sec_modo,
            text="   • Carrega todas as 600+ milhões de transações detalhadas mês a mês no BigQuery.\n   • Requer projeto com Faturamento (Billing/Cartão) ativo no Google Cloud Console devido ao tamanho.",
            font=("Segoe UI", 8),
            fg="#64748b",
            bg="#ffffff",
            justify="left"
        )
        lbl_desc2.pack(anchor="w", padx=(10, 0))

        # ---------------------------------------------------------------------
        # SEÇÃO 2: Credenciais e Conexão GCP
        # ---------------------------------------------------------------------
        sec_gcp = tk.LabelFrame(
            container,
            text=" 🔑 2. Configuração do Google Cloud Platform (GCP) ",
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#1e293b",
            padx=15,
            pady=8,
            relief="groove"
        )
        sec_gcp.pack(fill="x", pady=(0, 8))

        # Linha 1: Chave de Serviço
        tk.Label(sec_gcp, text="Chave de Serviço (Service Account JSON):", font=("Segoe UI", 8, "bold"), bg="#ffffff", fg="#475569").grid(row=0, column=0, sticky="w", pady=(0, 2))
        frame_key = tk.Frame(sec_gcp, bg="#ffffff")
        frame_key.grid(row=1, column=0, columnspan=3, sticky="we", pady=(0, 4))

        self.ent_key = tk.Entry(frame_key, textvariable=self.var_key_path, font=("Segoe UI", 9), bg="#f8fafc", relief="solid", bd=1)
        self.ent_key.pack(side="left", fill="x", expand=True, ipady=3, padx=(0, 8))

        btn_key = tk.Button(
            frame_key,
            text="Procurar...",
            command=self.browse_key,
            bg="#e2e8f0",
            fg="#1e293b",
            font=("Segoe UI", 8, "bold"),
            relief="groove",
            padx=10
        )
        btn_key.pack(side="right")

        # Linha 2: Projeto e Dataset + Botão Testar
        frame_proj = tk.Frame(sec_gcp, bg="#ffffff")
        frame_proj.grid(row=2, column=0, columnspan=3, sticky="we", pady=(2, 0))

        tk.Label(frame_proj, text="Projeto GCP:", font=("Segoe UI", 8, "bold"), bg="#ffffff", fg="#64748b").pack(side="left", padx=(0, 4))
        tk.Label(frame_proj, textvariable=self.var_project_id, font=("Segoe UI", 8, "bold"), bg="#f1f5f9", fg="#0f172a", padx=6, pady=2, relief="solid", bd=1).pack(side="left", padx=(0, 15))

        tk.Label(frame_proj, text="Dataset:", font=("Segoe UI", 8, "bold"), bg="#ffffff", fg="#64748b").pack(side="left", padx=(0, 4))
        tk.Label(frame_proj, textvariable=self.var_dataset_id, font=("Segoe UI", 8, "bold"), bg="#f1f5f9", fg="#0f172a", padx=6, pady=2, relief="solid", bd=1).pack(side="left", padx=(0, 20))

        self.lbl_conn_status = tk.Label(frame_proj, text="⏳ Checando conexão...", font=("Segoe UI", 8, "bold"), bg="#ffffff", fg="#d97706")
        self.lbl_conn_status.pack(side="left")

        self.btn_test_conn = tk.Button(
            frame_proj,
            text="🔄 Testar Conexão",
            command=self.check_connection_async,
            bg="#f8fafc",
            fg="#2563eb",
            font=("Segoe UI", 8, "bold"),
            relief="groove",
            padx=8
        )
        self.btn_test_conn.pack(side="right")

        sec_gcp.grid_columnconfigure(0, weight=1)

        # ---------------------------------------------------------------------
        # SEÇÃO 3: Pasta dos Arquivos Parquet
        # ---------------------------------------------------------------------
        sec_dir = tk.LabelFrame(
            container,
            text=" 📁 3. Origem dos Arquivos Parquet ",
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#1e293b",
            padx=15,
            pady=8,
            relief="groove"
        )
        sec_dir.pack(fill="x", pady=(0, 8))

        self.lbl_dir_title = tk.Label(sec_dir, text="Pasta dos Arquivos Parquet Consolidados (Fato Consolidada):", font=("Segoe UI", 8, "bold"), bg="#ffffff", fg="#475569")
        self.lbl_dir_title.grid(row=0, column=0, sticky="w", pady=(0, 2))
        
        frame_dir = tk.Frame(sec_dir, bg="#ffffff")
        frame_dir.grid(row=1, column=0, sticky="we", pady=(0, 3))

        self.ent_dir = tk.Entry(frame_dir, textvariable=self.var_parquet_dir, font=("Segoe UI", 9), bg="#f8fafc", relief="solid", bd=1)
        self.ent_dir.pack(side="left", fill="x", expand=True, ipady=3, padx=(0, 8))

        btn_dir = tk.Button(
            frame_dir,
            text="Procurar...",
            command=self.browse_dir,
            bg="#e2e8f0",
            fg="#1e293b",
            font=("Segoe UI", 8, "bold"),
            relief="groove",
            padx=10
        )
        btn_dir.pack(side="right")

        self.lbl_files_detected = tk.Label(sec_dir, text="Detectando arquivos...", font=("Segoe UI", 8, "italic"), bg="#ffffff", fg="#64748b")
        self.lbl_files_detected.grid(row=2, column=0, sticky="w")
        sec_dir.grid_columnconfigure(0, weight=1)

        # ---------------------------------------------------------------------
        # SEÇÃO 4: Tabelas para Sincronização no BigQuery
        # ---------------------------------------------------------------------
        sec_tabs = tk.LabelFrame(
            container,
            text=" 📊 4. Tabelas para Carga e Sincronização ",
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#1e293b",
            padx=15,
            pady=8,
            relief="groove"
        )
        sec_tabs.pack(fill="x", pady=(0, 8))

        frame_chk = tk.Frame(sec_tabs, bg="#ffffff")
        frame_chk.pack(fill="x")

        self.chk1 = tk.Checkbutton(
            frame_chk,
            text="Fato Consolidada (fato_bolsa_familia_consolidada) - Agregada por Município e Mês (~12 MB total)",
            variable=self.var_sync_fato,
            font=("Segoe UI", 9, "bold"),
            bg="#ffffff",
            fg="#0f172a",
            activebackground="#ffffff"
        )
        self.chk1.grid(row=0, column=0, sticky="w", pady=(0, 2))

        chk2 = tk.Checkbutton(
            frame_chk,
            text="Dimensão Regional (dim_regional) - 27 UFs e Mapeamento de Macro-Regiões",
            variable=self.var_sync_regional,
            font=("Segoe UI", 8),
            bg="#ffffff",
            fg="#475569",
            activebackground="#ffffff"
        )
        chk2.grid(row=1, column=0, sticky="w", pady=(0, 1))

        chk3 = tk.Checkbutton(
            frame_chk,
            text="População dos Municípios (Populacao_Municipios_UFs) - Censo/Estimativa IBGE para Indicadores per capita",
            variable=self.var_sync_pop,
            font=("Segoe UI", 8),
            bg="#ffffff",
            fg="#475569",
            activebackground="#ffffff"
        )
        chk3.grid(row=2, column=0, sticky="w", pady=(0, 1))

        chk4 = tk.Checkbutton(
            frame_chk,
            text="Dimensão Municípios (dim_municipios) - Códigos SIAFI e Nomes Oficiais",
            variable=self.var_sync_mun,
            font=("Segoe UI", 8),
            bg="#ffffff",
            fg="#475569",
            activebackground="#ffffff"
        )
        chk4.grid(row=3, column=0, sticky="w")

        # ---------------------------------------------------------------------
        # BOTÃO PRINCIPAL DE AÇÃO
        # ---------------------------------------------------------------------
        self.btn_iniciar = tk.Button(
            container,
            text="🚀 INICIAR CARGA NO BIGQUERY (MODO ECONÔMICO)",
            command=self.start_upload,
            font=("Segoe UI", 11, "bold"),
            bg="#1e40af",
            fg="#ffffff",
            activebackground="#1d4ed8",
            activeforeground="#ffffff",
            relief="raised",
            bd=0,
            cursor="hand2",
            pady=8
        )
        self.btn_iniciar.pack(fill="x", pady=(0, 6))

        # ---------------------------------------------------------------------
        # BARRA DE PROGRESSO & STATUS
        # ---------------------------------------------------------------------
        frame_stat = tk.Frame(container, bg="#f4f6f9")
        frame_stat.pack(fill="x", pady=(0, 3))

        self.lbl_status = tk.Label(
            frame_stat,
            text="Pronto para sincronizar com o BigQuery.",
            font=("Segoe UI", 9, "bold"),
            bg="#f4f6f9",
            fg="#334155"
        )
        self.lbl_status.pack(anchor="w")

        style = ttk.Style()
        style.theme_use('clam')
        style.configure("Custom.Horizontal.TProgressbar", foreground='#22c55e', background='#22c55e', troughcolor='#e2e8f0')

        self.progress_bar = ttk.Progressbar(
            container,
            orient="horizontal",
            mode="determinate",
            style="Custom.Horizontal.TProgressbar"
        )
        self.progress_bar.pack(fill="x", pady=(0, 6))

        # ---------------------------------------------------------------------
        # TERMINAL DE LOGS MODERNO (DARK SLATE)
        # ---------------------------------------------------------------------
        frame_log = tk.Frame(container, bg="#0f172a", bd=1, relief="solid")
        frame_log.pack(fill="both", expand=True)

        header_log = tk.Frame(frame_log, bg="#1e293b", height=22)
        header_log.pack(fill="x")

        tk.Label(header_log, text="📋 Console de Execução & Resumo da Carga", font=("Segoe UI", 8, "bold"), fg="#94a3b8", bg="#1e293b").pack(side="left", padx=8, pady=2)

        btn_clear = tk.Button(
            header_log,
            text="Limpar",
            command=lambda: self.txt_log.delete("1.0", "end"),
            bg="#1e293b",
            fg="#38bdf8",
            font=("Segoe UI", 7, "bold"),
            bd=0,
            cursor="hand2"
        )
        btn_clear.pack(side="right", padx=8)

        self.txt_log = tk.Text(
            frame_log,
            bg="#0b0f19",
            fg="#e2e8f0",
            font=("Consolas", 8),
            wrap="word",
            bd=0,
            padx=10,
            pady=6
        )
        self.txt_log.pack(side="left", fill="both", expand=True)

        scrollbar = tk.Scrollbar(frame_log, command=self.txt_log.yview)
        scrollbar.pack(side="right", fill="y")
        self.txt_log.config(yscrollcommand=scrollbar.set)

    # =========================================================================
    # EVENTOS E FUNÇÕES DE ARQUIVOS
    # =========================================================================
    def on_mode_change(self):
        m = self.var_modo_carga.get()
        if m == "consolidada":
            self.lbl_dir_title.config(text="Pasta dos Arquivos Parquet Consolidados (Fato Consolidada):")
            self.chk1.config(text="Fato Consolidada (fato_bolsa_familia_consolidada) - Agregada por Município e Mês (~12 MB total)")
            self.btn_iniciar.config(text="🚀 INICIAR CARGA NO BIGQUERY (MODO ECONÔMICO)")
            self.var_parquet_dir.set(auto_detect_parquet_dir("consolidada"))
        else:
            self.lbl_dir_title.config(text="Pasta dos Arquivos Parquet Detalhados (Fato Detalhada Completa):")
            self.chk1.config(text="Fato Detalhada Completa (fato_bolsa_familia_detalhada) - 600+ Milhões de Linhas (~60 GB)")
            self.btn_iniciar.config(text="🚀 INICIAR CARGA NO BIGQUERY (MODO COMPLETO)")
            self.var_parquet_dir.set(auto_detect_parquet_dir("detalhada"))
        self.check_connection_async()

    def browse_key(self):
        filename = filedialog.askopenfilename(
            title="Selecione a Chave de Serviço GCP (JSON)",
            filetypes=[("Arquivos JSON", "*.json"), ("Todos os arquivos", "*.*")]
        )
        if filename:
            self.var_key_path.set(filename)
            self.check_connection_async()

    def browse_dir(self):
        dirname = filedialog.askdirectory(title="Selecione a Pasta dos Arquivos Parquet")
        if dirname:
            self.var_parquet_dir.set(dirname)
            self.check_connection_async()

    def check_connection_async(self):
        threading.Thread(target=self._worker_check_conn, daemon=True).start()

    def _worker_check_conn(self):
        key_path = self.var_key_path.get().strip()
        if not os.path.exists(key_path):
            self.lbl_conn_status.config(text="❌ Chave JSON não encontrada", fg="#dc2626")
            return

        try:
            creds = service_account.Credentials.from_service_account_file(key_path)
            client = bigquery.Client(project=PROJECT_ID, credentials=creds)
            ds = client.get_dataset("bolsa_familia")
            
            # Checar meses existentes na tabela alvo
            table_target = "fato_bolsa_familia_consolidada" if self.var_modo_carga.get() == "consolidada" else "fato_bolsa_familia_detalhada"
            q = f"SELECT DISTINCT mes_competencia FROM `bolsa-familia-brasil-507002.bolsa_familia.{table_target}`"
            job = client.query(q, location="US")
            months = sorted([r.mes_competencia for r in job])
            
            self.lbl_conn_status.config(
                text=f"✅ Conectado ao BigQuery ({len(months)} meses gravados em {table_target})",
                fg="#16a34a"
            )
            self.update_files_detected(months)
        except Exception as e:
            self.lbl_conn_status.config(text=f"⚠️ Erro ao conectar: {str(e)[:35]}...", fg="#d97706")
            self.update_files_detected([])

    def update_files_detected(self, bq_months=None):
        p_dir = self.var_parquet_dir.get().strip()
        if not os.path.exists(p_dir):
            self.lbl_files_detected.config(text="⚠️ Pasta informada não existe.", fg="#dc2626")
            return

        # Procurar parquets em p_dir e subpastas
        files = glob.glob(os.path.join(p_dir, "*.parquet"))
        if not files:
            sub = "consolidado" if self.var_modo_carga.get() == "consolidada" else "detalhado"
            files = glob.glob(os.path.join(p_dir, sub, "*.parquet"))

        if not files:
            self.lbl_files_detected.config(text="Nenhum arquivo Parquet encontrado nesta pasta.", fg="#64748b")
            return

        months_in_disk = []
        for f in files:
            ym = extract_yyyymm(os.path.basename(f))
            if ym: months_in_disk.append(ym)

        if bq_months is not None:
            pendentes = [m for m in months_in_disk if m not in bq_months]
            if pendentes:
                self.lbl_files_detected.config(
                    text=f"📦 {len(files)} arquivos detectados ({len(pendentes)} novo mês pendente: {pendentes})",
                    fg="#2563eb"
                )
            else:
                self.lbl_files_detected.config(
                    text=f"✨ {len(files)} arquivos detectados. Todos já constam no BigQuery!",
                    fg="#16a34a"
                )
        else:
            self.lbl_files_detected.config(text=f"{len(files)} arquivos Parquet detectados.", fg="#334155")

    # =========================================================================
    # EXECUÇÃO DO PIPELINE DE CARGA
    # =========================================================================
    def start_upload(self):
        if self.is_running:
            return

        key_path = self.var_key_path.get().strip()
        if not os.path.exists(key_path):
            messagebox.showerror("Erro", f"Chave de serviço GCP não encontrada:\n{key_path}")
            return

        self.is_running = True
        modo_txt = "MODO ECONÔMICO" if self.var_modo_carga.get() == "consolidada" else "MODO COMPLETO"
        self.btn_iniciar.config(state="disabled", bg="#64748b", text=f"⏳ ENVIANDO DADOS ({modo_txt})...")
        self.btn_test_conn.config(state="disabled")
        self.progress_bar["value"] = 5
        self.safe_status("Iniciando conexão e validação...")
        
        threading.Thread(target=self._worker_upload, daemon=True).start()

    def _worker_upload(self):
        t0 = time.time()
        key_path = self.var_key_path.get().strip()
        p_dir = self.var_parquet_dir.get().strip()
        modo = self.var_modo_carga.get()

        try:
            self.safe_log("=" * 70)
            self.safe_log(f" >>> INICIANDO CARGA BOLSA FAMÍLIA -> BIGQUERY ({modo.upper()})")
            self.safe_log(f" [PROJETO]: {PROJECT_ID}")
            self.safe_log(f" [DATASET]: {self.var_dataset_id.get()}")
            if modo == "consolidada":
                self.safe_log(" [ESTRATÉGIA]: Carga Econômica / Gratuita da Fato Consolidada (~12 MB)")
            else:
                self.safe_log(" [ESTRATÉGIA]: Carga Completa da Fato Detalhada (~60 GB)")
            self.safe_log("=" * 70)

            # 1. Conexão BigQuery
            creds = service_account.Credentials.from_service_account_file(key_path)
            client = bigquery.Client(project=PROJECT_ID, credentials=creds)
            dataset = bigquery.Dataset(DATASET_ID)
            dataset.location = "US"
            dataset = client.create_dataset(dataset, exists_ok=True)
            self.safe_log("✅ Dataset 'bolsa_familia' conectado com sucesso na região US.")
            self.safe_status("Dataset conectado...", 15)

            # Sincronização prévia de diretórios locais
            c_cons = os.path.join(DEFAULT_BASE_C, "consolidado")
            d_cons = os.path.join(DEFAULT_BASE_D, "consolidado")
            c_det = os.path.join(DEFAULT_BASE_C, "detalhado")
            d_det = os.path.join(DEFAULT_BASE_D, "detalhado")
            os.makedirs(c_cons, exist_ok=True)
            os.makedirs(c_det, exist_ok=True)

            # -----------------------------------------------------------------
            # 1. DIMENSÃO REGIONAL
            # -----------------------------------------------------------------
            if self.var_sync_regional.get():
                self.safe_status("Sincronizando dim_regional...", 25)
                table_regional_id = f"{DATASET_ID}.dim_regional"
                try:
                    client.get_table(table_regional_id)
                    self.safe_log("ℹ️ Tabela dim_regional já existe no BigQuery.")
                except Exception:
                    self.safe_log("Criando tabela dim_regional...")
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
                    self.safe_log("✅ Tabela dim_regional criada com 27 UFs e Regiões!")

            # -----------------------------------------------------------------
            # 2. POPULAÇÃO DOS MUNICÍPIOS (IBGE)
            # -----------------------------------------------------------------
            if self.var_sync_pop.get():
                self.safe_status("Sincronizando Populacao_Municipios_UFs...", 35)
                table_pop_id = f"{DATASET_ID}.Populacao_Municipios_UFs"
                try:
                    tbl_pop = client.get_table(table_pop_id)
                    self.safe_log(f"ℹ️ Tabela Populacao_Municipios_UFs já existe ({tbl_pop.num_rows:,} municípios IBGE).")
                except Exception:
                    pop_excel = os.path.join(DEFAULT_ROOT, "Populacao_Municipios_UFs.xlsx")
                    if os.path.exists(pop_excel):
                        self.safe_log("Carregando Tabela de População IBGE...")
                        df_pop = pd.read_excel(pop_excel)
                        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_TRUNCATE")
                        job = client.load_table_from_dataframe(df_pop, table_pop_id, job_config=job_config)
                        job.result()
                        self.safe_log(f"✅ Tabela Populacao_Municipios_UFs criada com {len(df_pop):,} municípios!")

            # -----------------------------------------------------------------
            # 3. DIMENSÃO MUNICÍPIOS
            # -----------------------------------------------------------------
            if self.var_sync_mun.get():
                self.safe_status("Sincronizando dim_municipios...", 45)
                table_mun_id = f"{DATASET_ID}.dim_municipios"
                dim_mun_file = os.path.join(DEFAULT_BASE_D, "dimensoes", "dim_municipios.parquet")
                if not os.path.exists(dim_mun_file):
                    dim_mun_file = os.path.join(DEFAULT_BASE_C, "dimensoes", "dim_municipios.parquet")

                if os.path.exists(dim_mun_file):
                    try:
                        client.get_table(table_mun_id)
                        self.safe_log("ℹ️ Tabela dim_municipios já existe no BigQuery.")
                    except Exception:
                        self.safe_log("Carregando Tabela dim_municipios...")
                        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.PARQUET, write_disposition="WRITE_TRUNCATE")
                        with open(dim_mun_file, "rb") as sf:
                            job = client.load_table_from_file(sf, table_mun_id, job_config=job_config)
                            job.result()
                        self.safe_log("✅ Tabela dim_municipios criada com sucesso!")

            # -----------------------------------------------------------------
            # 4. MODO A: FATO CONSOLIDADA (ECONÔMICA - POWER BI)
            # -----------------------------------------------------------------
            if modo == "consolidada" and self.var_sync_fato.get():
                self.safe_status("Consultando meses existentes na Fato Consolidada...", 55)
                table_fato_id = f"{DATASET_ID}.fato_bolsa_familia_consolidada"

                try:
                    query_m = f"SELECT DISTINCT mes_competencia FROM `{table_fato_id}`"
                    job_m = client.query(query_m, location="US")
                    meses_carregados = set(r.mes_competencia for r in job_m)
                    self.safe_log(f"Meses já gravados no BigQuery ({len(meses_carregados)} meses): {sorted(list(meses_carregados))}")
                except Exception:
                    meses_carregados = set()

                # Coletar arquivos Parquet consolidados
                cons_files = glob.glob(os.path.join(c_cons, "*.parquet"))
                if not cons_files and os.path.exists(d_cons):
                    cons_files = glob.glob(os.path.join(d_cons, "*.parquet"))

                arquivos_pendentes = []
                for fpath in sorted(cons_files):
                    fname = os.path.basename(fpath)
                    ym = extract_yyyymm(fname)
                    if ym and ym in meses_carregados:
                        continue
                    arquivos_pendentes.append((ym, fpath))

                if not arquivos_pendentes:
                    self.safe_log("\n✨ Todos os arquivos consolidados já estão gravados no BigQuery!")
                else:
                    self.safe_log(f"\n📦 Arquivos consolidados pendentes para carregar: {len(arquivos_pendentes)}")
                    con = duckdb.connect()
                    temp_dir = os.path.join(DEFAULT_ROOT, "0_Bases_de_Dados", "temp")
                    os.makedirs(temp_dir, exist_ok=True)

                    total_pend = len(arquivos_pendentes)
                    for idx, (ym, fpath) in enumerate(arquivos_pendentes, start=1):
                        fname = os.path.basename(fpath)
                        fsize = os.path.getsize(fpath)
                        prog_val = 60 + int((idx / total_pend) * 35)
                        self.safe_status(f"Subindo {fname} ({idx}/{total_pend})...", prog_val)
                        t_sub = time.time()
                        self.safe_log(f" [{idx:02d}/{total_pend:02d}] Subindo {fname} ({format_bytes(fsize)})...")

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

                        # Sincronizar também fato_bolsa_familia_com_genero se houver arquivo detalhado correspondente
                        det_candidate = os.path.join(DEFAULT_BASE_C, "detalhado", f"{ym}_NovoBolsaFamilia.parquet")
                        if not os.path.exists(det_candidate):
                            det_candidate = os.path.join(DEFAULT_BASE_D, "detalhado", f"{ym}_NovoBolsaFamilia_fato.parquet")

                        if os.path.exists(det_candidate):
                            try:
                                clean_gen_temp = os.path.join(temp_dir, f"{ym}_gen_clean.parquet")
                                con.execute(f"""
                                    COPY (
                                        WITH raw_data AS (
                                            SELECT 
                                                mes_competencia,
                                                uf,
                                                nome_municipio,
                                                SPLIT_PART(nome_favorecido, ' ', 1) AS primeiro_nome,
                                                nis_favorecido,
                                                valor_parcela
                                            FROM '{det_candidate.replace(chr(92), "/")}'
                                        )
                                        SELECT 
                                            r.mes_competencia::INTEGER AS mes_competencia,
                                            r.uf::VARCHAR AS uf,
                                            r.nome_municipio::VARCHAR AS nome_municipio,
                                            IF(r.primeiro_nome LIKE '%A', 'Feminino', 'Masculino')::VARCHAR AS Genero,
                                            COUNT(r.nis_favorecido)::INTEGER AS total_beneficiarios,
                                            SUM(r.valor_parcela)::DECIMAL(38,9) AS valor_total_pago
                                        FROM raw_data r
                                        GROUP BY r.mes_competencia, r.uf, r.nome_municipio, 4
                                    ) TO '{clean_gen_temp.replace(chr(92), "/")}' (FORMAT PARQUET, COMPRESSION ZSTD);
                                """)
                                table_gen_id = f"{DATASET_ID}.fato_bolsa_familia_com_genero"
                                with open(clean_gen_temp, "rb") as sf_gen:
                                    job_gen = client.load_table_from_file(sf_gen, table_gen_id, job_config=job_config)
                                    job_gen.result()
                                if os.path.exists(clean_gen_temp):
                                    try: os.remove(clean_gen_temp)
                                    except: pass
                                self.safe_log(f"   -> Sincronizado também com fato_bolsa_familia_com_genero!")
                            except Exception as e_gen:
                                self.safe_log(f"   ⚠️ Aviso gênero: {e_gen}")

                        if os.path.exists(clean_temp):
                            try: os.remove(clean_temp)
                            except: pass

                        self.safe_log(f"   -> OK em {time.time()-t_sub:.1f}s!")

            # -----------------------------------------------------------------
            # 5. MODO B: FATO DETALHADA COMPLETA (OPCIONAL)
            # -----------------------------------------------------------------
            elif modo == "detalhada" and self.var_sync_fato.get():
                self.safe_status("Consultando meses existentes na Fato Detalhada...", 55)
                table_fato_id = f"{DATASET_ID}.fato_bolsa_familia_detalhada"

                try:
                    query_m = f"SELECT DISTINCT mes_competencia FROM `{table_fato_id}`"
                    job_m = client.query(query_m, location="US")
                    meses_carregados = set(r.mes_competencia for r in job_m)
                    self.safe_log(f"Meses detalhados no BigQuery ({len(meses_carregados)} meses): {sorted(list(meses_carregados))}")
                except Exception:
                    meses_carregados = set()

                det_files = glob.glob(os.path.join(c_det, "*.parquet"))
                if not det_files and os.path.exists(d_det):
                    det_files = glob.glob(os.path.join(d_det, "*.parquet"))

                arquivos_pendentes = []
                for fpath in sorted(det_files):
                    fname = os.path.basename(fpath)
                    ym = extract_yyyymm(fname)
                    if ym and ym in meses_carregados:
                        continue
                    arquivos_pendentes.append((ym, fpath))

                if not arquivos_pendentes:
                    self.safe_log("\n✨ Todos os arquivos detalhados já estão gravados no BigQuery!")
                else:
                    self.safe_log(f"\n📦 Arquivos detalhados pendentes para subir: {len(arquivos_pendentes)}")
                    total_pend = len(arquivos_pendentes)
                    for idx, (ym, fpath) in enumerate(arquivos_pendentes, start=1):
                        fname = os.path.basename(fpath)
                        fsize = os.path.getsize(fpath)
                        prog_val = 60 + int((idx / total_pend) * 35)
                        self.safe_status(f"Subindo {fname} ({idx}/{total_pend})...", prog_val)
                        t_sub = time.time()
                        self.safe_log(f" [{idx:02d}/{total_pend:02d}] Subindo {fname} ({format_bytes(fsize)})...")

                        job_config = bigquery.LoadJobConfig(
                            source_format=bigquery.SourceFormat.PARQUET,
                            write_disposition="WRITE_APPEND"
                        )
                        with open(fpath, "rb") as sf:
                            job = client.load_table_from_file(sf, table_fato_id, job_config=job_config)
                            job.result()

                        self.safe_log(f"   -> OK em {time.time()-t_sub:.1f}s!")

            # -----------------------------------------------------------------
            # RESUMO FINAL
            # -----------------------------------------------------------------
            self.safe_status("Finalizando resumo...", 100)
            table_final_id = f"{DATASET_ID}.fato_bolsa_familia_consolidada" if modo == "consolidada" else f"{DATASET_ID}.fato_bolsa_familia_detalhada"
            tbl_final = client.get_table(table_final_id)
            elapsed = time.time() - t0

            self.safe_log("\n" + "=" * 70)
            self.safe_log(" >>> PROCESSO CONCLUÍDO COM SUCESSO TOTAL! 🚀")
            self.safe_log("=" * 70)
            self.safe_log(f"⏱️ Tempo de Execução: {elapsed:.1f} segundos")
            self.safe_log(f"📊 Tabela Atualizada: {tbl_final.table_id}")
            self.safe_log(f"📋 Total de Registros: {tbl_final.num_rows:,} linhas")
            self.safe_log(f"💾 Espaço em Nuvem: {tbl_final.num_bytes / (1024*1024):.2f} MB")
            self.safe_log("=" * 70)
            self.safe_log("\n👉 PRÓXIMO PASSO NO POWER BI:")
            self.safe_log("   Abra o 'Bolsa_Familia_V3.pbix' no Power BI Desktop e clique em 'Atualizar'!")

            self.safe_status(f"Carga Concluída com Sucesso em {elapsed:.1f}s!", 100)
            messagebox.showinfo(
                "Sucesso",
                f"Carga no BigQuery finalizada com sucesso em {elapsed:.1f}s!\n\n"
                f"Modo: {'Econômico (Fato Consolidada)' if modo == 'consolidada' else 'Completo (Fato Detalhada)'}\n"
                f"Total de Linhas: {tbl_final.num_rows:,}\n"
                f"Tamanho em Nuvem: {tbl_final.num_bytes/(1024*1024):.2f} MB\n\n"
                "Pode atualizar seu Power BI agora!"
            )

        except Exception as e:
            self.safe_log(f"\n❌ [ERRO CRÍTICO]: {e}")
            self.safe_status(f"Erro: {str(e)[:50]}...", 0)
            messagebox.showerror("Erro na Carga", f"Ocorreu um erro durante a carga no BigQuery:\n\n{e}")
        finally:
            self.safe_enable_buttons()

def main():
    app = AppUploadBigQuery()
    app.mainloop()

if __name__ == "__main__":
    main()
