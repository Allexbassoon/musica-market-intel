"""
Módulo de Coleta (Ingestão) - Camada Bronze (Raw)
---------------------------------------------------
Responsabilidade:
1. Baixar PDFs de links diretos (Seeds).
2. Validar integridade via Magic Bytes (%PDF) IGNORANDO Content-Type se necessário.
3. Gerenciar Manifesto (Parquet) com rastreabilidade de redirecionamentos.
4. Resiliência: Retry inteligente para erros 429 e 5xx.
5. Fail-fast: Verifica dependências críticas.
6. Resource Safety: Garante fechamento de conexões HTTP via Context Manager.

Autor: Projeto Musical (Estratégia Sênior)
Versão: 1.5 (Gold Standard - Resource Safe)
"""

import os
import sys
import time
import hashlib
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Tuple

import pandas as pd
import requests

# --- VERIFICAÇÃO FAIL-FAST ---
try:
    import pyarrow
except ImportError:
    print("ERRO CRÍTICO: A biblioteca 'pyarrow' não foi encontrada.")
    print("Por favor, execute: pip install pyarrow")
    sys.exit(1)

# --- CONFIGURAÇÃO GLOBAL ---
FORCE_REDOWNLOAD = False

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
FILES_DIR = DATA_RAW_DIR / "files"
LOGS_DIR = BASE_DIR / "logs"
MANIFEST_PATH = DATA_RAW_DIR / "manifest.parquet"

# --- CONFIGURAÇÃO DE LOGS ---
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "ingestion.log", mode="a", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# --- LISTA DE SEEDS (MVP) ---
SEEDS = [
    {
        "source_name": "OSB",
        "source_url": "https://www.osb.com.br/",
        "download_url": "https://www.osb.com.br/wp-content/uploads/2025/11/Processo-Seletivo-OSB-2026.1.pdf",
    },
    {
        "source_name": "OSB",
        "source_url": "https://www.osb.com.br/",
        "download_url": "https://www.osb.com.br/wp-content/uploads/2025/05/Auditions-Annoucement-OSB-2024.pdf",
    },
    {
        "source_name": "Filarmonica_MG",
        "source_url": "https://filarmonica.art.br/",
        "download_url": "https://filarmonica.art.br/wp-content/uploads/2025/03/edital-audicoes-01.25-pt.pdf",
    },
    {
        "source_name": "OSBA",
        "source_url": "https://amigostca.org.br/",
        "download_url": "https://amigostca.org.br/wp-content/uploads/2025/06/Edital-07_2025_Selecao_OSBA-2025.pdf",
    },
]

# Schema do Manifesto
MANIFEST_SCHEMA = [
    "doc_id",
    "source_name",
    "source_url",
    "download_url",
    "final_url",
    "redirected",
    "downloaded_at",
    "http_status",
    "content_type",
    "file_path",
    "file_size",
    "sha256",
    "pdf_is_text",
    "needs_ocr",
    "notes",
    "raw_download_ok",
    "parsed_ok",
    "extracted_ok",
    "last_error_stage",
    "last_error_msg",
]

def setup_environment() -> None:
    DATA_RAW_DIR.mkdir(parents=True, exist_ok=True)
    FILES_DIR.mkdir(parents=True, exist_ok=True)
    logger.info(f"Ambiente verificado. Root: {BASE_DIR}")

def load_manifest() -> pd.DataFrame:
    if MANIFEST_PATH.exists():
        try:
            df = pd.read_parquet(MANIFEST_PATH)
            for col in MANIFEST_SCHEMA:
                if col not in df.columns:
                    df[col] = None
            return df
        except Exception as e:
            backup_path = MANIFEST_PATH.with_suffix(f".parquet.bak_{int(time.time())}")
            logger.error(f"Erro ao ler manifesto. Backup criado em {backup_path}. Erro: {e}")
            try:
                os.rename(MANIFEST_PATH, backup_path)
            except OSError:
                pass
    return pd.DataFrame(columns=MANIFEST_SCHEMA)

def save_manifest(df: pd.DataFrame) -> None:
    tmp_path = MANIFEST_PATH.with_suffix(".parquet.tmp")
    try:
        df.to_parquet(tmp_path, index=False, engine='pyarrow')
        tmp_path.replace(MANIFEST_PATH)
        logger.info(f"Manifesto salvo. Total de registros: {len(df)}")
    except Exception as e:
        logger.critical(f"FALHA CRÍTICA ao salvar manifesto: {e}")

def calculate_hashes(content: bytes) -> Tuple[str, str]:
    md5 = hashlib.md5(content).hexdigest()
    sha256 = hashlib.sha256(content).hexdigest()
    return md5, sha256

def is_valid_pdf(content: bytes, content_type: str) -> bool:
    if not content: return False
    
    # 1. Verifica Magic Bytes (Padrão Ouro)
    if b"%PDF" in content[:1024]:
        return True
    
    # 2. Se não achou Magic Bytes, rejeita, mas loga se header enganou.
    ct = (content_type or "").lower()
    if ("pdf" in ct) or ("application/octet-stream" in ct):
        logger.warning(f"REJEITADO: Header diz '{ct}', mas não encontramos assinatura %PDF.")
    
    return False

def download_file(session: requests.Session, url: str, max_retries: int = 3) -> Tuple[Optional[bytes], int, str, str, str, bool]:
    attempt = 0
    backoff = 2
    headers = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) MarketIntelBot/1.0"}

    while attempt < max_retries:
        try:
            # USO CORRETO DE RESOURCE MANAGEMENT (Context Manager)
            # Garante que a conexão socket seja liberada imediatamente após o uso.
            with session.get(url, headers=headers, timeout=(10, 30), stream=True, allow_redirects=True) as resp:
                
                # Lógica de Retry para Sobrecarga (429/5xx)
                if resp.status_code in [429, 500, 502, 503, 504]:
                    logger.warning(f"HTTP {resp.status_code} em {url}. Tentativa {attempt+1}/{max_retries}. Aguardando {backoff}s...")
                    time.sleep(backoff)
                    attempt += 1
                    backoff *= 2
                    continue 
                
                final_url = resp.url
                redirected = (resp.history and len(resp.history) > 0) or (final_url != url)
                content_type = resp.headers.get("Content-Type", "")

                if resp.status_code != 200:
                    return None, resp.status_code, content_type, f"HTTP {resp.status_code}", final_url, redirected

                # Lê o conteúdo para memória dentro do bloco 'with'
                content = resp.content 
                return content, resp.status_code, content_type, "", final_url, redirected

        except requests.exceptions.RequestException as e:
            attempt += 1
            logger.warning(f"Erro de Conexão na tentativa {attempt}/{max_retries}: {e}")
            time.sleep(backoff)
            backoff *= 2

    return None, 0, "", "Max retries exceeded", url, False

def process_seed(seed: Dict, manifest: pd.DataFrame, session: requests.Session) -> pd.DataFrame:
    url = seed["download_url"]
    source_name = seed["source_name"]

    # Idempotência
    if not FORCE_REDOWNLOAD and not manifest.empty:
        existing = manifest[(manifest["download_url"] == url) & (manifest["raw_download_ok"] == True)]
        if not existing.empty:
            logger.info(f"SKIP: URL já processada -> {url}")
            return manifest

    logger.info(f"BAIXANDO: {source_name} -> {url}")

    content, status, c_type, error, final_url, redirected = download_file(session, url)

    record = {col: None for col in MANIFEST_SCHEMA}
    record.update({
        "source_name": source_name,
        "source_url": seed["source_url"],
        "download_url": url,
        "final_url": final_url,
        "redirected": redirected,
        "downloaded_at": datetime.now().isoformat(),
        "http_status": status,
        "content_type": c_type,
        "raw_download_ok": False,
        "parsed_ok": False,
        "extracted_ok": False,
        "notes": "FORCE_REDOWNLOAD=True" if FORCE_REDOWNLOAD else None,
    })

    if not content or status != 200:
        record["last_error_stage"] = "download"
        record["last_error_msg"] = error or f"HTTP {status}"
        logger.error(f"ERRO DOWNLOAD: {record['last_error_msg']}")
        return pd.concat([manifest, pd.DataFrame([record])], ignore_index=True)

    if not is_valid_pdf(content, c_type):
        record["last_error_stage"] = "validation"
        record["last_error_msg"] = "Arquivo rejeitado (Magic Bytes inválidos)."
        logger.error(record["last_error_msg"])
        return pd.concat([manifest, pd.DataFrame([record])], ignore_index=True)

    md5_id, sha256 = calculate_hashes(content)
    record["doc_id"] = md5_id
    record["sha256"] = sha256
    record["file_size"] = len(content)

    if not manifest.empty and ("sha256" in manifest.columns) and (sha256 in manifest["sha256"].dropna().values):
        logger.info("Conteúdo duplicado (SHA256 idêntico). Reutilizando arquivo físico.")
        existing_row = manifest[manifest["sha256"] == sha256].dropna(subset=["file_path"]).head(1)
        if not existing_row.empty:
            record["file_path"] = existing_row.iloc[0]["file_path"]
        record["notes"] = (str(record["notes"] or "") + " | conteúdo idêntico anterior").strip(" | ")
        record["raw_download_ok"] = True
    else:
        filename = f"{md5_id}.pdf"
        file_path = FILES_DIR / filename
        try:
            with open(file_path, "wb") as f:
                f.write(content)
            record["file_path"] = str(file_path.relative_to(BASE_DIR))
            record["raw_download_ok"] = True
            logger.info(f"SALVO: {filename}")
        except Exception as e:
            record["last_error_stage"] = "save_to_disk"
            record["last_error_msg"] = str(e)
            logger.error(f"ERRO DISCO: {e}")

    return pd.concat([manifest, pd.DataFrame([record])], ignore_index=True)

def main() -> None:
    setup_environment()
    logger.info("--- START: Pipeline de Coleta (Raw v1.5) ---")

    manifest = load_manifest()

    with requests.Session() as session:
        for seed in SEEDS:
            try:
                manifest = process_seed(seed, manifest, session)
                time.sleep(0.8) 
            except Exception as e:
                logger.error(f"ERRO NÃO TRATADO na seed {seed.get('download_url')}: {e}")

    save_manifest(manifest)
    logger.info("--- END: Pipeline Finalizado ---")

if __name__ == "__main__":
    main()