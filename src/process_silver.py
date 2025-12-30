"""
Módulo de Processamento (Silver) - Camada Intermediária
-------------------------------------------------------
Responsabilidade:
1. Ler o manifesto para identificar novos arquivos (raw_download_ok=True, parsed_ok=False).
2. Abrir PDFs usando pdfplumber.
3. Extrair texto bruto de cada página.
4. Aplicar heurística de OCR: Se % de texto for muito baixo, marcar needs_ocr=True.
5. Salvar conteúdo extraído em data/intermediate/parsed_pages.parquet.
6. Atualizar manifesto com status do processamento.

Autor: Projeto Musical (Estratégia Sênior)
Versão: 1.0 (Text Extraction + OCR Flagging)
"""

import logging
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Any

import pandas as pd
import pdfplumber
import pyarrow

# --- CONFIGURAÇÃO GLOBAL ---
BASE_DIR = Path(__file__).resolve().parent.parent
DATA_RAW_DIR = BASE_DIR / "data" / "raw"
DATA_INTER_DIR = BASE_DIR / "data" / "intermediate"
FILES_DIR = DATA_RAW_DIR / "files"
LOGS_DIR = BASE_DIR / "logs"
MANIFEST_PATH = DATA_RAW_DIR / "manifest.parquet"
SILVER_DATA_PATH = DATA_INTER_DIR / "parsed_pages.parquet"

# --- CONFIGURAÇÃO DE LOGS ---
LOGS_DIR.mkdir(parents=True, exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[
        logging.FileHandler(LOGS_DIR / "processing_silver.log", mode="a", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

def setup_environment():
    DATA_INTER_DIR.mkdir(parents=True, exist_ok=True)

def load_manifest() -> pd.DataFrame:
    if not MANIFEST_PATH.exists():
        logger.critical("Manifesto não encontrado. Execute a etapa Bronze primeiro.")
        return pd.DataFrame()
    return pd.read_parquet(MANIFEST_PATH)

def save_manifest(df: pd.DataFrame):
    # Atomic write para segurança
    tmp_path = MANIFEST_PATH.with_suffix(".parquet.tmp")
    df.to_parquet(tmp_path, index=False, engine='pyarrow')
    tmp_path.replace(MANIFEST_PATH)
    logger.info("Manifesto atualizado com status de processamento.")

def append_to_silver(new_data: List[Dict[str, Any]]):
    """
    Adiciona novas páginas extraídas ao arquivo Parquet da camada Silver.
    Se o arquivo não existir, cria. Se existir, faz append.
    """
    if not new_data:
        return

    df_new = pd.DataFrame(new_data)
    
    if SILVER_DATA_PATH.exists():
        # Lê o existente e concatena (para MVP isso é aceitável)
        df_old = pd.read_parquet(SILVER_DATA_PATH)
        df_final = pd.concat([df_old, df_new], ignore_index=True)
    else:
        df_final = df_new

    # Salva
    df_final.to_parquet(SILVER_DATA_PATH, index=False, engine='pyarrow')
    logger.info(f"Salvo {len(new_data)} novas páginas na Camada Silver.")

def process_pdf(file_path: Path, doc_id: str) -> Dict[str, Any]:
    """
    Processa um único PDF: extrai texto de todas as páginas.
    Retorna:
      - pages_data: Lista de dicionários (um por página)
      - meta_update: Dicionário para atualizar o manifesto (needs_ocr, pdf_is_text)
    """
    pages_data = []
    total_chars = 0
    total_pages = 0
    
    try:
        with pdfplumber.open(file_path) as pdf:
            total_pages = len(pdf.pages)
            
            for i, page in enumerate(pdf.pages):
                text = page.extract_text() or ""
                char_count = len(text)
                total_chars += char_count
                
                pages_data.append({
                    "doc_id": doc_id,
                    "page_num": i + 1,
                    "full_text": text,
                    "char_count": char_count,
                    "processed_at": datetime.now().isoformat()
                })
        
        # Heurística Simples: Se média de caracteres por página < 50, provavelmente é imagem (scan)
        avg_chars = total_chars / total_pages if total_pages > 0 else 0
        is_text_digital = avg_chars > 50
        
        return {
            "success": True,
            "pages_data": pages_data,
            "meta_update": {
                "pdf_is_text": is_text_digital,
                "needs_ocr": not is_text_digital,
                "parsed_ok": True,
                "last_error_msg": None
            }
        }

    except Exception as e:
        logger.error(f"Erro ao processar {file_path.name}: {e}")
        return {
            "success": False,
            "pages_data": [],
            "meta_update": {
                "parsed_ok": False,
                "last_error_stage": "silver_parsing",
                "last_error_msg": str(e)
            }
        }

def main():
    setup_environment()
    logger.info("--- START: Pipeline Silver (Processamento de Texto) ---")
    
    manifest = load_manifest()
    if manifest.empty:
        return

    # Filtra arquivos que foram baixados (raw_download_ok) mas AINDA NÃO processados (parsed_ok != True)
    mask_to_process = (manifest["raw_download_ok"] == True) & (manifest["parsed_ok"].fillna(False) == False)
    pending_files = manifest[mask_to_process]

    if pending_files.empty:
        logger.info("Nenhum arquivo pendente para processamento.")
        return

    logger.info(f"Arquivos na fila para processamento: {len(pending_files)}")
    
    all_new_pages = []

    for idx, row in pending_files.iterrows():
        doc_id = row["doc_id"]
        file_path_rel = row["file_path"]
        
        # Reconstrói caminho absoluto
        full_path = BASE_DIR / file_path_rel
        
        if not full_path.exists():
            logger.error(f"Arquivo físico não encontrado: {full_path}")
            manifest.at[idx, "last_error_msg"] = "Arquivo físico sumiu"
            continue

        logger.info(f"Processando: {row['source_name']} (ID: {doc_id[:8]}...)")
        
        result = process_pdf(full_path, doc_id)
        
        # Atualiza o manifesto em memória
        meta = result["meta_update"]
        manifest.at[idx, "parsed_ok"] = meta.get("parsed_ok")
        manifest.at[idx, "pdf_is_text"] = meta.get("pdf_is_text")
        manifest.at[idx, "needs_ocr"] = meta.get("needs_ocr")
        if not result["success"]:
            manifest.at[idx, "last_error_msg"] = meta.get("last_error_msg")
            manifest.at[idx, "last_error_stage"] = meta.get("last_error_stage")
        
        # Acumula as páginas extraídas
        if result["success"]:
            all_new_pages.extend(result["pages_data"])

    # Salva tudo
    if all_new_pages:
        append_to_silver(all_new_pages)
        save_manifest(manifest)
    else:
        logger.warning("Nenhuma página foi extraída com sucesso.")

    logger.info("--- END: Pipeline Silver Finalizado ---")

if __name__ == "__main__":
    main()