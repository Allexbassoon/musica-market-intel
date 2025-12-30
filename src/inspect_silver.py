import pandas as pd
from pathlib import Path

# --- CONFIGURAÇÕES VISUAIS ---
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.float_format', '{:.1f}'.format)

# --- CAMINHOS ---
BASE_DIR = Path(__file__).resolve().parent.parent
SILVER_PATH = BASE_DIR / "data" / "intermediate" / "parsed_pages.parquet"
MANIFEST_PATH = BASE_DIR / "data" / "raw" / "manifest.parquet"

def inspect_final():
    print("\n" + "="*70)
    print("🔎 INSPEÇÃO SILVER (FINAL) - AUDITORIA BLINDADA")
    print("="*70 + "\n")

    if not SILVER_PATH.exists() or not MANIFEST_PATH.exists():
        print("❌ Arquivos necessários não encontrados.")
        return

    # 1. CARGA OTIMIZADA E SEGURA
    try:
        # Leitura Otimizada (Silver)
        silver_cols = ['doc_id', 'page_num', 'char_count', 'full_text']
        df_silver = pd.read_parquet(SILVER_PATH, columns=silver_cols)
        
        # Leitura Otimizada (Manifesto)
        try:
            manifest_cols = ['doc_id', 'source_name', 'downloaded_at', 'raw_download_ok']
            df_manifest = pd.read_parquet(MANIFEST_PATH, columns=manifest_cols)
        except Exception:
            df_manifest = pd.read_parquet(MANIFEST_PATH)

        # BLINDAGEM DE TIPOS
        df_silver['doc_id'] = df_silver['doc_id'].astype(str)
        df_manifest['doc_id'] = df_manifest['doc_id'].astype(str)

        # Tratamento de colunas ausentes
        if 'source_name' not in df_manifest.columns:
            df_manifest['source_name'] = "DESCONHECIDO"
        else:
            df_manifest['source_name'] = df_manifest['source_name'].fillna("DESCONHECIDO")

        # Tratamento de char_count (Numérico)
        df_silver['char_count'] = pd.to_numeric(df_silver['char_count'], errors='coerce').fillna(0).astype(int)

    except Exception as e:
        print(f"❌ Erro crítico na carga de dados: {e}")
        return

    if df_silver.empty:
        print("⚠️ Arquivo Silver vazio.")
        return

    # 2. DEDUPLICAÇÃO INTELIGENTE
    if 'downloaded_at' in df_manifest.columns:
        df_manifest['downloaded_at'] = pd.to_datetime(df_manifest['downloaded_at'], errors='coerce')
    
    sort_keys = ['doc_id']
    sort_asc = [True]

    # Prioridade 1: Download foi bem sucedido?
    if 'raw_download_ok' in df_manifest.columns:
        # CORREÇÃO CRÍTICA (ChatGPT): Normaliza para booleano (Nulos viram False)
        df_manifest['raw_download_ok'] = df_manifest['raw_download_ok'].fillna(False).astype(bool)
        
        sort_keys.append('raw_download_ok')
        sort_asc.append(False) # True (1) > False (0), então Descendente
    
    # Prioridade 2: Data mais recente
    if 'downloaded_at' in df_manifest.columns:
        sort_keys.append('downloaded_at')
        sort_asc.append(False) 

    # Aplica ordenação e deduplicação
    df_manifest = df_manifest.sort_values(by=sort_keys, ascending=sort_asc, na_position='last')
    df_manifest = df_manifest.drop_duplicates(subset=['doc_id'], keep='first')

    # 3. MERGE FINAL
    df_merged = df_silver.merge(
        df_manifest[['doc_id', 'source_name']], 
        on='doc_id', 
        how='left'
    )
    df_merged['source_name'] = df_merged['source_name'].fillna("DESCONHECIDO")

    # 4. MÉTRICAS DE QUALIDADE
    print("📊 1. QUALIDADE POR EDITAL")
    
    def pct_empty(series): return (series < 20).mean() * 100
    def pct_low(series): return (series < 100).mean() * 100

    stats = df_merged.groupby(['source_name', 'doc_id']).agg(
        total_paginas=('page_num', 'count'),
        media_chars=('char_count', 'mean'),
        paginas_vazias_pct=('char_count', pct_empty),
        paginas_baixas_pct=('char_count', pct_low)
    ).reset_index()

    if stats.empty:
        print("⚠️ Nenhum dado agrupado encontrado.")
        return

    stats = stats.sort_values(by=['paginas_vazias_pct', 'paginas_baixas_pct'], ascending=False)

    stats['doc_id_short'] = stats['doc_id'].str[:8] + "..."
    cols = ['source_name', 'doc_id_short', 'total_paginas', 'media_chars', 'paginas_vazias_pct', 'paginas_baixas_pct']
    print(stats[cols].to_string(index=False))
    print("\n" + "-"*70 + "\n")

    # 5. RANKING DE PÁGINAS "MUDAS"
    print("📉 2. TOP 5 PÁGINAS COM MENOS TEXTO")
    weakest = df_merged.nsmallest(5, 'char_count')
    
    safe_text = lambda x: str(x).replace('\n', ' ')[:50] + "..." if pd.notnull(x) else "[NULO]"
    
    print(weakest[['source_name', 'page_num', 'char_count', 'full_text']].to_string(index=False, formatters={'full_text': safe_text}))
    print("\n" + "-"*70 + "\n")

    # 6. AMOSTRA DO MAIOR EDITAL
    top_doc = stats.sort_values('total_paginas', ascending=False).iloc[0]
    target_id = top_doc['doc_id']
    target_name = top_doc['source_name']
    
    print(f"📝 3. LEITURA DE PROVA: {target_name}")
    doc_pages = df_merged[df_merged['doc_id'] == target_id].sort_values('page_num')
    
    indices = sorted(list(set([0, len(doc_pages)//2, len(doc_pages)-1])))
    
    for idx in indices:
        if idx < len(doc_pages):
            row = doc_pages.iloc[idx]
            print(f"\n--- [Pág {row['page_num']}] ({row['char_count']} chars) ---")
            txt_preview = str(row['full_text']).replace('\n', ' ')[:300]
            print(f"\"{txt_preview}...\"")

    print("\n" + "="*70)
    print("✅ FIM DA AUDITORIA FINAL")

if __name__ == "__main__":
    inspect_final()