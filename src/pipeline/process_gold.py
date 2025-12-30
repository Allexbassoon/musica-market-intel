import pandas as pd
import re
import logging
import sys
import unicodedata
from pathlib import Path
from typing import List, Dict

# --- CONFIGURAÇÃO DE LOGS ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# --- CONFIGURAÇÃO DE DIRETÓRIOS ---
try:
    BASE_DIR = Path(__file__).resolve().parents[2]
except NameError:
    BASE_DIR = Path(".").resolve()

SILVER_PATH = BASE_DIR / "data" / "intermediate" / "parsed_pages.parquet"
MANIFEST_PATH = BASE_DIR / "data" / "raw" / "manifest.parquet"
GOLD_OUTPUT_DIR = BASE_DIR / "data" / "gold"
GOLD_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# --- TAXONOMIA DE INSTRUMENTOS ---
INSTRUMENT_TAXONOMY = {
    "Fagote": [r"fagote", r"fagotista"],
    "Flauta": [r"flauta", r"flautista", r"flautim", r"piccolo"],
    "Oboé": [r"obo[eé]", r"obo[ií]sta", r"corne\s+ingl[eê]s"],
    "Clarinete": [r"clarinete", r"clarinetista", r"clarineta", r"clarone"],
    "Trompa": [r"trompa", r"trompista"],
    "Trompete": [r"trompete", r"trompetista"],
    "Trombone": [r"trombone", r"trombonista"],
    "Tuba": [r"tuba", r"tubista", r"euphonium", r"bombardino"],
    "Violino": [r"violino", r"violinista", r"spalla", r"concertino"],
    "Viola": [r"\bviola(?!\w)", r"violista"], 
    "Violoncelo": [r"violoncelo", r"violoncelista", r"cello"],
    "Contrabaixo": [r"contrabaixo", r"contrabaixista", r"baixo\s+ac[uú]stico"],
    "Piano": [r"piano", r"pianista", r"correpetidor"],
    "Percussão": [r"percuss[aã]o", r"percussionista", r"t[ií]mpanos"],
    "Harpa": [r"harpa", r"harpista"],
    "Canto": [r"cantor(?:a)?", r"corista", r"solista\s+vocal", r"artista\s+do\s+coro"]
}

# --- CONFIGURAÇÃO DE SALÁRIOS ---
SALARY_CONFIG = {
    'min_val': 700.0,       
    'max_val': 45000.0,     
    'regex': r'(?<!\d)(R\$\s*)?((?:\d{1,3}(?:\.\d{3})+)|(?:\d{3,6}))(?:,(\d{2}))?(?!\d)',
    'positive_keywords': [
        'remuneracao', 'salario', 'mensal', 'cache', 'bolsa', 'vencimento', 
        'subsidio', 'honorario', 'premio', 'pagamento'
    ],
    # CORREÇÃO DEFINITIVA 2: Lista Negativa Apenas Financeira
    'negative_keywords': [
        'multa', 'taxa', 'inscricao', 'boleto', 'gru', 'darf',
        'custo', 'diaria', 'ajuda de custo', 'verba', 'recurso'
    ]
}

# --- CONFIGURAÇÃO DE VÍNCULOS ---
BOND_KEYWORDS = {
    'CLT': [r'clt', r'carteira assinada', r'regime celetista'],
    'Estatutário': [r'estatut[áa]rio', r'cargo p[úu]blico', r'concurso p[úu]blico', r'efetivo'],
    'Temporário': [r'tempor[áa]rio', r'processo seletivo', r'pss', r'determinado'],
    'Bolsa': [r'bolsa', r'bolsista', r'est[áa]gio', r'ajuda de custo']
}

class GoldProcessor:
    def __init__(self):
        self.silver_data = None
        self.manifest_data = None
        self.candidates = []
        self.salaries = []
        self.opportunities = []

    def _normalize_text(self, text: str) -> str:
        if not isinstance(text, str): return ""
        return unicodedata.normalize('NFKD', text).encode('ASCII', 'ignore').decode('utf-8').lower()

    def _has_keyword_simple(self, text: str, keywords: List[str]) -> bool:
        for kw in keywords:
            pattern = r'\b' + re.escape(kw) + r'\b'
            if re.search(pattern, text):
                return True
        return False

    def load_data(self):
        """Bloco 1: Load + Merge + Validate."""
        logger.info(f"Base Dir identificado: {BASE_DIR}")
        
        if not SILVER_PATH.exists(): raise FileNotFoundError(f"Arquivo Silver não encontrado: {SILVER_PATH}")
        self.silver_data = pd.read_parquet(SILVER_PATH)
        
        required_silver_cols = {'doc_id', 'full_text', 'page_num'}
        if not required_silver_cols.issubset(self.silver_data.columns):
            msg = f"CRÍTICO: Colunas ausentes no Silver: {required_silver_cols - set(self.silver_data.columns)}"
            logger.critical(msg); raise ValueError(msg)
            
        if not MANIFEST_PATH.exists(): raise FileNotFoundError(f"Manifesto não encontrado: {MANIFEST_PATH}")
        self.manifest_data = pd.read_parquet(MANIFEST_PATH)
        
        self.silver_data['doc_id'] = self.silver_data['doc_id'].astype(str)
        self.manifest_data['doc_id'] = self.manifest_data['doc_id'].astype(str)
        
        if self.manifest_data['doc_id'].duplicated().any():
            if 'downloaded_at' in self.manifest_data.columns:
                self.manifest_data['downloaded_at'] = pd.to_datetime(self.manifest_data['downloaded_at'], errors='coerce')
                self.manifest_data = self.manifest_data.sort_values(['doc_id', 'downloaded_at'], ascending=[True, False])
                self.manifest_data = self.manifest_data.drop_duplicates(subset=['doc_id'], keep='first')
            else:
                self.manifest_data = self.manifest_data.drop_duplicates(subset=['doc_id'], keep='last')

        cols_available = [c for c in ['doc_id', 'source_name', 'file_path', 'sha256'] if c in self.manifest_data.columns]
        if 'doc_id' not in cols_available: raise KeyError("Coluna 'doc_id' obrigatória no manifesto.")
        
        self.silver_data = self.silver_data.merge(self.manifest_data[cols_available], on='doc_id', how='left')
        if 'source_name' in self.silver_data.columns:
            self.silver_data['source_name'] = self.silver_data['source_name'].fillna('unknown_source')

    def extract_instruments(self):
        """Bloco 2: Extração de Instrumentos."""
        logger.info("Iniciando extração de instrumentos...")
        extracted_rows = []
        basic_markers = [r'\s', '[', '(', '|', '*', '+', '?', '{']

        for idx, row in self.silver_data.iterrows():
            text_original = str(row['full_text'])
            doc_id = row['doc_id']
            source_name = row.get('source_name', 'unknown')
            
            pn = pd.to_numeric(row.get('page_num'), errors='coerce')
            if pd.isna(pn): continue
            page_num = int(pn)
            
            for canonical, patterns in INSTRUMENT_TAXONOMY.items():
                for pattern in patterns:
                    try:
                        is_complex = (
                            ('\\' in pattern) or
                            (r'\b' in pattern) or 
                            ('(?' in pattern) or 
                            any(marker in pattern for marker in basic_markers)
                        )
                        regex_pattern = pattern if is_complex else r'\b' + pattern + r'\b'
                        
                        matches = list(re.finditer(regex_pattern, text_original, re.IGNORECASE))
                        for match in matches:
                            start, end = match.span()
                            ctx_start = max(0, start - 80)
                            ctx_end = min(len(text_original), end + 80)
                            evidence = text_original[ctx_start:ctx_end].replace('\n', ' ').strip()
                            
                            extracted_rows.append({
                                'doc_id': doc_id, 'source_name': source_name, 'page_num': page_num,
                                'entity_type': 'INSTRUMENTO', 'canonical_value': canonical,
                                'raw_value': match.group(), 'evidence_text': evidence,
                                'match_start_idx': start, 'confidence': 'HIGH'
                            })
                    except re.error: pass

        self.candidates = pd.DataFrame(extracted_rows)
        if not self.candidates.empty:
            self.candidates['evidence_len'] = self.candidates['evidence_text'].str.len()
            self.candidates = self.candidates.sort_values('evidence_len', ascending=False)
            self.candidates = self.candidates.drop_duplicates(subset=['doc_id', 'page_num', 'canonical_value'])
            self.candidates.drop(columns=['evidence_len'], inplace=True)
        logger.info(f"Instrumentos identificados: {len(self.candidates)}")

    def extract_salaries(self):
        """Bloco 3: Extração de Salários V5.0 (Correção Definitiva)."""
        logger.info("Iniciando extração de salários...")
        extracted_rows = []
        salary_pattern = re.compile(SALARY_CONFIG['regex'])
        
        for idx, row in self.silver_data.iterrows():
            text = str(row['full_text'])
            doc_id = row['doc_id']
            source_name = row.get('source_name', 'unknown')
            
            pn = pd.to_numeric(row.get('page_num'), errors='coerce')
            if pd.isna(pn): continue
            page_num = int(pn)

            matches = list(salary_pattern.finditer(text))
            for match in matches:
                symbol_found = bool(match.group(1)) 
                int_part_raw = match.group(2)
                dec_part_raw = match.group(3)
                if not int_part_raw: continue

                # Contexto e Normalização
                start, end = match.span()
                ctx_start = max(0, start - 120)
                ctx_end = min(len(text), end + 120)
                evidence = text[ctx_start:ctx_end].replace('\n', ' ').strip()
                context_norm = self._normalize_text(text[ctx_start:ctx_end])

                has_positive = self._has_keyword_simple(context_norm, SALARY_CONFIG['positive_keywords'])
                has_negative = self._has_keyword_simple(context_norm, SALARY_CONFIG['negative_keywords'])
                
                # Regra de Segurança Geral (Sem R$ exige formatação OU contexto positivo)
                if not symbol_found:
                    has_format = ('.' in int_part_raw) or (dec_part_raw is not None)
                    if (not has_format) and (not has_positive):
                        continue

                try:
                    int_clean = int_part_raw.replace('.', '')
                    dec_clean = dec_part_raw if dec_part_raw else "00"
                    val_float = float(f"{int_clean}.{dec_clean}")
                except ValueError: continue

                # Filtro de Faixa
                if not (SALARY_CONFIG['min_val'] <= val_float <= SALARY_CONFIG['max_val']): continue

                # --- CORREÇÃO DEFINITIVA 1: ANTI-ANO INTELIGENTE ---
                is_year_range = (1900 <= val_float <= 2100)
                is_raw_number = ('.' not in int_part_raw) and (dec_part_raw is None)

                # Só descarta se for ano (1900-2100), número cru, sem R$ E sem keyword positiva
                if is_year_range and is_raw_number and not symbol_found and not has_positive:
                    continue

                score = 0
                if symbol_found: score += 2
                if has_positive: score += 3
                if has_negative: score -= 10
                
                if not symbol_found and not has_positive: continue
                if score < 0: continue

                extracted_rows.append({
                    'doc_id': doc_id, 'source_name': source_name, 'page_num': page_num,
                    'entity_type': 'SALARIO', 'canonical_value': val_float,
                    'raw_value': match.group(0), 'evidence_text': evidence,
                    'match_start_idx': start, 'score': score
                })

        self.salaries = pd.DataFrame(extracted_rows)
        if not self.salaries.empty:
            self.salaries = self.salaries.sort_values('score', ascending=False)
            logger.info(f"Salários identificados: {len(self.salaries)}")
        else:
            logger.warning("Nenhum salário válido identificado.")

    def match_opportunities(self):
        """Bloco 4: Matchmaker Final."""
        logger.info("Iniciando Associação (Matchmaking)...")
        
        if self.candidates.empty:
            logger.warning("Sem instrumentos para associar.")
            return

        final_rows = []
        salaries_map = {}
        salary_audit = {} 

        if hasattr(self, 'salaries') and not self.salaries.empty:
            for _, row in self.salaries.iterrows():
                if pd.isna(row.get('doc_id')): continue
                pn = pd.to_numeric(row.get('page_num'), errors='coerce')
                if pd.isna(pn): continue
                page_num = int(pn)
                
                key = (str(row['doc_id']), page_num)
                if key not in salaries_map: salaries_map[key] = []
                salaries_map[key].append(row)
                
                audit_key = f"{row['source_name']} (Pág {page_num})"
                salary_audit[audit_key] = salary_audit.get(audit_key, 0) + 1

        logger.info("--- Diagnóstico de Salários (Top páginas ricas) ---")
        for k, v in sorted(salary_audit.items(), key=lambda x: x[1], reverse=True)[:5]:
            logger.info(f"  {k}: {v} salários encontrados")

        for idx, inst in self.candidates.iterrows():
            if pd.isna(inst.get('match_start_idx')): continue
            
            pn = pd.to_numeric(inst.get('page_num'), errors='coerce')
            if pd.isna(pn): continue
            page_num = int(pn)
            
            doc_id = str(inst['doc_id'])
            inst_pos = inst['match_start_idx']
            
            possible_salaries = salaries_map.get((doc_id, page_num), [])
            best_salary = None
            valid_candidates = []
            
            for sal in possible_salaries:
                if pd.isna(sal.get('match_start_idx')): continue
                dist = abs(inst_pos - sal['match_start_idx'])
                if dist < 3000: 
                    valid_candidates.append((sal, dist))
            
            if valid_candidates:
                valid_candidates.sort(key=lambda x: (-x[0]['score'], x[1]))
                best_salary = valid_candidates[0][0]
                min_dist = valid_candidates[0][1]
            else:
                min_dist = -1

            inst_evidence = str(inst.get('evidence_text', ''))
            sal_evidence = str(best_salary['evidence_text']) if best_salary is not None else ""
            combined_context = self._normalize_text(inst_evidence + " " + sal_evidence)
            
            detected_bond = "Não Informado"
            for bond_type, patterns in BOND_KEYWORDS.items():
                if any(re.search(pat, combined_context) for pat in patterns):
                    detected_bond = bond_type
                    break

            row = {
                'doc_id': doc_id,
                'source_name': inst.get('source_name', 'unknown'),
                'page_num': page_num,
                'instrumento': inst['canonical_value'],
                'contexto_instrumento': inst['evidence_text'],
                'tipo_vinculo': detected_bond,
            }
            
            if best_salary is not None:
                row['salario_valor'] = best_salary['canonical_value']
                row['salario_score'] = best_salary['score']
                row['distancia_chars'] = min_dist
                row['contexto_salario'] = best_salary['evidence_text']
                row['match_type'] = 'PAGE_PROXIMITY'
            else:
                row['salario_valor'] = None 
                row['salario_score'] = 0
                row['distancia_chars'] = -1
                row['contexto_salario'] = None
                row['match_type'] = 'NO_MATCH'

            final_rows.append(row)

        self.opportunities = pd.DataFrame(final_rows)
        
        def formatar_real(val):
            if pd.isna(val) or val is None: return "Não Informado"
            return f"R$ {val:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")

        self.opportunities['salario_formatado'] = self.opportunities['salario_valor'].apply(formatar_real)

        logger.info(f"Associação concluída. Total de Oportunidades: {len(self.opportunities)}")
        
        output_path = GOLD_OUTPUT_DIR / "opportunities.parquet"
        self.opportunities.to_parquet(output_path, index=False)
        csv_path = GOLD_OUTPUT_DIR / "opportunities.csv"
        self.opportunities.to_csv(csv_path, index=False)
        
        logger.info(f"🏆 DATASET GOLD GERADO: {output_path}")
        logger.info(f"📊 Versão CSV disponível: {csv_path}")

        print("\n--- 🎹 PREVIEW FINAL DO DATASET (TOP 10) 🎹 ---")
        cols_show = ['source_name', 'instrumento', 'salario_formatado', 'tipo_vinculo', 'match_type']
        cols_show = [c for c in cols_show if c in self.opportunities.columns]
        print(self.opportunities[cols_show].head(10))

    def save_all(self):
        """Salva os stagings."""
        if not self.candidates.empty:
            path = GOLD_OUTPUT_DIR / "staging_instruments.parquet"
            self.candidates.to_parquet(path, index=False)
            logger.info(f"Staging Instrumentos: {path}")
            
        if hasattr(self, 'salaries') and not self.salaries.empty:
            path_sal = GOLD_OUTPUT_DIR / "staging_salaries.parquet"
            self.salaries.to_parquet(path_sal, index=False)
            logger.info(f"Staging Salários: {path_sal}")

if __name__ == "__main__":
    try:
        processor = GoldProcessor()
        processor.load_data()
        
        processor.extract_instruments()
        processor.extract_salaries()
        processor.save_all()
        processor.match_opportunities()
    except Exception as e:
        logger.critical(f"Falha na execução do pipeline Gold: {e}")
        sys.exit(1)