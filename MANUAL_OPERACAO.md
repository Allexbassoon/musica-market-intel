# 🕹️ MANUAL DE OPERAÇÃO: Pipeline de Inteligência Musical
**Versão:** 1.1 (Revisada e Blindada)
**Status:** Operacional

Este documento descreve o ritual semanal para atualizar a base de dados de vagas, garantindo que o sistema capture novas oportunidades e processe as informações corretamente através das camadas Bronze, Silver e Gold.

---

## 1. Preparação (Input)
*Antes de voar, abastecemos o sistema com novos alvos.*

* [ ] **Descobrir novos editais:** Navegue pelos sites das orquestras ou receba dicas da comunidade.
* [ ] **Copiar o Link:** Clique com o botão direito no PDF -> "Copiar endereço do link".
* [ ] **Atualizar as Sementes (Seeds):**
    1.  No VS Code, abra `src/pipeline/collect_raw.py`.
    2.  Localize a lista `SEEDS` (perto do topo).
    3.  Adicione o novo bloco (mantendo os antigos para histórico):
        ```python
        {
            "source_name": "Nome_da_Orquestra",
            "source_url": "Site_onde_achou",
            "download_url": "Link_do_PDF_aqui",
        },
        ```
    * *Nota Técnica:* O sistema já possui um *delay* de segurança (0.8s) entre downloads. Não remova isso para evitar bloqueios (Erro 429).

---

## 2. Execução (Voo Completo)
*Rodamos as três camadas do pipeline em sequência para transformar PDF bruto em Dados.*

* [ ] **Abrir Terminal:** Garanta que está na pasta do projeto (`musica_market_intel`).
* [ ] **Ativar Ambiente:**
    ```bash
    source .venv/bin/activate
    ```
* [ ] **Rodar Ingestão (Bronze):** Baixa os arquivos da internet.
    ```bash
    python src/pipeline/collect_raw.py
    ```
* [ ] **Rodar Processamento (Silver):** Lê o texto de dentro dos PDFs.
    ```bash
    python src/pipeline/process_silver.py
    ```
* [ ] **Rodar Inteligência (Gold):** Extrai salários, instrumentos e gera o CSV final.
    ```bash
    python src/pipeline/process_gold.py
    ```

---

## 3. Conferência (Pós-Voo)
*Validamos se o pouso foi seguro olhando os artefatos gerados.*

* [ ] **Check de Arquivos (A Prova Real):**
    * Abra a aba de arquivos do VS Code.
    * Verifique se o arquivo `data/gold/opportunities.csv` teve seu horário (timestamp) atualizado para **agora**.
    * Verifique se `data/intermediate/parsed_pages.parquet` também foi atualizado.
* [ ] **Check de Erros:**
    * Olhe o terminal. Se houver mensagens de `Traceback` ou `Error`, anote para investigar. Mensagens de `SKIP` são normais e esperadas.

---

## 4. Gestão de Mudanças e Erratas (Procedimento Especial ⚠️)
*Procedimentos para quando o edital muda (conteúdo), mas o link continua o mesmo.*

**O Cenário:** A orquestra lançou uma "Errata" e substituiu o arquivo PDF no site, mas manteve a mesma URL.
**O Risco:** O sistema pode achar que já tem esse arquivo e pular o download.

**Como Forçar a Atualização (Ciclo Completo):**

1.  **Configurar:** Abra `src/pipeline/collect_raw.py` e mude:
    * De: `FORCE_REDOWNLOAD = False`
    * Para: `FORCE_REDOWNLOAD = True`
2.  **Re-executar TUDO:** Para que a errata chegue no CSV final, você precisa rodar a fila toda novamente:
    ```bash
    python src/pipeline/collect_raw.py
    python src/pipeline/process_silver.py
    python src/pipeline/process_gold.py
    ```
3.  **Desligar:** Volte a variável para `False` imediatamente após o uso, para evitar downloads desnecessários na próxima semana.

---

**Fim do Procedimento.** 