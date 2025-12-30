# 🎻 Observatório de Mercado da Música Erudita no Brasil

> **Status:** MVP 1.0 (Funcional - Congelado)
> **Arquitetura:** ETL Python (Medallion Architecture)

## 🎯 A Visão (The Big Picture)

O mercado de trabalho para músicos de concerto (orquestras, bandas sinfônicas, coros) e professores de música no Brasil sofre de uma **assimetria de informação severa**.

Não existem plataformas unificadas (como LinkedIn ou Glassdoor) para este nicho. As oportunidades reais — concursos, testes seletivos e audições — são publicadas de forma dispersa em Diários Oficiais, sites institucionais efêmeros e **PDFs burocráticos**.

**O Objetivo deste projeto não é apenas "baixar arquivos", mas responder com dados estruturados:**
> *"Existe um campo de trabalho viável e sustentável para a profissionalização em música erudita hoje? Onde estão as vagas e quanto elas pagam?"*

---

## 🏗️ Arquitetura da Solução

Este projeto implementa um pipeline de Engenharia de Dados focado em **Documentos Não Estruturados (PDFs)**, transformando burocracia em inteligência de mercado.

A arquitetura segue o padrão **Medallion (Bronze ➡️ Silver ➡️ Gold)** para garantir rastreabilidade e qualidade dos dados.

### 1. Ingestão (Bronze Layer)
* **Responsabilidade:** Coleta e preservação.
* **Processo:** Download de editais a partir de links curados.
* **Segurança:** Implementação de Hashing (SHA256) para evitar duplicatas e garantir a integridade dos arquivos originais (Auditabilidade).
* **Output:** Arquivos PDF brutos e Manifesto de Ingestão (`manifest.parquet`).

### 2. Processamento (Silver Layer)
* **Responsabilidade:** Limpeza e Extração de Texto.
* **Processo:** Uso de `pdfplumber` para abrir arquivos PDF complexos e extrair o texto bruto, página por página.
* **Output:** Tabela intermediária (`parsed_pages.parquet`) contendo o conteúdo textual purificado.

### 3. Inteligência (Gold Layer)
* **Responsabilidade:** Regras de Negócio e Estruturação.
* **Processo:** Aplicação de **Regex (Expressões Regulares)** e lógica de NLP básica para identificar:
    * 💰 **Salários:** Padrões monetários (ex: "R$ 5.200,00").
    * 🎻 **Instrumentos:** Identificação de naipes (ex: "Fagote", "Violino", "Tuba").
    * 📍 **Vagas:** Quantidade de posições ofertadas.
* **Output Final:** Dataset Analítico (`opportunities.csv`).

---

## 🛠️ Tech Stack & Ferramentas

O projeto foi desenvolvido em ambiente Linux (WSL/Ubuntu), priorizando a reprodutibilidade.

* **Linguagem:** Python 3.12+
* **Core ETL:** Pandas, PyArrow (Parquet)
* **Parsing:** pdfplumber (Extração PDF)
* **Request:** Requests, Hashlib
* **Versionamento:** Git & GitHub

---

## 📂 Estrutura do Repositório

```text
├── src/
│   └── pipeline/          # Código Fonte do ETL
│       ├── collect_raw.py # Coletor (Bronze)
│       ├── process_silver.py # Parser (Silver)
│       └── process_gold.py # Extrator de Entidades (Gold)
├── data/
│   ├── raw/               # (Ignorado no Git) PDFs Originais
│   ├── intermediate/      # (Ignorado no Git) Texto Extraído
│   └── gold/              # Dados Finais (CSV disponível)
├── requirements.txt       # Dependências do Projeto
└── ESTRUTURA_PROJETO.txt  # Documentação detalhada dos arquivos