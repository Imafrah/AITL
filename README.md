<h1 align="center">
  AITL - Artificial Intelligence Translation Layer
</h1>

<p align="center">
  <strong>Transform unstructured and tabular data into high-fidelity structured intelligence. Powered by Hybrid AI, Deterministic Parsing, and Dynamic Cleaning.</strong>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python">
  <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?style=flat-square&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/Gemini_2.5_Flash-AI-8E75B2?style=flat-square&logo=google&logoColor=white" alt="Gemini">
  <img src="https://img.shields.io/badge/PostgreSQL-15+-4169E1?style=flat-square&logo=postgresql&logoColor=white" alt="PostgreSQL">
  <img src="https://img.shields.io/badge/SQLite-Memory-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite">
</p>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Features](#-features)
- [Architecture](#-architecture)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Getting Started](#-getting-started)
- [Environment Variables](#-environment-variables)
- [API Reference](#-api-reference)
- [Pipeline Deep Dive](#-pipeline-deep-dive)
- [Out of Scope](#-out-of-scope)

---

## 🚀 Overview

**AITL (AI Data Translation Layer)** is a next-generation data intelligence platform that converts messy, unstructured documents and tabular data into production-ready structured intelligence. 

By combining **Gemini 2.5 Flash** with a deterministic **Dynamic Cleaning Engine**, AITL ensures that your raw data is not just localized, but cleaned, validated, and normalized for downstream consumption. 

Upload a `.txt` invoice, a `.csv` dump, or a `.pdf` file, and AITL will:

1. **Classify & Route**: Automatically detect if the file is an unstructured document or a tabular dataset.
2. **Extract**: Use deterministic parsers for CSV and AI Large Language Models for complex text extraction.
3. **Clean & Validate**: Dynamically compute statistics and clean the data.
4. **Export**: Provide normalized multi-format exports.

---

## ✨ Features

- **Universal Parsing Engine:** Modular parsers for `.csv`, `.txt`, and `.pdf` files.
- **Hybrid AI Extraction:** Uses the lightning-fast `Gemini 2.5 Flash` to extract semantic entities (amounts, dates, names) from unstructured inputs.
- **Schema Memory:** Backed by SQLite to remember and map recurring CSV columns, saving tokens and mapping structures.
- **Dynamic Cleaning & Anomaly Detection:** Applies automated zero-null policies, type-repairs, and identifies outliers.
- **Persistent Storage:** Fully integrated with `PostgreSQL` via `SQLAlchemy`.

---

## 🏗️ Architecture

The pipeline follows a robust hybrid flow optimized for data integrity:

```mermaid
graph TD
    User([User/Client]) -->|Upload File| API[FastAPI Entry Point]
    
    subgraph "Universal Intelligence Pipeline"
        API --> Classification{File Router}
        Classification -->|Unstructured| AI[Gemini AI Extract]
        Classification -->|Tabular| CSV[Robust CSV Parser]
        
        AI --> Normalization[Schema Normalization]
        CSV --> Memory[(SQLite Schema Memory)]
        Memory <--> CSV
        
        Normalization --> Cleaning[Cleaning & Validation]
        CSV --> Cleaning
        
        Cleaning --> Anomaly[Anomaly Detection]
        Anomaly --> Final[Final Output Assembly]
    end
    
    Final --> DB[(PostgreSQL)]
```

### Flow Breakdown:
1. **Parse:** Convert raw file bytes to clean text or tabular datasets.
2. **Extract:** Send localized unstructured text to Gemini AI or run `csv.DictReader`.
3. **Post-Process:** Assign IDs, normalize labels, clean strings, flag outliers.
4. **Persist:** Save structured output to PostgreSQL logic records.

---

## 💻 Tech Stack

- **Backend:** `Python 3.11`, `FastAPI`
- **AI Processing:** `Google GenAI API` (Gemini 2.5 Flash)
- **Database:** `PostgreSQL` (Record Storage), `SQLite` (Schema Memory), `SQLAlchemy`
- **Logic & Cleaning:** `pandas`, `rapidfuzz`
- **Extractors:** `pdfplumber`, `csv`

---

## 📂 Project Structure

```text
AITL/
├── main.py                  # FastAPI Application Entry Point
├── api/
│   └── routes.py            # API controller and HTTP routing
├── core/                    # Core Business Logic & Pipelines
│   ├── universal_pipeline.py# Main Pipeline orchestration
│   ├── final_cleaning.py    # Zero-null policies and final repairs
│   └── anomaly_detector.py  # Statistical outliers detection
├── ai_layer/                # Interfacing with Google Gemini
│   ├── extractor.py         
│   └── schema_detector.py   
├── parsers/                 # Specialized internal parsers
│   ├── csv_parser.py        
│   ├── txt_parser.py        
│   └── pdf_parser.py        
├── db/                      # Database Initialization & CRUD
│   ├── database.py          
│   └── crud.py              
├── .env                     # Environment variables details
├── Dockerfile               # Containerization definition
└── requirements.txt         # Project runtime dependencies
```

---

## 🏁 Getting Started

### Prerequisites
- Python 3.11+
- PostgreSQL Server Details
- Google Gemini API Key

### 1. Clone & Install
```bash
git clone https://github.com/your-username/aitl.git
cd AITL
python -m venv .venv
# Activate venv depending on OS
source .venv/bin/activate  # Linux/macOS
.venv\Scripts\activate     # Windows

pip install -r requirements.txt
```

### 2. Configure Environment

Create a `.env` file in the root based on the required settings (see **Environment Variables** below).

### 3. Run Locally (Without Docker)
```bash
uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will now be available on: `http://localhost:8000`

Interactive Documentation is at: `http://localhost:8000/docs`

### 4. 🐳 Docker Deployment

AITL provides a fully containerized architecture using Docker and Docker Compose, covering both the FastAPI backend and the Vite frontend.

**Build and Run**
Execute the following from the root directory:
```bash
docker-compose up --build -d
```

**Accessing Services:**
- **Frontend Dashboard**: `http://localhost:80`
- **Backend API**: `http://localhost:8000`
- **Interactive API Docs**: `http://localhost:8000/docs`

Volumes strictly mount `sample_data/`, `output/`, and the `schema_memory.db` for state persistence and seamless file processing.

---

## 🔒 Environment Variables

Fill this `.env` file appropriately in the root of the project:

```env
# Google GenAI Settings
GEMINI_API_KEY=your_gemini_api_key_here

# PostgreSQL Details
DATABASE_URL=postgresql://user:password@localhost:5432/aitldb

# Application Setups
DEBUG_MODE=True
ALLOWED_ORIGINS=http://localhost:5173,https://aitl.vercel.app
```

---

## 🌐 API Reference

### Health Check
**`GET /health`**
Used for pinging the service functionality.

### Upload and Parse
**`POST /api/v1/upload`**
Upload any accepted file format for parsing. 

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `file`    | File | ✅      | Uploaded file (.csv, .pdf, .txt) |

**Returns:**
JSON object containing pipeline output (cleansed structures, entities, anomaly flags).

---

## 🔍 Pipeline Deep Dive

### 1. Robust CSV Reading 
Instead of delegating massive files completely over AI API tokens, the application strictly uses local `pandas` or python `csv` dictionaries paired with an SQLite cognitive memory layer to maintain schema consistencies without large LLM latencies.

### 2. Universal Data Standardizer & Strict Cleaning
Using `rapidfuzz` and python typing guards, data columns indicating `prices`, `dates`, or `booleans` are correctly cast dynamically in runtime, catching dirty artifacts along process pathways before pushing to Db.

### 3. 🧹 Cleaning Dirty Datasets: Overview & Mechanisms
AITL handles aggressive noise and varied errors within datasets. Our Dynamic Cleaning Engine operates on a **strict "zero-imputation" policy**, prioritizing data integrity mathematically and logically.

**Types of Dirty Data We Clean:**
- **Missing & Anomalous Values:** Inconsistent missing values such as `NaN`, `N/A`, `---`, `null`, `None`, or whitespace blocks.
- **Type Inconsistencies:** Strings nested in numeric schemas (`$1200.50`, `1.2k`), localized boolean structures (`yes/no`, `1/0`, `T/F`), and disjointed date signatures.
- **Garbage & Intermediaries:** AI-generated artifacts, raw JSON leftover bits, and schema deduction tokens within structural columns.

**The Basis of Our Cleaning (How it Works):**
1. **Dynamic Type Coercion:** Values are aggressively type-cast utilizing a mixture of regular expressions and contextual AI evaluation to resolve strings into actionable floats, integers, or booleans.
2. **Zero-Imputation (No Nulls Policy):** The pipeline explicitly **does not guess**. If a field falls short of strict validation bounds or ends up devoid of substance, we do not impute a fallback. Rather, the entire record is safely **dropped** from the schema to assure 100% downstream validity.
3. **Outlier Filtering & Metadata Stripping:** Extraneous validation columns (`_anomaly_score`, flags) generated mid-pipeline are pruned, and values behaving outside threshold standard limits are filtered out.

---

## ❌ Out of Scope

The current implementation limits itself purposely across certain aspects to guarantee quality via restricted scope:
- Deep layout OCR parsing for image-heavy documents.
- `.docx` or email (`.eml`, `.msg`) parsing formats.
- Batch multiple asynchronous file upload flows over native APIs.
