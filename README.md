<p align="center">
  <h1 align="center">🧠 AITL — Universal Data Intelligence Layer</h1>
  <p align="center">
    <strong>Transform unstructured and tabular data into high-fidelity structured intelligence — Powered by Hybrid AI/Rules + Dynamic Cleaning</strong>
  </p>
  <p align="center">
    <a href="https://aitl.vercel.app">Live Demo</a> · 
    <a href="#-api-reference">API Docs</a> · 
    <a href="#-getting-started">Quick Start</a>
  </p>
  <p align="center">
    <img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white" alt="Python">
    <img src="https://img.shields.io/badge/FastAPI-0.100+-009688?style=flat-square&logo=fastapi&logoColor=white" alt="FastAPI">
    <img src="https://img.shields.io/badge/React-19-61DAFB?style=flat-square&logo=react&logoColor=black" alt="React">
    <img src="https://img.shields.io/badge/Gemini_2.5_Flash-AI-8E75B2?style=flat-square&logo=google&logoColor=white" alt="Gemini">
    <img src="https://img.shields.io/badge/PostgreSQL-15+-4169E1?style=flat-square&logo=postgresql&logoColor=white" alt="PostgreSQL">
    <img src="https://img.shields.io/badge/SQLite-Memory-003B57?style=flat-square&logo=sqlite&logoColor=white" alt="SQLite">
  </p>
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
- [Frontend](#-frontend)
- [Database Schema](#-database-schema)
- [Deployment](#-deployment)
- [Sample Data](#-sample-data)
- [Error Handling](#-error-handling)
- [Contributing](#-contributing)
- [License](#-license)

---

## 🌟 Overview

**AITL (AI Data Translation Layer)** is a next-generation data intelligence platform that converts messy, unstructured documents and tabuar data into production-ready structured intelligence. 

By combining **Gemini 2.5 Flash** with a deterministic **Dynamic Cleaning Engine**, AITL ensures that your raw data is not just "translated," but cleaned, validated, and normalized for downstream consumption.

Upload a `.txt` invoice, a billion-row `.csv` (truncated), or a complex `.pdf`, and AITL will:

1.  **Classify & Route**: Automatically detect if the file is an unstructured document or a tabular dataset.
2.  **Intelligent Extraction**: Use hybrid AI techniques to extract semantic entities (Names, Dates, Locations, Amounts).
3.  **Schema Memory**: Recognize recurring CSV schemas using SQLite-backed "memory" for consistent field mapping.
4.  **Anomaly Detection**: Identify statistical outliers and data quality issues during the transformation.
5.  **Final Cleaning Layer**: Enforce zero-null policies, perform type-repair, and unify schemas into a universal intelligence record.
6.  **Multi-Format Export**: Persist results to PostgreSQL and provide on-demand JSON, CSV, Table, and TOML exports.

---

## ✨ Features

| Feature | Description |
|---|---|
| 📄 **Universal Parsing** | Supports `.txt`, `.csv`, and `.pdf` with robust, auto-detecting parsers |
| 🤖 **Hybrid Extraction** | Combines Large Language Models (Gemini) with deterministic heuristics for maximum accuracy |
| 🧠 **Schema Memory** | SQLite-backed cognitive layer that "remembers" previous CSV column mappings |
| 🧼 **Dynamic Cleaning** | Automated type repair, date normalization, and "no-null" imputation policies |
| 🚨 **Anomaly Detection** | Real-time statistical identification of outliers and suspicious data points |
| 🎯 **Adaptive Confidence** | Confidence scores dynamically adjusted based on data quality and normalization state |
| 🗄️ **PostgreSQL Store** | Results are automatically persisted for long-term auditability |
| 🗳️ **Multi-Format Export** | On-demand export to **JSON**, **TOML**, **CSV**, and **Interactive Dashboard** |
| 🖥️ **React Intelligence Lab** | Beautiful dark-themed dashboard to visualize the data intelligence pipeline |
| 🐳 **Docker Ready** | One-command depl## 🏗 Architecture

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
        Anomaly --> Confidence[Adaptive Confidence]
        Confidence --> FinalClearing[Final Cleaning Layer]
    end
    
    FinalClearing --> DB[(PostgreSQL)]
    FinalClearing --> Export{Export Formats}
    
    Export --> JSON[JSON/TOML]
    Export --> DASH[React Dashboard]
    Export --> CSVO[Clean CSV]
```

The pipeline follows a **7-stage hybrid flow** optimized for data integrity:

1.  **Classify** → Route to AI extractor (TXT/PDF) or Robust Parser (CSV).
2.  **Extract** → Gemini 2.5 Flash for entities or `csv.DictReader` for tabular.
3.  **Remember** → (CSV only) Load/Save column mappings from SQLite memory.
4.  **Clean** → Standardize dates, emails, phones, and currency.
5.  **Detect** → Identify statistical anomalies and outliers.
6.  **Score** → Compute adaptive confidence based on data quality.
7.  **Unify** → Enforce zero-null policies and repair data types.
�     ▼          ▼            ▼            ▼                   │
│  ┌──────┐ ┌────────┐ ┌──────────┐ ┌──────────┐              │
│  │Parse │ │  AI    │ │  Post    │ │ Database │              │
│  │      │ │Extract │ │ Process  │ │  Save    │              │
│  │ TXT  │ │        │ │          │ │          │              │
│  │ CSV  │ │Gemini  │ │ Entity   │ │PostgreSQL│              │
│  │ PDF  │ │2.5     │ │ IDs +    │ │  via     │              │
│  │      │ │Flash   │ │ Normalize│ │SQLAlchemy│              │
│  └──────┘ └────────┘ └──────────┘ └──────────┘              │
└─────────────────────────────────────────────────────────────┘
```

The pipeline follows a **4-step sequential flow** with error handling at each stage:

1. **Parse** → Convert raw file bytes to clean text + metadata
2. **Extract** → Send text to Gemini AI for entity extraction
3. **Post-Process** → Assign IDs, normalize labels, compute confidence
4. **Persist** → Save structured output to PostgreSQL

---

## 🛠 Tech Stack

### Backend
| Technology | Purpose |
|---|---|
| **Python 3.11** | Core runtime |
| **FastAPI** | High-performance async API framework |
| **Google Gemini 2.5 Flash** | AI model for entity extraction |
| **SQLAlchemy** | ORM for database operations |
| **PostgreSQL** | Dynamic result storage |
| **SQLite (Memory)** | Persistent schema mapping retention |
| **pdfplumber** | PDF text extraction |
| **pandas / csv** | Robust tabular parsing |
| **rapidfuzz** | Fuzzy string matching for cleaning |

### Frontend
| Technology | Purpose |
|---|---|
| **React 19** | UI framework |
| **Vite 8** | Build tool and dev server |
| **Axios** | HTTP client for API calls |

### DevOps
| Technology | Purpose |
|---|---|
| **Docker** | Containerization |
| **Render** | Backend hosting |
| **Vercel** | Frontend hosting |

---
 
## ❌ Out of Scope (v1)
 
These are deliberately excluded from v1:
 
- Scanned / image-based PDFs (requires OCR)
- Word documents (.docx)
- Email files (.eml, .msg)
- Multi-language support (English only)
- Authentication / API keys
- Batch file uploads
- Vector / semantic search
 
---

## 📁 Project Structure

```
AITL/
├── main.py                    # FastAPI app entry point
├── orchestrator.py            # Legacy orchestrator (maintained for compatibility)
├── logger.py                  # Centralized logging configuration
├── requirements.txt           # Python dependencies
├── Dockerfile                 # Docker containerization
├── schema_memory.db           # SQLite database for Schema Memory
│
├── core/                      # Pipeline Intelligence Layer
│   ├── universal_pipeline.py  # Main hybrid pipeline entry
│   ├── cleaning.py            # Basic data cleaning rules
│   ├── final_cleaning.py      # Type repair & zero-null enforcement
│   ├── schema_memory.py       # SQLite CRUD for CSV mappings
│   ├── anomaly_detector.py    # Statistical outlier detection
│   ├── analytics_engine.py    # Meta-analysis of extracted data
│   ├── semantic_mapping.py    # Grouping raw columns into intelligence roles
│   └── output_formatter.py    # Converters for Export formats
│
├── api/                       # API Layer
│   └── routes.py              # GET /translate, POST /export/toml
│
├── parsers/                   # File Parsing Layer
│   ├── router.py              # Routes files to correct parser
│   ├── csv_robust.py          # High-performance CSV parser
│   ├── pdf_parser.py          # PDF parser (via pdfplumber)
│   └── txt_parser.py          # Plain text parser
│
├── ai_layer/                  # AI Extraction Layer
│   ├── extractor.py           # Gemini 2.5 Flash integration
│   └── schema_detector.py     # AI-based column role detection
│
├── post_processor/            # Post-Processing Layer
│   └── processor.py           # Legacy entity IDs & ID normalization
│
├── db/                        # PostgreSQL Layer
│   ├── database.py            # SQLAlchemy engine & model
│   └── crud.py                # Persistence logic
│
└── frontend/                  # React Intelligence Dashboard
    ├── App.jsx                # Multi-view dashboard (JSON/Table/Charts)
    └── index.css              # Custom design system
```

---

## 🚀 Getting Started

### Prerequisites

- **Python** 3.11+
- **Node.js** 18+
- **PostgreSQL** 15+ (local or hosted)
- **Google Gemini API Key** — [Get one here](https://aistudio.google.com/app/apikey)

### 1. Clone the Repository

```bash
git clone https://github.com/Imafrah/AITL.git
cd AITL
```

### 2. Backend Setup

```bash
# Create and activate virtual environment
python -m venv venv

# Windows
venv\Scripts\activate

# macOS/Linux
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt
```

### 3. Configure Environment Variables

Create a `.env` file in the project root:

```env
GEMINI_API_KEY=your_gemini_api_key_here
DATABASE_URL=postgresql://aitl_user:aitl_pass@localhost:5432/aitl_db
```

### 4. Set Up PostgreSQL

```bash
# Create the database (PostgreSQL CLI)
createdb aitl_db

# Or with psql
psql -U postgres -c "CREATE DATABASE aitl_db;"
psql -U postgres -c "CREATE USER aitl_user WITH PASSWORD 'aitl_pass';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE aitl_db TO aitl_user;"
```

> **Note:** The database tables are automatically created on server startup via `init_db()`.

### 5. Start the Backend

```bash
uvicorn main:app --reload --port 8000
```

The API will be available at `http://localhost:8000`.

### 6. Frontend Setup

```bash
cd frontend
npm install
npm run dev
```

The frontend will be available at `http://localhost:5173`.

### 7. Using Docker (Alternative)

```bash
# Build the image
docker build -t aitl .

# Run the container
docker run -p 8000:8000 \
  -e GEMINI_API_KEY=your_key_here \
  -e DATABASE_URL=postgresql://user:pass@host:5432/aitl_db \
  aitl
```

---

## 🔐 Environment Variables

| Variable | Required | Description |
|---|---|---|
| `GEMINI_API_KEY` | ✅ | Google Gemini API key for AI extraction |
| `DATABASE_URL` | ✅ | PostgreSQL connection string |

> **Format:** `postgresql://username:password@host:port/database_name`
>
> **Note:** The app automatically converts `postgres://` URLs (used by Render) to `postgresql://` for SQLAlchemy compatibility.

---

## 📡 API Reference

### Health Check

```http
GET /health
```

**Response:**
```json
{
  "status": "ok",
  "service": "AITL"
}
```

---

### Translate Document

```http
POST /translate
Content-Type: multipart/form-data
```

**Parameters:**

| Field | Type | Description |
|---|---|---|
| `file` | `File` | The document to translate (`.txt`, `.csv`, or `.pdf`) |

**Constraints:**
- Max file size: **10 MB**
- Supported types: `txt`, `csv`, `pdf`
- File must not be empty

**Example (cURL):**
```bash
curl -X POST "http://localhost:8000/translate" \
  -F "file=@sample_data/invoice_001.txt"
```

**Success Response (200):**
```json
{
  "document_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "document_type": "invoice",
  "source_file": "invoice_001.txt",
  "status": "success",
  "error": null,
  "entities": {
    "person_names": [
      { "id": "p1", "value": "John Doe", "confidence": 1.0 }
    ],
    "organizations": [
      { "id": "o1", "value": "ABC Corp", "confidence": 1.0 }
    ],
    "dates": [
      { "id": "d1", "value": "2024-01-12", "confidence": 1.0 },
      { "id": "d2", "value": "2024-01-30", "confidence": 1.0 }
    ],
    "amounts": [
      { "id": "a1", "value": 5000, "currency": "INR", "label": "invoice_total", "confidence": 1.0 }
    ]
  },
  "relationships": [
    {
      "type": "payment",
      "from": "p1",
      "to": "o1",
      "confidence": 1.0,
      "attributes": {}
    }
  ],
  "metadata": {
    "file_type": "txt",
    "page_count": null,
    "word_count": 24,
    "confidence_overall": 1.0,
    "processed_at": "2024-01-12T10:30:00+00:00"
  }
}
```

**Error Responses:**

| Status | Reason |
|---|---|
| `422` | Unsupported file type, file too large, or empty file |
| `500` | Internal server error |

---

### Get Result by ID

```http
GET /results/{document_id}
```

**Example:**
```bash
curl "http://localhost:8000/results/a1b2c3d4-e5f6-7890-abcd-ef1234567890"
```

**Success Response (200):**
```json
{
  "document_id": "a1b2c3d4-e5f6-7890-abcd-ef1234567890",
  "source_file": "invoice_001.txt",
  "document_type": "invoice",
  "status": "success",
  "structured_output": { ... },
  "created_at": "2024-01-12T10:30:00"
}
```

**Error Responses:**

| Status | Reason |
|---|---|
| `404` | Document not found |
| `500` | Database error |

---

## 🔬 Pipeline Deep Dive

### Step 1: Parsing (`parsers/`)

The parser router selects the correct parser based on file extension:

| Parser | File Type | Library | Features |
|---|---|---|---|
| `txt_parser.py` | `.txt` | Built-in | UTF-8 decoding, word count |
| `csv_parser.py` | `.csv` | pandas | UTF-8/Latin-1 fallback, row/column counts, DataFrame to text |
| `pdf_parser.py` | `.pdf` | pdfplumber | Multi-page extraction, page count |

**Output format:**
```json
{
  "text": "extracted plain text content...",
  "metadata": {
    "file_type": "txt|csv|pdf",
    "page_count": null,
    "word_count": 24,
    "row_count": 3,
    "columns": ["vendor", "amount", "currency"]
  }
}
```

### Step 2: AI Extraction (`ai_layer/`)

The extracted text is sent to **Google Gemini 2.5 Flash** with a structured prompt that:

- Defines the expected JSON schema
- Establishes confidence scoring rules:
  - **1.0** — Explicitly stated in the document
  - **0.85–0.95** — Clearly implied but not exact
  - **0.70–0.84** — Inferred from context
  - **Below 0.70** — Uncertain or ambiguous
- Enforces date format (`YYYY-MM-DD`) and numeric amounts
- Requests entity extraction for: persons, organizations, dates, amounts
- Detects relationships between entities

**Temperature** is set to `0.2` for deterministic, consistent output.

### Step 3: Post-Processing (`post_processor/`)

The raw AI output is enriched with:

1. **Entity ID Assignment** — Each entity gets a short ID:
   - `p1`, `p2` for person names
   - `o1`, `o2` for organizations
   - `d1`, `d2` for dates
   - `a1`, `a2` for amounts

2. **Label Normalization** — Maps AI-generated labels to standard terms:
   ```
   amount → invoice_total
   total  → invoice_total
   price  → invoice_total
   fee    → invoice_total
   ```

3. **Relationship Processing** — Replaces raw string references with entity IDs

4. **Overall Confidence** — Computes average confidence across all entities

5. **Document ID** — Assigns a UUID for database storage

### Step 4: Database Persistence (`db/`)

The final structured output is saved to PostgreSQL using SQLAlchemy ORM.

---

## 🖥 Frontend

The React frontend provides a clean, dark-themed dashboard for:

- **File Selection** — Drag-and-drop style upload for `.txt`, `.csv`, `.pdf`
- **Document Translation** — One-click processing via the `/translate` endpoint
- **Result Visualization:**
  - Status badge (SUCCESS / PARTIAL / FAILED)
  - Entity cards grouped by type with confidence bars
  - Relationship flow visualization
  - Metadata table
  - Raw JSON output

### Running Locally

```bash
cd frontend
npm install
npm run dev
```

The dev server starts at `http://localhost:5173` with API proxy configured to `http://127.0.0.1:8000`.

### Building for Production

```bash
cd frontend
npm run build
```

Output is generated in `frontend/dist/`.

---

## 🗄 Database Schema

### `documents` Table

| Column | Type | Description |
|---|---|---|
| `document_id` | `String` (PK) | UUID assigned during post-processing |
| `source_file` | `String` | Original filename |
| `document_type` | `String` | Detected type (e.g., `invoice`) |
| `status` | `String` | Pipeline status: `success`, `partial`, `failed` |
| `raw_text` | `Text` | Extracted plain text from the parser |
| `structured_output` | `JSON` | Full structured JSON result |
| `created_at` | `DateTime` | UTC timestamp of creation |

---

## ☁️ Deployment

### Backend — Render

The backend is deployed on [Render](https://render.com) at `https://aitl.onrender.com`.

**Configuration:**
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `uvicorn main:app --host 0.0.0.0 --port 8000`
- **Environment Variables:** Set `GEMINI_API_KEY` and `DATABASE_URL` in Render dashboard

> **Note:** Render provides PostgreSQL as an add-on. The app auto-converts `postgres://` to `postgresql://` for compatibility.

### Frontend — Vercel

The frontend is deployed on [Vercel](https://vercel.com) at `https://aitl.vercel.app`.

**Configuration:**
- **Framework Preset:** Vite
- **Root Directory:** `frontend`
- **Build Command:** `npm run build`
- **Output Directory:** `dist`

### Docker

```bash
docker build -t aitl .
docker run -p 8000:8000 \
  -e GEMINI_API_KEY=your_key \
  -e DATABASE_URL=postgresql://user:pass@host:5432/db \
  aitl
```

---

## 📂 Sample Data

The `sample_data/` directory contains test files:

| File | Description | Purpose |
|---|---|---|
| `invoice_001.txt` | Clean, well-structured invoice | Tests high-confidence extraction |
| `invoice_002.txt` | Ambiguous, informal invoice | Tests low-confidence handling |
| `sales_001.csv` | Multi-row sales spreadsheet | Tests CSV parsing + multiple entity extraction |
| `empty.txt` | Empty file | Tests error handling for empty input |

### Example: invoice_001.txt

```
INVOICE #1001
Date: 12 January 2024
From: John Doe
To: ABC Corp
Amount: INR 5,000
Description: Web development services for Q4 2023
Payment Due: 30 January 2024
```

---

## ⚠️ Error Handling

AITL uses a **graceful degradation** strategy — if any pipeline step fails, the system returns whatever data was successfully extracted rather than failing entirely.

| Pipeline Step | On Failure | Status Returned |
|---|---|---|
| **Parsing** | Returns empty entities + error message | `failed` |
| **AI Extraction** | Returns parsed metadata + error | `partial` |
| **Post-Processing** | Returns raw AI entities + error | `partial` |
| **Database Save** | Returns full result + DB error flag | `partial` |

### Custom Exception Classes

| Exception | Module | Description |
|---|---|---|
| `ParseError` | `parsers/txt_parser.py` | File parsing failures |
| `AIServiceError` | `ai_layer/extractor.py` | Gemini API or JSON parsing failures |
| `ValidationError` | `post_processor/processor.py` | Post-processing failures |
| `DBError` | `db/crud.py` | Database operation failures |

---

## 🤝 Contributing

1. **Fork** the repository
2. **Create** a feature branch: `git checkout -b feature/my-feature`
3. **Commit** your changes: `git commit -m "Add my feature"`
4. **Push** to the branch: `git push origin feature/my-feature`
5. **Open** a Pull Request

### Development Notes

- Backend runs on `http://localhost:8000`
- Frontend runs on `http://localhost:5173` with proxy to backend
- The Vite dev server proxies `/translate` and `/results` to the backend
- CORS is configured for both `localhost:5173` (dev) and `aitl.vercel.app` (production)
- Database tables are auto-created on startup

---

## 📄 License

This project is open source and available under the [MIT License](LICENSE).

---

<p align="center">
  Built with ❤️ by <a href="https://github.com/Imafrah">Imafrah</a>
</p>
