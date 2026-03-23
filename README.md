# 🧠 GenAI Document Summarizer

An **Agentic AI-powered hierarchical document summarization system** that processes large documents and generates **structured multi-level summaries** using **LLMs, FastAPI, and AWS Bedrock**.

---

## 🚀 Project Overview

GenAI Document Summarizer is an end-to-end intelligent backend system designed to summarize large PDF/TXT documents using a **hierarchical Map-Reduce summarization architecture**.

Instead of generating a single flat summary, the system performs:

✅ Intelligent document ingestion
✅ Recursive token-aware chunking
✅ Parallel LLM summarization
✅ Section-level synthesis
✅ Executive-level strategic summary
✅ Responsible AI schema validation

This architecture enables scalable summarization of long enterprise documents such as:

* Research Papers
* Technical Reports
* Business Documents
* Policy Documents
* Whitepapers

---

## 🏗️ System Architecture

```
Document Upload
       ↓
Document Ingestion
       ↓
Recursive Chunking
       ↓
Parallel Chunk Summarization (LLM)
       ↓
Section-Level Aggregation
       ↓
Executive Summary Generation
       ↓
Schema Validation (Responsible AI)
       ↓
Structured JSON Output
```

---

## ⚙️ Tech Stack

### Backend

* **FastAPI** — High-performance API framework
* **Python 3.11**
* **Pydantic** — Schema validation
* **LangChain Text Splitters**
* **ThreadPoolExecutor** — Parallel processing

### AI / LLM

* **AWS Bedrock**
* **Meta Llama 3 (11B Instruct)**
* Hierarchical Map-Reduce Summarization

### Document Processing

* **PyMuPDF (fitz)** — PDF extraction
* Intelligent text cleaning pipeline

### Infrastructure

* Environment-based configuration
* Structured logging system
* Rotating log files
* Async + Threadpool execution

---

## 🧩 Key Features

### ✅ Hierarchical Summarization

Multi-stage summarization improves factual grounding and scalability.

* Chunk summaries
* Section summaries
* Executive summary

---

### ✅ Parallel LLM Execution

Chunks are processed concurrently to reduce latency.

```
Workers → Parallel Bedrock Calls
```

---

### ✅ Responsible AI Layer

Output validation using strict schemas:

* Coverage tracking
* Missing section detection
* Structured JSON enforcement
* Failure-safe execution

---

### ✅ Production-Level Logging

* Pipeline lifecycle tracking
* Model latency monitoring
* Error tracing
* Rotating log storage

Logs stored in:

```
Backend/logs/
 ├── app.log
 └── error.log
```

---

## 📂 Project Structure

```
GenAI-Document-Summarizer
│
├── Backend
│   ├── app
│   │   ├── api
│   │   ├── services
│   │   │   ├── ingestion.py
│   │   │   ├── chunking.py
│   │   │   ├── summarizer.py
│   │   │   └── pipeline.py
│   │   ├── models
│   │   ├── prompts
│   │   └── core
│   │
│   ├── logs
│   ├── source
│   ├── main.py
│   └── requirements.txt
│
├── Frontend
└── README.md
```

---

## 🔄 Pipeline Flow

### 1️⃣ Document Ingestion

* Auto-detects PDF/TXT
* Cleans noisy text
* Removes formatting artifacts

### 2️⃣ Intelligent Chunking

Uses **RecursiveCharacterTextSplitter**:

* Token-aware splitting
* Context preservation
* Overlap handling

### 3️⃣ Chunk Summarization

Each chunk sent to Bedrock LLM.

Output:

```json
{
  "summary": "",
  "tldr": "",
  "key_points": [],
  "risks": [],
  "action_items": []
}
```

---

### 4️⃣ Section Aggregation

Chunks grouped → synthesized summaries.

---

### 5️⃣ Executive Summary

Strategic document-level understanding generated.

---

### 6️⃣ Validation Layer

Pydantic schema ensures reliable structured output.

---

## 📡 API Usage

### Run Backend

```bash
cd Backend
uvicorn app.main:app --reload
```

---

### Endpoint

#### POST `/summarize`

Upload a document:

```
PDF or TXT
```

---

### Example Response

```json
{
  "metadata": {
    "total_chunks": 11,
    "coverage_percent": 100,
    "status": "ok"
  },
  "executive_summary": {
    "tldr": "...",
    "summary": "..."
  }
}
```

---

## 🔐 Environment Setup

Create `.env` inside Backend:

```
AWS_ACCESS_KEY_ID=XXXX
AWS_SECRET_ACCESS_KEY=XXXX
AWS_REGION=us-east-1
```

---

## ▶️ Installation

```bash
git clone https://github.com/<your-username>/GenAI-Document-Summarizer.git

cd Backend
python -m venv .venv
.venv\Scripts\activate

pip install -r requirements.txt
```

---

## 📊 Observability

System logs include:

* Request lifecycle
* Chunk processing
* Model latency
* Failures
* Coverage metrics

---

## 🎯 Use Cases

* Enterprise document intelligence
* Research summarization
* Legal document analysis
* Knowledge extraction systems
* AI assistants for long-context reasoning

---

## 🧠 Future Improvements

* Streaming summaries
* Vector database integration
* RAG-based querying
* Multi-document reasoning
* UI dashboard
* Agentic workflow orchestration

---

## 👨‍💻 Author

**Harshit Raj**

AI & Machine Learning Engineer
Focused on Generative AI, Agentic Systems, and Scalable LLM Architectures.

---

## ⭐ If you like this project

Give it a ⭐ on GitHub!
