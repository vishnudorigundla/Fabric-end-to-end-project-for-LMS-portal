# Fabric-end-to-end-project-for-LMS-portal 
# 🏗️ Microsoft Fabric — End-to-End Data Engineering Pipeline

> A fully automated, enterprise-grade data lakehouse solution built on **Microsoft Fabric**, implementing a **Medallion Architecture** (Raw → Landing → Bronze → Silver → Gold) with dynamic parameterization, upsert logic, and scheduled orchestration.

---

## 📌 Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Tech Stack](#tech-stack)
- [Project Structure](#project-structure)
- [Layer-by-Layer Implementation](#layer-by-layer-implementation)
  - [Raw Layer — Azure Data Lake Storage](#raw-layer--azure-data-lake-storage)
  - [Landing Layer — Data Ingestion](#landing-layer--data-ingestion)
  - [Bronze Layer — Raw to Structured](#bronze-layer--raw-to-structured)
  - [Silver Layer — Data Transformation](#silver-layer--data-transformation)
  - [Gold Layer — Business Aggregation](#gold-layer--business-aggregation)
- [Pipeline Orchestration](#pipeline-orchestration)
- [Dynamic Parameterization](#dynamic-parameterization)
- [Scheduling & Automation](#scheduling--automation)
- [Setup & Prerequisites](#setup--prerequisites)
- [Contributing](#contributing)

---

## 📖 Project Overview

This project demonstrates a **production-ready, end-to-end data engineering pipeline** using **Microsoft Fabric** as the unified analytics platform. The solution ingests raw daily files from **Azure Blob Storage**, applies staged transformations across the **Medallion Architecture**, and delivers clean, business-ready fact and dimension tables in the **Gold layer** — all fully automated with no manual intervention.

Key design principles followed:
- **Immutability of raw data** — source files are protected and never modified
- **Incremental processing** — upsert (MERGE) logic handles new inserts and updates efficiently
- **Dynamic parameterization** — all pipeline activities use runtime context, eliminating hardcoded values
- **Separation of concerns** — each medallion layer has a clearly defined responsibility
- **Fault tolerance** — try/except exception handling ensures pipeline resilience

---

## 🏛️ Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     AZURE DATA LAKE STORAGE                         │
│  ┌──────────────┐          ┌─────────────────┐                      │
│  │  raw/        │ ──────►  │  landing/       │  (Protected Source)  │
│  │  (immutable) │          │  (ingested copy)│                      │
│  └──────────────┘          └─────────────────┘                      │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                                    ▼
┌─────────────────────────────────────────────────────────────────────┐
│                     MICROSOFT FABRIC WORKSPACE                      │
│                                                                     │
│   Landing ──► Bronze (LH_bronze) ──► Silver ──► Gold               │
│               Structured Tables     Cleaned    Fact & Dim Tables    │
│               + Upsert Logic        + Transformed  + Upsert Logic   │
└─────────────────────────────────────────────────────────────────────┘
                                    │
                              Orchestrated by
                         Master Pipeline (Invoke Pipeline)
                         + Daily Scheduled Trigger
```

---

## 🛠️ Tech Stack

| Component | Technology |
|---|---|
| Cloud Storage | Azure Blob Storage (ADLS Gen2) |
| Analytics Platform | Microsoft Fabric |
| Compute | Fabric Notebooks (PySpark / SQL) |
| Orchestration | Fabric Data Pipelines |
| Storage Format | Delta Lake (Lakehouse Tables) |
| Transformation | PySpark, Spark SQL |
| Data Modeling | Star Schema (Fact + Dimension Tables) |
| Scheduling | Fabric Pipeline Scheduled Triggers |
| Version Control | GitHub |

---

## 📁 Project Structure

```
fabric-medallion-pipeline/
│
├── pipelines/
│   ├── pl_raw_to_landing.json          # Pipeline: Raw → Landing (GetMetadata + ForEach)
│   ├── pl_landing_to_bronze.json       # Pipeline: Landing → Bronze (Notebook invocation)
│   ├── pl_landing_to_silver.json       # Pipeline: Landing → Silver (Notebook invocation)
│   ├── pl_landing_to_gold.json         # Pipeline: Landing → Gold (Notebook invocation)
│   └── pl_master_orchestrator.json     # Master pipeline (Invoke Pipeline activities)
│
├── notebooks/
│   ├── nb_raw_to_landing.ipynb         # Copies files from raw to landing container
│   ├── nb_landing_to_bronze.ipynb      # Ingests landing data → LH_bronze Delta table
│   ├── nb_landing_to_silver.ipynb      # Applies transformations → Silver layer
│   └── nb_landing_to_gold.ipynb        # Builds fact & dimension tables → Gold layer
│
├── lakehouse/
│   ├── bronze/
│   │   └── LH_bronze/                  # Bronze Lakehouse definition
│   ├── silver/
│   │   └── LH_silver/                  # Silver Lakehouse definition
│   └── gold/
│       └── LH_gold/                    # Gold Lakehouse with Fact & Dim tables
│
├── config/
│   └── parameters.json                 # Centralized parameter reference
│
├── docs/
│   └── architecture_diagram.png        # Architecture overview
│
└── README.md
```

---

## 🔄 Layer-by-Layer Implementation

### Raw Layer — Azure Data Lake Storage

- **Azure Storage Account** provisioned with a dedicated container for raw data ingestion.
- Daily files are landed into the `raw/` container folder.
- Raw data is treated as **immutable** — no transformations or deletions are performed at this layer, ensuring a reliable audit trail and re-processability.
- A separate `landing/` folder is maintained within the same storage account to stage files for downstream processing while keeping raw data intact.

---

### Landing Layer — Data Ingestion

**Pipeline:** `pl_raw_to_landing`

**Approach:**
- **Get Metadata** activity dynamically enumerates all files present in the `raw/` container at runtime.
- **ForEach** activity iterates over the discovered file list and triggers the ingestion notebook per file.
- An embedded **Notebook activity** (inside ForEach) accepts toggle parameters:
  - `processed_date` — dynamically resolved to today's date at runtime
  - `file_name` — dynamically passed from the ForEach iterator item

**Design rationale:** Parameterizing the notebook inside ForEach enables parallel, file-level processing without requiring any hardcoded file references. New files added to raw are automatically picked up on the next scheduled run.

---

### Bronze Layer — Raw to Structured

**Notebook:** `nb_landing_to_bronze`

**Toggle Parameters:**
- `workspace_name` — Fabric workspace identifier (resolved dynamically at runtime)
- `processed_date` — today's date for partitioning and audit columns

**Implementation highlights:**
- Reads files from the `landing/` folder using the parameterized paths.
- Creates and manages the `LH_bronze` Lakehouse table using **Spark SQL**.
- **Try/Except exception handling** wraps all write operations to ensure failures are caught gracefully and surfaced with meaningful error messages.
- **Upsert (MERGE) logic** is implemented to handle two scenarios:
  - **New records** — inserted into the bronze table on first load.
  - **Updated records** — if source data is corrected or re-delivered in landing, the corresponding bronze records are updated rather than duplicated, maintaining data integrity.

---

### Silver Layer — Data Transformation

**Notebook:** `nb_landing_to_silver`

**Implementation highlights:**
- Reads from the `LH_bronze` Delta table.
- Applies a comprehensive **data cleansing and transformation** pipeline:
  - Null handling and type casting
  - Duplicate removal and deduplication logic
  - Standardization of date formats, string normalization, and column renaming
  - Derived column computation as required
- Business-aligned transformations are applied at this layer based on domain requirements.
- Writes clean, analytics-ready data to the **Silver Lakehouse** as Delta tables.

---

### Gold Layer — Business Aggregation

**Notebook:** `nb_landing_to_gold`

**Toggle Parameters:**
- Additional business-context parameters configured as toggle inputs

**Implementation highlights:**
- Implements a **Star Schema** design:
  - **Fact Tables** — transactional/measurable business events
  - **Dimension Tables** — descriptive attributes (e.g., date, product, customer)
- **Upsert (MERGE) logic** is applied in the Gold layer as well, because:
  - Late-arriving dimension updates (e.g., a customer changes their region) must be reflected without full table reloads.
  - Fact table corrections from upstream restatements are handled without duplicating rows.
- All Gold layer tables are optimized for BI tool consumption (Power BI, etc.).

---

## ⚙️ Pipeline Orchestration

**Master Pipeline:** `pl_master_orchestrator`

The master pipeline uses **Invoke Pipeline** activities to sequentially chain all layer-specific pipelines:

```
pl_master_orchestrator
    │
    ├── [1] Invoke: pl_raw_to_landing
    │         └── GetMetadata → ForEach → nb_raw_to_landing
    │
    ├── [2] Invoke: pl_landing_to_bronze
    │         └── nb_landing_to_bronze
    │
    ├── [3] Invoke: pl_landing_to_silver
    │         └── nb_landing_to_silver
    │
    └── [4] Invoke: pl_landing_to_gold
              └── nb_landing_to_gold
```

**Key design decision — Dynamic Context over Static Values:**
All pipeline parameters are passed using **dynamic expressions** (e.g., `@utcNow()`, `@pipeline().parameters.workspaceName`) rather than static hardcoded strings. This ensures the pipeline is environment-agnostic, reusable across workspaces, and fully automated without requiring manual updates on each run.

---

## 🔁 Dynamic Parameterization

All notebooks are designed with **toggle parameters** (Fabric notebook parameters cell) to support dynamic runtime injection from pipelines:

| Parameter | Used In | Value Source |
|---|---|---|
| `processed_date` | Raw→Landing, Bronze | `@formatDateTime(utcNow(), 'yyyy-MM-dd')` |
| `file_name` | Raw→Landing | ForEach iterator: `@item().name` |
| `workspace_name` | Bronze, Silver, Gold | Pipeline parameter / environment variable |

This approach means **zero code changes** are needed when the pipeline is promoted across Dev → UAT → Production environments.

---

## 🕐 Scheduling & Automation

- A **Scheduled Trigger** is configured on the master pipeline (`pl_master_orchestrator`).
- The pipeline runs **daily at a configured time**, automatically processing whatever new file has been dropped into the `raw/` container.
- No manual intervention is required for day-to-day operations once deployed.

---

## 🚀 Setup & Prerequisites

### Prerequisites

- Active **Azure Subscription** with Contributor access
- **Microsoft Fabric** workspace (Trial, Capacity, or Premium)
- Azure Storage Account with ADLS Gen2 hierarchical namespace enabled
- Fabric Lakehouse instances created for Bronze, Silver, and Gold layers

### Deployment Steps

1. **Clone this repository**
   ```bash
   git clone https://github.com/<your-org>/fabric-medallion-pipeline.git
   cd fabric-medallion-pipeline
   ```

2. **Configure Azure Storage**
   - Create a storage account and enable ADLS Gen2
   - Create a container with `raw/` and `landing/` folders
   - Grant Fabric workspace Managed Identity access (Storage Blob Data Contributor)

3. **Import Notebooks into Fabric**
   - Navigate to your Fabric Workspace
   - Import each `.ipynb` file from the `notebooks/` directory
   - Update the `workspace_name` parameter in each notebook's parameters cell to match your environment

4. **Import Pipelines into Fabric**
   - Import each `.json` pipeline definition from the `pipelines/` directory
   - Update linked service connections to point to your Azure Storage Account
   - Verify notebook activity references point to the correct imported notebooks

5. **Configure Master Pipeline Parameters**
   - Open `pl_master_orchestrator`
   - Confirm all Invoke Pipeline activities reference the correct child pipelines
   - Validate dynamic expressions for date and workspace parameters

6. **Set Up Scheduled Trigger**
   - In `pl_master_orchestrator`, add a Schedule trigger
   - Set the desired daily run time (recommended: early morning before business hours)
   - Activate the trigger

7. **Run & Validate**
   - Drop a test file into the `raw/` container
   - Trigger the master pipeline manually for the first run
   - Validate row counts and data quality across Bronze → Silver → Gold layers

---

## 🤝 Contributing

Contributions are welcome. Please follow the branching strategy below:

```
main          → Production-ready code only
dev           → Active development branch
feature/*     → Feature branches (e.g., feature/add-silver-transformation)
hotfix/*      → Critical production fixes
```

1. Fork the repository
2. Create your feature branch: `git checkout -b feature/your-feature-name`
3. Commit your changes: `git commit -m "feat: describe your change"`
4. Push to the branch: `git push origin feature/your-feature-name`
5. Open a Pull Request against `dev`

---

## 📄 License

This project is licensed under the MIT License. See [LICENSE](LICENSE) for details.

---

> Built with ❤️ using **Microsoft Fabric** | Medallion Architecture | Delta Lake | PySpark
