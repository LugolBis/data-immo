# data-immo

**data-immo** is a high-performance ETL pipeline built in **Rust** to efficiently extract, transform, and load real estate transaction data from the **DVF+ API** into a **Lakehouse (Dremio)**.  

The project is designed with a focus on **performance, reliability, and scalability**, leveraging modern data engineering tools and practices.

## 🔍​ Schema of the pipeline

```mermaid
flowchart LR
    A[**API** DVF+] -->|Extraction| B[**Rust**]
    B -->|Transformation| C[**Rust**]
    C -->|Loading| D[(**DuckDB**)]
    D -->|Saving cleaned data| E{**dbt**}
    E -->|Validation<br>& loading| F[(**Dremio**)]

    subgraph Extraction [Extract]
        A
    end
    
    subgraph TransformationRust [Transform]
        B
        C
    end

    subgraph LT [Load & Transform]
        D
    end

    subgraph VD [Validate Data quality]
        E
    end
    
    subgraph LakeHouse [LakeHouse]
        F
    end

    subgraph Docker [Docker Container]
        LakeHouse
    end
    
    style A fill:#000091,stroke:#ffffff,color:#ffffff,stroke-width:1px
    style B fill:#955a34,stroke:#000000,color:#000000,stroke-width:1px
    style C fill:#955a34,stroke:#000000,color:#000000,stroke-width:1px
    style D fill:#fef242,stroke:#000000,color:#000000,stroke-width:1px
    style E fill:#fc7053,stroke:#000000,color:#000000,stroke-width:1px
    style F fill:#31d3db,stroke:#ffffff,color:#ffffff,stroke-width:1px
    style Docker fill: #099cec
```

## 🚀 Features  

- **Data Extraction**  
  - Fetches real estate transaction data from the **DVF+ API**.  
  - Handles API rate limiting, retries, and efficient pagination using Rust’s concurrency model.

- **Data Transformation**  
  - Uses **DuckDB** to transform optimized **Parquet** data into structured, queryable formats.  
  - Rust is used for additional transformations, data enrichment, and performance-critical operations (I/O, etc.).  

- **Data Validation & Loading**  
  - **dbt** is used to validate, test, and model the data.  
  - The cleaned and validated data is loaded into **Dremio**, enabling a Lakehouse architecture.  

## 🛠️ Tech Stack  

- **Rust** → Core language for API calls, transformations, and performance optimization.  
- **DuckDB** → In-process SQL engine for fast transformations of optimized Parquet datasets.  
- **dbt** → Data modeling, testing, and validation layer.  
- **Dremio** → Lakehouse platform for analytics and querying.  

## 📂 Pipeline Overview  

1. **Extract**: Retrieve raw transaction data from DVF+ API.  
2. **Stage**: Store raw data as Parquet.  
3. **Transform**: Apply transformations using DuckDB and Rust.  
4. **Validate & Model**: Use dbt to ensure data quality and prepare final schemas.  
5. **Load**: Push validated datasets into Dremio for downstream analytics.
