# Architecture

```text
                    +----------------+
                    | SOURCE SYSTEM  |
                    | Synthea / CSV  |
                    +-------+--------+
                            |
                            | ADF Copy
                            v
                    +---------------+
                    | RAW           |
                    | immutable     |
                    +-------+-------+
                            |
                            v
                    +---------------+
                    | CONTROL TABLE |
                    | SHA-256       |
                    | status        |
                    +-------+-------+
                            |
                 +----------+----------+
                 |                     |
              duplicate              new
                 |                     |
                SKIP               INGESTED
                                       |
                                       v
                                +-------------+
                                | DATABRICKS  |
                                | PySpark     |
                                +------+------+ 
                                       |
                            +----------+----------+
                            |                     |
                         SILVER                 GOLD
                            |                     |
                            +----------+----------+
                                       |
                                    success
                                       |
                                       v
                                   ARCHIVE
```

Core tools:

- ADLS Gen2: storage
- ADF: orchestration
- Databricks: processing
- Delta Lake: Bronze/Silver/Gold storage
- Key Vault: secrets
- Unity Catalog: governance
- GitHub: version control
- Optional Synapse/Power BI/GenAI: downstream consumption
