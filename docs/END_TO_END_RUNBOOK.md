# End-to-End Runbook

## Data lifecycle

```text
Source
  |
  v
Raw
  |
  v
Control / Hash Check
  |
  +--> SUCCESS hash exists --> SKIP
  |
  +--> New/Failed ----------> Ingested
                                  |
                                  v
                               Bronze
                                  |
                                  v
                               Silver
                                  |
                                  v
                                Gold
                                  |
                                  v
                               Archive
```

## Why Raw and Ingested are separate

Raw is a source snapshot and traceability layer. It is not the processing queue.

Ingested is the active queue. Only files approved by the control table are processed.

## Why Archive is after Gold

A file is archived only after the entire processing chain succeeds. This prevents a failed input from disappearing before it can be retried.

## Why a hash is used

A filename can remain the same while content changes. SHA-256 distinguishes:

```text
patients.csv + hash_A
patients.csv + hash_B
```

If the hash is already `SUCCESS`, the content is a duplicate and is skipped.

## Recovery

If Silver or Gold fails:

- do not archive
- keep the Ingested file
- retain the control status
- fix the issue
- retry the run

## Production hardening later

- Unity Catalog
- managed identities
- Key Vault
- alerting
- schema registry/contracts
- quarantine folder for invalid files
- automated tests
- CI/CD
- data lineage
