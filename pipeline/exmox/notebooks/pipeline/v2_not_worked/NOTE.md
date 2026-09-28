# v2_not_worked

These are the v2 pipeline notebooks and files that were attempted but did not work reliably.
They are kept here for reference only.

The working pipeline uses the v1 (legacy) notebooks in the parent directory:
- 01_bronze_ingestion → 02_silver_transform → 03_gold_aggregation

## Contents

- `01_bronze_to_silver` — combined bronze+silver notebook (v2)
- `02_silver_to_gold` — silver+gold notebook (v2)
- `transforms.py` — extracted transforms module (v2)
- `tests/` — pytest tests for v2 transforms
- `databricks.yml` — DAB config for v2 pipeline
- `README.md` — v2 pipeline documentation
- `run_tests.py` — test runner script

## Why v2 didn't work

- Auto Loader checkpoint issues (stale checkpoints prevented re-ingestion)
- Schema mismatches between v1 and v2 table naming conventions (bronze_ prefix)
- DQ metrics table created with wrong schema
- Silver MERGE failed on _rescued_data column from Auto Loader

The v1 notebooks are more reliable and trustworthy for this pipeline.