# chip-lakehouse

A local fintech data lakehouse: Spark + Delta Lake for storage/compute, Unity
Catalog (OSS, self-hosted via Docker) for the catalog and access control,
following a bronze/silver/gold medallion layout plus an `ml` schema for
model-ready features.

## Architecture

- **Storage/compute**: PySpark + Delta Lake
- **Catalog**: [Unity Catalog OSS](https://www.unitycatalog.io/) running in
  Docker, fronted by Spark's `UCSingleCatalog` connector
- **Schemas**: `bronze` (raw ingest) → `silver` (cleaned/conformed) → `gold`
  (business-level marts), including a conformed Kimball star (`dim_*` / `fct_*`)
  → `ml` (features/model inputs)
- **Access control**: declarative, in [iam/access.yaml](iam/access.yaml),
  applied via [src/manage_access.py](src/manage_access.py)

## Setup

1. **Unity Catalog config** — copy the template and fill in real values:
   ```bash
   cp docker/etc/conf/server.properties.example docker/etc/conf/server.properties
   ```
   Edit `docker/etc/conf/server.properties` with your own Google OAuth
   client id/secret (or leave `server.authorization=disable` for a
   no-auth local setup).

2. **Start Unity Catalog**:
   ```bash
   cd docker && docker compose up -d
   ```
   On boot the server generates an admin token and mirrors it to
   `docker/etc/conf/token.txt` (used by the Python scripts below).

3. **Python environment**:
   ```bash
   python3 -m venv finenv && source finenv/bin/activate
   pip install -r requirements.txt
   ```

4. **Bootstrap the catalog + schemas**:
   ```bash
   python3 src/spark_session.py
   ```

5. **Apply the access model** (users + grants declared in `iam/access.yaml`):
   ```bash
   python3 src/manage_access.py            # plan (dry run)
   python3 src/manage_access.py --apply    # apply
   ```

6. **Interactive exploration** (optional) — `notebooks/explore.ipynb` imports
   the same `src/` modules the pipeline uses, for poking at results without
   re-running a whole script:
   ```bash
   python3 -m ipykernel install --user --name=chip-lakehouse --display-name "chip-lakehouse (finenv)"
   jupyter lab notebooks/
   ```
   No pipeline logic belongs in notebooks - see `docs/standard.md`.

## Running the pipeline

After setup, run the stages in order (each writes into the `chip_lakehouse` catalog):

```bash
python3 data_gen/generate_data.py     # synthetic CSVs (first run only)
python3 src/bronze_ingest.py          # raw -> bronze
python3 src/silver_transform.py       # bronze -> silver (cleaned, normalized)
python3 src/gold_marts.py             # silver -> gold wide marts
python3 src/gold_star.py              # silver -> gold Kimball star (dim_*/fct_*)
python3 tests/verify_e2e.py           # cross-layer invariant checks
```

`gold_marts.py` and `gold_star.py` are independent and can run in either
order. `gold_star.py`'s `dim_customer` / `dim_account` persist across
runs (SCD2); every other gold table is a full overwrite.

## Project layout

```
src/            Spark session bootstrap, access reconciliation, pipeline stages
docker/         Unity Catalog server (docker-compose + config)
iam/            Declarative user/privilege config
data_gen/       Synthetic data generation
notebooks/      Interactive exploration (no pipeline logic)
infra/          Terraform
docs/           Governance, model card, standards
tests/          Data quality tests
```
