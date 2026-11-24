# Group G source validator

Build and run from the repository root in a Linux Codespace:

```bash
docker build -t bdcc-g-validator:dev docker
mkdir -p docker/output/dev_initial
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e VALIDATOR_IMAGE_ID="$(docker image inspect bdcc-g-validator:dev --format '{{.Id}}')" \
  -v "$PWD/Data/development_G:/input:ro" \
  -v "$PWD/docker/output/dev_initial:/output" \
  bdcc-g-validator:dev --input /input --output /output --payment-day 2026-01-31
```

The run writes three files under `docker/output/dev_initial/`:

| File | Notebook | Purpose |
| --- | --- | --- |
| `run_record.json` | `01_land_raw.ipynb` | Input fingerprints, delivery gate, and row checks |
| `source_profile.csv` | `02_profile_sources.ipynb` | Independent key, replay, and conflict counts for all eight sources |
| `cutoff_profile.csv` | `03_clean_snapshots.ipynb` | Independent C1 and C2 status counts for the six close sources |

Upload all three files to
`/Volumes/workspace/bdcc_g_dev/landing/validator/dev_initial/` in Databricks.
The `inputs` file sizes in `run_record.json` must match the nine CSVs uploaded to
the Databricks Volume. Use copies of the same files in both places. Set
`--payment-day` to notebook 03's `payment_day` widget; C1 and C2 are 08:00 UTC
on the following two days. The default is `2026-01-31`.

The manifest gate is `PASS` only when every manifest row is usable and every
logical delivery contains its declared number of source rows. Source rows that
fail a row check are counted by reason but are retained in the release.

The output folder is generated evidence. Keep the code and Dockerfile in Git;
rerun when the input CSV bytes or payment day changes. A `HOLD` gate removes
any previous profile CSVs in the output folder so stale profiles cannot be used.

