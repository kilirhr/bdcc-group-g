# Group G source validator (development extract)

Build and run from the repository root in a Linux Codespace:

```bash
docker build -t bdcc-g-validator:dev docker
mkdir -p docker/output/dev_initial
docker run --rm \
  --user "$(id -u):$(id -g)" \
  -e VALIDATOR_IMAGE_ID="$(docker image inspect bdcc-g-validator:dev --format '{{.Id}}')" \
  -v "$PWD/Data/development_G:/input:ro" \
  -v "$PWD/docker/output/dev_initial:/output" \
  bdcc-g-validator:dev --input /input --output /output
```

The run writes `docker/output/dev_initial/run_record.json`. Upload that file to
`/Volumes/workspace/bdcc_g_dev/landing/validator/dev_initial/` for section 5 of
`01_land_raw.ipynb`. The `inputs` file sizes must match the nine CSVs uploaded to
the Databricks Volume. Use copies of the same files in both places.

The manifest gate is `PASS` only when every manifest row is usable and every
logical delivery contains its declared number of source rows. Source rows that
fail a row check are counted by reason but are retained in the release.

The output folder is generated evidence. Keep the code and Dockerfile in Git;
run again whenever the input CSV bytes change.

