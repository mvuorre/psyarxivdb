# OSFdata

A [Datasette](https://datasette.io/) instance serving preprint data tables from the [Open Science Framework](https://osf.io/) (OSF). Updated daily.

Browse preprints at <https://osfdata.vuorre.com>, where you can easily search, select, and filter preprint metadata according to your needs. 

For example, [here](https://osfdata.vuorre.com/preprints/preprints_by_time?granularity=month#g.mark=line&g.x_column=time_period&g.x_type=ordinal&g.y_column=count&g.y_type=quantitative) is a graph of preprints submitted over time, and [here](https://osfdata.vuorre.com/preprints.copyable?sql=select+id%2C+preprint_doi+from+preprints+where+%22date_created%22+like+%3Ap0+and+%22has_data_links%22+%3D+%3Ap1+and+%22provider%22+%3D+%3Ap2+order+by+date_created+desc+limit+11&p0=%25-12-24%25&p1=available&p2=psyarxiv&_table_format=github) are psyArXiv preprints submitted on christmas eve that have data:

| id       | preprint_doi                          |
|----------|---------------------------------------|
| 3u748_v1 | https://doi.org/10.31234/osf.io/3u748 |
| hsdbc_v1 | https://doi.org/10.31234/osf.io/hsdbc |
| avpk7_v1 | https://doi.org/10.31234/osf.io/avpk7 |
| j9xdr_v1 | https://doi.org/10.31234/osf.io/j9xdr |
| 56ugs_v1 | https://doi.org/10.31234/osf.io/56ugs |

## Development

### Prerequisites

- Python 3.8+
- [UV](https://github.com/astral-sh/uv)

### Setup

```bash
# Clone the repository
git clone https://github.com/mvuorre/osfdata
cd osfdata

# Create and activate virtual environment (with uv)
uv venv
source .venv/bin/activate

# Install dependencies
uv pip install -r requirements.txt

# Save dependencies when needed
uv pip freeze > requirements.txt
```

### Codebase

- `data/`: raw data and SQLite database
- `datasette/`: Datasette metadata and configuration
- `osf/`: OSF API client and data ingestion logic
- `scripts/`: harvesting, ingesting, and optimizing the database
- `tools/`: daily harvesting and ingesting, miscellaneous

### Data storage

Raw preprint data from the OSF API are stored as JSON files
```bash
data/raw/{preprint_year}/{preprint_month}/{preprint_day}/{preprint_id}.json
```

And then ingested into a SQLite database:

```bash
data/preprints.db
```

## Deploy & schedule

- Run `.venv/bin/datasette data/preprints.db --metadata datasette/metadata.yml --host 0.0.0.0 --port 8001` (with e.g. PM2)
- Schedule `tools/daily_osf_update.sh` with e.g. cron to
  - harvest preprints from the OSF API
  - ingest new preprints into the SQLite database
  - build the UI table

## Notes

[Comments](https://github.com/mvuorre/osfdatasette/issues) are welcome. 90% vibe-coded®. Thanks to the OSF and all who submitted preprints.
