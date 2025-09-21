#!/bin/bash

sqlite3 ./data/preprints.db \
  "SELECT * FROM preprints WHERE provider='psyarxiv';" \
  -header -csv | gzip > ./psyarxiv2.csv.gz