# 0.1.1

- Filter out preprints whose review state is 'pending' during ingestion
  - These preprints' contributor metadata cannot be accessed via API
  - Change may result in a tiny change in numbers of ingested preprints
  - On 2026-03-11 there were a total of 50k 'accepted', 3k 'withdrawn', and 316 'pending' preprints
- Minor fixes to documentation
- Minor updates to dependencies
