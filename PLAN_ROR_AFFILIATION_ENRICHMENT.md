# ROR API Affiliation Enrichment Plan

## Executive Summary

This plan outlines the strategy for enriching affiliation data in PsyArXivDB using the Research Organization Registry (ROR) API. The goal is to normalize, structure, and enhance institution data from OSF contributor employment records with standardized ROR identifiers and metadata.

---

## 1. Current State Analysis

### Existing Data Model
- **Storage**: Affiliations stored in `contributors.employment` as JSON string
- **Source**: OSF API user attributes (`employment` field)
- **Limitations**:
  - No normalization of institution names
  - No structured search/filtering capabilities
  - No linkage to external organization databases
  - Employment data stored as opaque blob

### Current Data Pipeline
```
OSF API → raw_data → ingest → contributors (employment JSON) → Datasette
```

---

## 2. Proposed Database Schema

### New Tables

#### `affiliations` - Normalized Organization Data
```sql
CREATE TABLE affiliations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    ror_id TEXT UNIQUE,                -- ROR identifier (e.g., "https://ror.org/03vek6s52")
    name TEXT NOT NULL,                -- Primary organization name
    acronyms TEXT,                     -- JSON array of acronyms
    aliases TEXT,                      -- JSON array of alternative names
    country_code TEXT,                 -- ISO 3166-1 alpha-2 code
    country_name TEXT,                 -- Full country name
    continent_code TEXT,               -- Two-letter continent code
    continent_name TEXT,               -- Full continent name
    city TEXT,                         -- Primary city location
    types TEXT,                        -- JSON array of organization types
    established INTEGER,               -- Year established
    status TEXT,                       -- active/inactive/withdrawn
    domains TEXT,                      -- JSON array of domains
    links TEXT,                        -- JSON array of website/Wikipedia links
    external_ids TEXT,                 -- JSON array of other identifiers (ISNI, Wikidata, etc.)
    ror_metadata TEXT,                 -- Full ROR response for future reprocessing
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_ror_id (ror_id),
    INDEX idx_name (name),
    INDEX idx_country (country_code)
);
```

#### `contributor_affiliations` - Join Table
```sql
CREATE TABLE contributor_affiliations (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    osf_user_id TEXT NOT NULL,         -- Links to contributors.osf_user_id
    affiliation_id INTEGER NOT NULL,   -- Links to affiliations.id
    institution_name TEXT,             -- Original name from OSF (for auditing)
    start_date TEXT,                   -- Employment start date (if available)
    end_date TEXT,                     -- Employment end date (if available)
    is_current BOOLEAN,                -- Whether this is current employment
    match_confidence REAL,             -- 0.0-1.0 confidence score for ROR match
    match_method TEXT,                 -- How match was found (exact/fuzzy/manual)
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    FOREIGN KEY (osf_user_id) REFERENCES contributors(osf_user_id),
    FOREIGN KEY (affiliation_id) REFERENCES affiliations(id),
    INDEX idx_osf_user (osf_user_id),
    INDEX idx_affiliation (affiliation_id),
    UNIQUE (osf_user_id, affiliation_id, start_date)
);
```

#### `ror_cache` - API Response Cache
```sql
CREATE TABLE ror_cache (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    query_string TEXT NOT NULL,        -- Original search query
    query_hash TEXT UNIQUE NOT NULL,   -- MD5 hash of normalized query
    ror_response TEXT,                 -- Full API response JSON
    num_results INTEGER,               -- Number of results returned
    date_cached TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    INDEX idx_query_hash (query_hash),
    INDEX idx_date_cached (date_cached)
);
```

#### `affiliation_match_queue` - Manual Review Queue
```sql
CREATE TABLE affiliation_match_queue (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    institution_name TEXT NOT NULL,    -- Original name needing review
    ror_candidates TEXT,               -- JSON array of possible ROR matches
    num_contributors INTEGER,          -- How many contributors have this affiliation
    status TEXT DEFAULT 'pending',     -- pending/reviewed/rejected
    matched_ror_id TEXT,               -- Selected ROR ID (if reviewed)
    notes TEXT,                        -- Admin notes
    date_added TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    date_reviewed TIMESTAMP,

    INDEX idx_status (status),
    UNIQUE (institution_name)
);
```

### Enhanced Views

#### `contributors_with_affiliations` - Queryable View
```sql
CREATE VIEW contributors_with_affiliations AS
SELECT
    c.osf_user_id,
    c.full_name,
    c.date_registered,
    a.ror_id,
    a.name AS affiliation_name,
    a.country_name,
    a.city,
    ca.is_current,
    ca.match_confidence,
    json_extract(a.types, '$') AS organization_types
FROM contributors c
LEFT JOIN contributor_affiliations ca ON c.osf_user_id = ca.osf_user_id
LEFT JOIN affiliations a ON ca.affiliation_id = a.id;
```

---

## 3. ROR API Integration Strategy

### ROR API v2 Overview
- **Base URL**: `https://api.ror.org/v2/organizations`
- **Authentication**: None required (but client ID recommended for higher rate limits in Dec 2025)
- **Rate Limits**:
  - With client ID: 2000 requests per 5 minutes (6.7 req/sec)
  - Without client ID (after Dec 2025): 50 requests per 5 minutes (0.17 req/sec)
- **Response Format**: JSON
- **Current Version**: v2 (released April 2024, mandatory after Dec 2025)

### Key Endpoints

1. **Search Organizations**
   - `GET /v2/organizations?query={query}`
   - Supports Elasticsearch query string syntax
   - Can search across all fields (names, aliases, acronyms, locations)

2. **Retrieve Single Organization**
   - `GET /v2/organizations/{ror_id}`
   - Direct lookup by ROR identifier

3. **Advanced Query**
   - Supports field-specific searches, wildcards, Boolean operators
   - Example: `?query.advanced=names.value:Stanford AND locations.geonames_details.country_code:US`

### Matching Strategy (Multi-Tier Approach)

#### Tier 1: Exact Match (High Confidence)
- Direct string match against ROR primary names
- Match against aliases and acronyms
- Confidence: 1.0

#### Tier 2: Normalized Match (High Confidence)
- Lowercase, remove punctuation, strip whitespace
- Handle common abbreviations (Univ → University, Inst → Institute)
- Confidence: 0.9-0.95

#### Tier 3: Fuzzy Match (Medium Confidence)
- Use ROR API search with relevance scoring
- Levenshtein distance for string similarity
- Consider location context (country, city) if available
- Confidence: 0.7-0.89

#### Tier 4: Manual Review (Variable Confidence)
- Queue ambiguous matches for manual review
- Show top N candidates from ROR API
- Allow admin to confirm/reject/select match
- Confidence: Set by reviewer

#### No Match
- Store original institution name
- Add to manual review queue
- Periodically retry with ROR data updates

### Caching Strategy

1. **Query-Level Cache** (`ror_cache` table)
   - Cache all ROR API responses for 90 days
   - Use MD5 hash of normalized query as key
   - Reduces redundant API calls for common institutions

2. **Organization-Level Cache** (`affiliations` table)
   - Once matched, ROR data persists permanently
   - Update on-demand or periodic batch refresh
   - Store full ROR metadata for future schema changes

3. **Negative Cache**
   - Store "no match found" results to avoid repeated failed lookups
   - Clear on ROR data releases (quarterly)

---

## 4. Data Enrichment Pipeline

### Enhanced Pipeline Flow
```
OSF API → raw_data → ingest →
  ↓
  contributors (employment JSON)
  ↓
  [NEW] affiliation extraction
  ↓
  [NEW] ROR matching (with caching)
  ↓
  affiliations + contributor_affiliations
  ↓
  Datasette (enhanced queries)
```

### Implementation Phases

#### Phase 1: Infrastructure Setup
1. Create new database tables
2. Implement ROR API client (`osf/ror_client.py`)
   - Request handling with retries
   - Rate limiting (respect 2000 req/5min)
   - Error handling (network, API errors)
   - Response parsing and validation
3. Implement caching layer
4. Add configuration for ROR API (`osf/config.py`)

#### Phase 2: Data Extraction
1. Parse `contributors.employment` JSON
2. Extract institution names, dates, employment status
3. Normalize institution name variations
4. Handle edge cases (empty, malformed, non-institutional affiliations)

#### Phase 3: ROR Matching Engine
1. Implement matching algorithm (`osf/affiliation_matcher.py`)
   - Tier 1: Exact matching
   - Tier 2: Normalized matching
   - Tier 3: Fuzzy matching with ROR search
2. Confidence scoring system
3. Manual review queue population
4. Batch processing with progress tracking

#### Phase 4: Database Population
1. Insert unique affiliations to `affiliations` table
2. Create contributor-affiliation links
3. Update existing contributor records
4. Create Datasette views and facets

#### Phase 5: Manual Review Workflow (Optional)
1. Admin interface for reviewing ambiguous matches
2. Bulk approval/rejection tools
3. Periodic re-matching of unmatched affiliations

#### Phase 6: Maintenance & Updates
1. Scheduled ROR data refresh (quarterly with ROR releases)
2. Monitor ROR API changes (v1 sunset Dec 2025)
3. Backfill new contributors with enriched affiliations
4. Performance monitoring and optimization

---

## 5. Implementation Details

### New Python Modules

#### `osf/ror_client.py` - ROR API Client
```python
class RORClient:
    """Client for interacting with ROR API v2."""

    def __init__(self, cache_db_path: str):
        self.base_url = "https://api.ror.org/v2/organizations"
        self.rate_limiter = RateLimiter(max_requests=2000, window=300)
        self.cache = RORCache(cache_db_path)

    def search(self, query: str, use_cache: bool = True) -> List[Dict]:
        """Search organizations by name."""
        pass

    def get_by_id(self, ror_id: str) -> Dict:
        """Retrieve organization by ROR ID."""
        pass

    def advanced_query(self, query_params: Dict) -> List[Dict]:
        """Perform advanced search with field-specific filters."""
        pass
```

#### `osf/affiliation_matcher.py` - Matching Logic
```python
class AffiliationMatcher:
    """Matches institution names to ROR entries."""

    def __init__(self, ror_client: RORClient, db_path: str):
        self.ror_client = ror_client
        self.db = sqlite3.connect(db_path)

    def match_institution(self, name: str, context: Dict = None) -> MatchResult:
        """
        Match institution name to ROR entry.

        Returns MatchResult with:
        - ror_id: Matched ROR identifier
        - confidence: 0.0-1.0 confidence score
        - method: Matching method used
        - candidates: Alternative matches
        """
        pass

    def normalize_name(self, name: str) -> str:
        """Normalize institution name for matching."""
        pass

    def fuzzy_match(self, name: str, threshold: float = 0.7) -> List[MatchResult]:
        """Perform fuzzy matching against ROR database."""
        pass
```

#### `osf/affiliation_extractor.py` - Data Extraction
```python
class AffiliationExtractor:
    """Extracts affiliation data from OSF employment JSON."""

    def extract_from_employment(self, employment_json: str) -> List[Affiliation]:
        """
        Parse OSF employment JSON and extract structured affiliations.

        Returns list of Affiliation objects with:
        - institution_name
        - start_date
        - end_date
        - is_current
        """
        pass

    def validate_affiliation(self, aff: Affiliation) -> bool:
        """Check if affiliation data is valid and complete."""
        pass
```

### Integration into Ingest Pipeline

Modify `osf/ingestor.py`:

```python
# Add to Ingestor class
def enrich_affiliations(self, contributor_data: Dict) -> None:
    """Enrich contributor with ROR-matched affiliations."""

    # 1. Extract affiliations from employment JSON
    employment = json.loads(contributor_data.get('employment', '[]'))
    affiliations = self.affiliation_extractor.extract_from_employment(employment)

    # 2. Match each affiliation to ROR
    for aff in affiliations:
        match_result = self.affiliation_matcher.match_institution(
            aff.institution_name,
            context={'country': aff.country}  # if available
        )

        # 3. Store in database
        if match_result.confidence >= 0.7:
            # High/medium confidence - auto-accept
            affiliation_id = self.upsert_affiliation(match_result.ror_data)
            self.link_contributor_affiliation(
                contributor_data['osf_user_id'],
                affiliation_id,
                aff,
                match_result
            )
        else:
            # Low confidence - queue for review
            self.queue_for_manual_review(aff, match_result.candidates)
```

### CLI Scripts

#### `scripts/enrich_affiliations.py`
```python
"""
Enrich existing contributor affiliations with ROR data.

Usage:
    python scripts/enrich_affiliations.py [--limit N] [--force-refresh]
"""
```

#### `scripts/review_affiliation_matches.py`
```python
"""
Interactive CLI for reviewing ambiguous affiliation matches.

Usage:
    python scripts/review_affiliation_matches.py [--batch-size N]
"""
```

#### `scripts/refresh_ror_data.py`
```python
"""
Refresh ROR metadata for existing affiliations.

Usage:
    python scripts/refresh_ror_data.py [--since DATE]
"""
```

---

## 6. Configuration Updates

### `osf/config.py` - Add ROR Settings
```python
# ROR API Configuration
ROR_API_BASE_URL = "https://api.ror.org/v2/organizations"
ROR_API_TIMEOUT = 30  # seconds
ROR_REQUEST_DELAY = 0.5  # seconds between requests (conservative)
ROR_CACHE_DAYS = 90  # days to cache API responses
ROR_MATCH_THRESHOLD = 0.7  # minimum confidence for auto-match

# Future: Add client ID for higher rate limits (Dec 2025+)
ROR_CLIENT_ID = None  # Set to your client ID when available
```

### `pyproject.toml` - Add Dependencies
```toml
[project]
dependencies = [
    # ... existing dependencies ...
    "fuzzywuzzy>=0.18.0",  # Fuzzy string matching
    "python-Levenshtein>=0.25.0",  # Fast string distance
    "requests-cache>=1.1.0",  # Enhanced caching (optional)
]
```

### `Makefile` - Add New Targets
```makefile
.PHONY: enrich-affiliations
enrich-affiliations:
	python scripts/enrich_affiliations.py

.PHONY: review-matches
review-matches:
	python scripts/review_affiliation_matches.py

.PHONY: refresh-ror
refresh-ror:
	python scripts/refresh_ror_data.py

# Update daily workflow
daily-update: harvest ingest enrich-affiliations fix-version-flags analyze vacuum
```

---

## 7. Datasette Integration

### Enhanced Metadata (`datasette/metadata.yml`)

```yaml
databases:
  preprints:
    tables:
      affiliations:
        title: "Research Organizations (ROR)"
        description: "Normalized organization data from Research Organization Registry"
        columns:
          ror_id: "ROR Identifier"
          name: "Organization Name"
          country_name: "Country"
          types: "Organization Types"
        facets:
          - country_name
          - continent_name
          - status

      contributor_affiliations:
        title: "Contributor Affiliations"
        description: "Links between contributors and their organizational affiliations"
        columns:
          match_confidence: "Match Confidence Score"
          match_method: "Matching Method"
        facets:
          - match_confidence
          - is_current

      contributors_with_affiliations:
        title: "Contributors with Affiliations"
        description: "Contributors enriched with ROR organization data"
```

### Faceted Search Enhancements
- Filter preprints by author affiliation country
- Filter by organization type (Education, Healthcare, Company, etc.)
- Search by institution name with autocomplete
- View preprints by continent/region

---

## 8. Performance Considerations

### Scalability
- **Current Contributors**: ~50K-100K (estimate based on PsyArXiv size)
- **Unique Institutions**: ~5K-10K (estimate)
- **ROR Database Size**: ~100K organizations
- **Initial Enrichment Time**:
  - 10K unique institutions × 0.5s = ~1.5 hours (with caching)
  - Can be run offline, incrementally

### Optimization Strategies
1. **Batch Processing**: Process contributors in batches of 100
2. **Cache Warming**: Pre-load common institution names
3. **Parallel Processing**: Multi-threaded ROR lookups (respect rate limits)
4. **Lazy Loading**: Enrich on-demand for new contributors
5. **Incremental Updates**: Only process new/modified employment data

### Storage Impact
- **affiliations table**: ~10K rows × 2KB = ~20MB
- **contributor_affiliations table**: ~100K rows × 500B = ~50MB
- **ror_cache table**: ~10K rows × 5KB = ~50MB
- **Total Additional Storage**: ~120MB (negligible for modern systems)

---

## 9. Quality Assurance & Monitoring

### Metrics to Track
- **Match Rate**: % of affiliations successfully matched to ROR
- **Confidence Distribution**: Histogram of match confidence scores
- **Manual Review Queue Size**: Number of pending reviews
- **API Success Rate**: ROR API uptime and error rates
- **Cache Hit Rate**: % of queries served from cache

### Data Quality Checks
1. **Validation**: Ensure all ROR IDs are valid format
2. **Completeness**: Track % of contributors with affiliation data
3. **Consistency**: Verify employment dates are logical
4. **Freshness**: Monitor ROR data staleness

### Logging & Error Handling
- Log all ROR API requests and responses
- Alert on repeated matching failures
- Track institutions that consistently fail to match
- Monitor rate limit utilization

---

## 10. Migration & Rollout Strategy

### Pre-Launch Checklist
- [ ] Test ROR API integration with sandbox data
- [ ] Benchmark matching algorithm accuracy on sample data
- [ ] Create database backups
- [ ] Document manual review workflow
- [ ] Set up monitoring and alerting

### Rollout Phases
1. **Week 1-2**: Infrastructure setup and testing
   - Create tables and indexes
   - Implement ROR client and caching
   - Test on small dataset (100 contributors)

2. **Week 3-4**: Matching algorithm development
   - Implement tiered matching strategy
   - Tune confidence thresholds
   - Validate against known ground truth

3. **Week 5**: Backfill existing data
   - Run enrichment on all existing contributors
   - Populate manual review queue
   - Generate quality metrics

4. **Week 6**: Manual review and refinement
   - Process manual review queue
   - Adjust matching thresholds based on feedback
   - Re-run low-confidence matches

5. **Week 7**: Integration and deployment
   - Integrate into daily ingest pipeline
   - Deploy Datasette with enhanced views
   - Update documentation

6. **Week 8**: Monitoring and optimization
   - Monitor performance and quality metrics
   - Optimize slow queries
   - Fine-tune matching algorithm

### Rollback Plan
- All changes are additive (no modification of existing tables)
- Can disable enrichment in ingest pipeline if issues arise
- Original employment JSON preserved for reprocessing
- Database backups available for restoration

---

## 11. Future Enhancements

### Short-Term (3-6 months)
- [ ] Add affiliation-based analytics (top institutions, country distributions)
- [ ] Implement affiliation co-authorship network graphs
- [ ] Create affiliation disambiguation tool for similar institution names
- [ ] Add bulk import for manually curated ROR mappings

### Medium-Term (6-12 months)
- [ ] Integrate with ORCID for contributor identity resolution
- [ ] Add affiliation change tracking over time
- [ ] Implement predictive matching using ML (train on manual reviews)
- [ ] Create affiliation authority file for common variants

### Long-Term (12+ months)
- [ ] Contribute affiliation corrections back to ROR community
- [ ] Integrate with OpenAlex for publication-affiliation validation
- [ ] Add support for historical affiliation name changes (ROR relationships)
- [ ] Build affiliation quality dashboard for data stewardship

---

## 12. Risk Assessment & Mitigation

### Technical Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| ROR API downtime | High | Low | Cache all responses, implement fallback to cached data |
| Rate limit exceeded | Medium | Medium | Conservative rate limiting, request batching, client ID registration |
| Poor match quality | High | Medium | Multi-tier matching, manual review queue, confidence thresholds |
| OSF employment data quality | Medium | High | Validation, normalization, robust parsing |
| Schema changes in ROR v2+ | Medium | Low | Store full ROR metadata, version tracking, periodic refresh |

### Data Quality Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Ambiguous institution names | Medium | High | Context-aware matching, manual review queue |
| Missing employment data | Low | Medium | Graceful handling, track completeness metrics |
| Incorrect matches | High | Low | Confidence thresholds, manual review, rollback capability |
| Stale ROR data | Low | Low | Periodic refresh, track ROR data releases |

### Operational Risks

| Risk | Impact | Probability | Mitigation |
|------|--------|-------------|------------|
| Long initial enrichment time | Low | High | Offline processing, progress tracking, incremental approach |
| Manual review burden | Medium | Medium | Good tooling, batch operations, prioritization by contributor count |
| Increased database size | Low | Low | Efficient schema, indexing, compression |

---

## 13. Success Criteria

### Quantitative Goals
- [ ] **Match Rate**: ≥85% of affiliations matched to ROR with confidence ≥0.7
- [ ] **Coverage**: ≥90% of contributors with affiliation data enriched
- [ ] **Performance**: Initial enrichment completes within 2 hours
- [ ] **Query Speed**: Affiliation-filtered queries return within 200ms
- [ ] **Cache Hit Rate**: ≥80% of ROR queries served from cache

### Qualitative Goals
- [ ] Datasette users can easily filter by affiliation country/type
- [ ] Manual review workflow is intuitive and efficient
- [ ] System is maintainable and well-documented
- [ ] Integration is seamless with existing pipeline
- [ ] Data quality improves over time through manual reviews

---

## 14. Resources & References

### ROR Documentation
- ROR API Docs: https://ror.readme.io/docs/rest-api
- ROR Schema v2: https://ror.readme.io/docs/schema-2-1
- ROR API v2 Announcement: https://ror.org/blog/2024-04-15-announcing-ror-v2/
- ROR Data Dump: https://ror.readme.io/docs/data-dump

### Related Technologies
- sqlite-utils: https://sqlite-utils.datasette.io/
- Datasette: https://docs.datasette.io/
- Elasticsearch Query String Syntax: https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-query-string-query.html

### Code References
- Current schema: `/home/user/psyarxivdb/osf/database.py`
- Ingestion logic: `/home/user/psyarxivdb/osf/ingestor.py`
- Harvesting logic: `/home/user/psyarxivdb/osf/harvester.py`

---

## 15. Estimated Effort

| Phase | Tasks | Estimated Time |
|-------|-------|----------------|
| **Phase 1: Infrastructure** | Schema, ROR client, caching | 3-4 days |
| **Phase 2: Data Extraction** | Employment parsing, normalization | 2-3 days |
| **Phase 3: Matching Engine** | Algorithm, confidence scoring | 4-5 days |
| **Phase 4: Database Population** | Integration, backfill | 2-3 days |
| **Phase 5: Manual Review** | Admin tools, workflow | 2-3 days |
| **Phase 6: Testing & QA** | Validation, performance testing | 2-3 days |
| **Phase 7: Documentation** | User docs, API docs, runbook | 1-2 days |
| **Total** | | **16-23 days** |

Note: This assumes one developer working part-time. Can be parallelized or compressed based on urgency.

---

## Conclusion

This plan provides a comprehensive roadmap for enriching PsyArXivDB affiliation data with standardized ROR identifiers. The approach is:

- **Incremental**: Adds new tables without modifying existing data
- **Robust**: Multi-tier matching with confidence scoring and manual review
- **Scalable**: Caching and rate limiting for efficient API usage
- **Maintainable**: Modular design with clear separation of concerns
- **Future-proof**: Stores full ROR metadata for schema evolution

The enriched affiliation data will enable powerful new queries and analytics, including filtering by institution, country, and organization type, while maintaining data quality through manual review workflows.

**Next Steps**: Begin Phase 1 implementation with infrastructure setup and ROR client development.
