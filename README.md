<div align="center">

# LogLens

### HTTP logs into useful answers

**Python · SQLite · Streaming imports · Offline HTML reports**

[Quick start](#quick-start) · [Log format](#log-format) · [Commands](#commands) · [Design](#design)

</div>

---

LogLens is a local command-line analytics tool for structured HTTP access logs. Import JSONL or gzip files, identify error patterns, inspect slow response times, and generate a self-contained HTML traffic report without installing any Python packages.

The project complements a REST API portfolio with data ingestion, SQL aggregation, input validation, transaction handling, and automated tests.

## Features

- Stream logs in batches of 500; cap individual input lines at 1 MiB.
- Validate timestamps, methods, status codes, paths, and finite response times.
- Store request data and import history in an indexed SQLite database.
- Detect re-imports of identical file contents using SHA-256, even after renaming or gzip compression.
- Skip malformed lines with a summary, or roll back the entire import in strict mode.
- Calculate request volume, 4xx/5xx rates, average latency, and exact nearest-rank p50/p95 latency.
- Filter by UTC time range, literal path prefix, or minimum HTTP status.
- Export a local HTML report with hourly volume, status counts, and the ten busiest routes.
- Discard query strings, fragments, client IPs, and unrecognized JSON fields during import.

## Quick start

Requires **Python 3.9+** with its standard `sqlite3` module. No API key, external service, pip install, or server is needed. Tested locally on macOS with Python 3.9.

```sh
git clone https://github.com/MuhammadAkram00/loglens.git
cd loglens
python3 -m unittest -v

# Generate 12,000 deterministic synthetic requests over 48 hours
python3 generate_sample.py --count 12000 --out sample.jsonl

# Import, inspect, and report
python3 loglens.py ingest sample.jsonl
python3 loglens.py summary
python3 loglens.py report --out report.html
```

Open `report.html` in your browser. The report is a read-only snapshot and needs no internet connection. [sample-report.html](sample-report.html) is a checked-in example generated from synthetic data; download it to view the rendered report.

Repeating the same import returns `"status": "duplicate"` and inserts zero new requests. The generated demo includes a simulated increase in server errors during hours 29-31. All sample data is artificial.

On systems with Make, `make test` and `make demo` run these steps.

## Log format

One UTF-8 JSON object per line, with five required fields:

```json
{"ts":"2026-11-01T12:00:00Z","method":"GET","path":"/api/products?q=demo","status":200,"duration_ms":42.5}
```

| Field | Rules |
| --- | --- |
| `ts` | ISO 8601 timestamp with timezone; normalized to UTC at millisecond precision |
| `method` | 1-16 uppercase letters |
| `path` | Absolute URL path, at most 2,048 characters, no control characters; query and fragment removed |
| `status` | Integer from 100 to 599 |
| `duration_ms` | Finite number from 0 to 86,400,000; milliseconds |

This tool reads the documented structured format, **not** raw Apache/Nginx text logs. Convert other formats before import. Blank lines count as invalid records. Extra fields are ignored. Use immutable log snapshots while importing.

## Commands

The optional global `--db` flag goes **before** the command. Its default is `loglens.db` in your current directory.

```sh
# Import an uncompressed file, skipping invalid lines
python3 loglens.py --db traffic.db ingest requests.jsonl

# Abort and roll back on the first invalid line
python3 loglens.py --db traffic.db ingest requests.jsonl.gz --strict

# View import metadata and file fingerprints
python3 loglens.py --db traffic.db imports

# Summarize an inclusive start and exclusive end time
python3 loglens.py --db traffic.db summary \
  --since 2026-11-01T00:00:00Z --until 2026-11-02T00:00:00Z

# Inspect the latest server errors, with deterministic ordering
python3 loglens.py --db traffic.db search --min-status 500 --limit 10

# Export only requests whose paths start with /api
python3 loglens.py --db traffic.db report --path-prefix /api --out api-report.html
```

`summary`, `search`, and `report` all accept `--since`, `--until`, `--path-prefix`, and `--min-status`. Search limits range from 1 to 100, with a default of 20. Commands return JSON on stdout. Invalid arguments or failed operations produce a nonzero exit code and a message on stderr. The HTML report cannot overwrite the active database file.

## Metric definitions

- **Error rate:** responses with status 400-599 divided by matching requests.
- **Server error rate:** responses with status 500-599 divided by matching requests.
- **p50 and p95:** sort matching response times and select rank `ceil(p/100 * n)` (one-based). These are exact query results, not sampled estimates.
- **Hourly traffic:** group request timestamps by their UTC hour.
- **Top routes:** group by the literal HTTP method and sanitized URL path; sort by volume, then method and path. Dynamic path IDs are not merged into route templates.
- Empty datasets have zero error rates and `null` latency values (`n/a` in HTML).

## Design

```text
JSONL / gzip → validate and normalize → 500-row batches → SQLite transaction
                                                         ↓
                                   indexed filters and SQL aggregates
                                                         ↓
                                      JSON summary / search / HTML
```

`loglens.py` contains the parser, import pipeline, storage schema, filters, analytics, and CLI. `generate_sample.py` creates reproducible synthetic data. `test_loglens.py` checks the behavior through the Python interfaces and subprocess CLI.

**Import integrity:** requests and file metadata commit in one transaction. A SHA-256 digest covers the complete decompressed input, including invalid lines. A duplicate fingerprint causes the provisional import to roll back. Identical requests on different lines remain separate events. Partially overlapping files are not deduplicated; avoid importing overlapping snapshots.

**Resource tradeoffs:** ingestion holds one bounded line and a 500-record batch in application memory. SQLite manages transaction journals and query storage on disk. Exact percentiles use sorting, so report generation can be slower for very large datasets. Hourly report data grows with the number of distinct hours. This project does not claim a measured production throughput or memory bound for the whole process.

**Local data:** the SQLite database and generated report remain on your machine. Query strings and IPs are omitted, but URL paths and filenames may still contain sensitive values; use suitable log data. HTML cells escape imported values, and the report includes no external scripts or resources.

## Validation

The automated suite covers malformed input, timezone normalization, field boundaries, oversized lines, strict rollback after a flushed batch, renamed duplicates, gzip duplicates, corrupt gzip, exact metrics, time filters, literal `%` prefixes, SQL-like strings, HTML escaping, deterministic samples, and end-to-end CLI export.

## Development

An AI-assisted portfolio learning project created for Muhammad Akram. All metrics in the included report come from generated sample data, not production traffic.

## License

[MIT](LICENSE)
