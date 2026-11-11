#!/usr/bin/env python3
"""LogLens: stream structured HTTP logs into SQLite and generate local reports."""
import argparse
from datetime import datetime, timezone
import gzip
import hashlib
import html
import json
import math
from pathlib import Path
import re
import sqlite3
import sys
from string import Template
from urllib.parse import urlsplit

MAX_LINE_BYTES = 1024 * 1024


def utc_timestamp(value):
    if not isinstance(value, str):
        raise ValueError('ts must be an ISO 8601 string with a timezone')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.utcoffset() is None:
            raise ValueError('timezone required')
        return parsed.astimezone(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00', 'Z')
    except (ValueError, OverflowError) as error:
        raise ValueError('ts must be a valid ISO 8601 timestamp with a timezone') from error


def parse_record(raw):
    try:
        record = json.loads(raw.decode('utf-8') if isinstance(raw, bytes) else raw)
    except (ValueError, UnicodeError) as error:
        raise ValueError('invalid UTF-8 JSON') from error
    if not isinstance(record, dict):
        raise ValueError('record must be a JSON object')
    ts = utc_timestamp(record.get('ts'))
    method = record.get('method')
    if not isinstance(method, str) or not re.fullmatch(r'[A-Z]{1,16}', method):
        raise ValueError('method must contain 1-16 uppercase letters')
    path = record.get('path')
    if not isinstance(path, str) or not path.startswith('/') or path.startswith('//') or len(path) > 2048:
        raise ValueError('path must be an absolute URL path of at most 2048 characters')
    if any(ord(char) < 32 or ord(char) == 127 for char in path):
        raise ValueError('path must not contain control characters')
    # Do not retain query strings, fragments, client IPs, or extra JSON fields.
    path = urlsplit(path).path
    status = record.get('status')
    if type(status) is not int or not 100 <= status <= 599:
        raise ValueError('status must be an integer between 100 and 599')
    duration = record.get('duration_ms')
    if type(duration) not in (int, float) or not 0 <= duration <= 86400000 or not math.isfinite(duration):
        raise ValueError('duration_ms must be finite and between 0 and 86400000')
    return ts, method, path, status, float(duration)


def connect(database):
    connection = sqlite3.connect(database, timeout=10)
    connection.row_factory = sqlite3.Row
    connection.execute('PRAGMA foreign_keys=ON')
    connection.execute('PRAGMA journal_mode=WAL')
    connection.executescript('''
        CREATE TABLE IF NOT EXISTS imports (
            id INTEGER PRIMARY KEY, sha256 TEXT UNIQUE,
            filename TEXT NOT NULL, imported_at TEXT NOT NULL,
            valid_lines INTEGER NOT NULL DEFAULT 0, invalid_lines INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS requests (
            id INTEGER PRIMARY KEY, import_id INTEGER NOT NULL REFERENCES imports(id),
            ts TEXT NOT NULL, method TEXT NOT NULL, path TEXT NOT NULL,
            status INTEGER NOT NULL, duration_ms REAL NOT NULL
        );
        CREATE INDEX IF NOT EXISTS requests_ts ON requests(ts);
        CREATE INDEX IF NOT EXISTS requests_path ON requests(path);
        CREATE INDEX IF NOT EXISTS requests_status ON requests(status);
    ''')
    return connection


def bounded_lines(stream):
    """Yield whole-line digests without allocating an unbounded input line."""
    digest = hashlib.sha256()
    while True:
        raw = stream.readline(MAX_LINE_BYTES + 1)
        if not raw:
            break
        digest.update(raw)
        oversized = len(raw) > MAX_LINE_BYTES
        if oversized:
            while raw and not raw.endswith(b'\n'):
                raw = stream.readline(MAX_LINE_BYTES + 1)
                digest.update(raw)
            yield None, digest
        else:
            yield raw, digest


def ingest(connection, filename, strict=False):
    filename = Path(filename)
    valid = invalid = 0
    errors = []
    digest = hashlib.sha256()
    opener = gzip.open if filename.suffix.lower() == '.gz' else open
    # Records and import metadata commit together, or all roll back on failure.
    with connection:
        import_id = connection.execute(
            'INSERT INTO imports(filename,imported_at) VALUES(?,?)',
            (filename.name, utc_timestamp(datetime.now(timezone.utc).isoformat())),
        ).lastrowid
        with opener(filename, 'rb') as stream:
            batch = []
            for line_number, (raw, running_digest) in enumerate(bounded_lines(stream), 1):
                digest = running_digest
                try:
                    if raw is None:
                        raise ValueError('line exceeds 1 MiB')
                    entry = parse_record(raw)
                    batch.append((import_id, *entry))
                    valid += 1
                    if len(batch) >= 500:
                        connection.executemany(
                            'INSERT INTO requests(import_id,ts,method,path,status,duration_ms) VALUES(?,?,?,?,?,?)', batch)
                        batch.clear()
                except (ValueError, RecursionError) as error:
                    invalid += 1
                    if strict:
                        raise ValueError('Line {}: {}'.format(line_number, error)) from error
                    if len(errors) < 5:
                        errors.append({'line': line_number, 'reason': str(error)})
            if batch:
                connection.executemany(
                    'INSERT INTO requests(import_id,ts,method,path,status,duration_ms) VALUES(?,?,?,?,?,?)', batch)
        checksum = digest.hexdigest()
        previous = connection.execute('SELECT id FROM imports WHERE sha256=?', (checksum,)).fetchone()
        if previous:
            # This transaction contains only the current import, so roll back its provisional rows.
            connection.rollback()
            return {'status': 'duplicate', 'import_id': previous['id'], 'inserted': 0,
                    'valid_lines': valid, 'invalid_lines': invalid, 'errors': errors}
        connection.execute('UPDATE imports SET sha256=?,valid_lines=?,invalid_lines=? WHERE id=?',
                           (checksum, valid, invalid, import_id))
    return {'status': 'imported', 'import_id': import_id, 'inserted': valid,
            'valid_lines': valid, 'invalid_lines': invalid, 'errors': errors}


def filters(since=None, until=None, path_prefix=None, min_status=None):
    clauses, values = [], []
    if since:
        since = utc_timestamp(since)
        clauses.append('ts >= ?')
        values.append(since)
    if until:
        until = utc_timestamp(until)
        clauses.append('ts < ?')
        values.append(until)
    if since and until and since >= until:
        raise ValueError('--since must be earlier than --until')
    if path_prefix is not None:
        if not path_prefix.startswith('/'):
            raise ValueError('--path-prefix must start with /')
        clauses.append('substr(path,1,length(?))=?')
        values.extend([path_prefix, path_prefix])
    if min_status is not None:
        if not 100 <= min_status <= 599:
            raise ValueError('--min-status must be between 100 and 599')
        clauses.append('status >= ?')
        values.append(min_status)
    return (' WHERE ' + ' AND '.join(clauses) if clauses else ''), values


def summary(connection, **options):
    where, values = filters(**options)
    # A read transaction keeps every metric on the same SQLite snapshot.
    with connection:
        connection.execute('BEGIN')
        totals = dict(connection.execute('''SELECT count(*) requests,
            coalesce(sum(status>=400 AND status<500),0) client_errors,
            coalesce(sum(status>=500),0) server_errors,
            coalesce(sum(status>=400),0) errors,
            min(ts) first_request, max(ts) last_request,
            avg(duration_ms) avg_ms, max(duration_ms) max_ms
            FROM requests''' + where, values).fetchone())
        count = totals['requests']
        totals['error_rate_percent'] = round(100 * totals['errors'] / count, 2) if count else 0.0
        totals['server_error_rate_percent'] = round(100 * totals['server_errors'] / count, 2) if count else 0.0
        for field in ('avg_ms', 'max_ms'):
            totals[field] = round(totals[field], 2) if totals[field] is not None else None
        for percentile in (50, 95):
            offset = max(0, math.ceil(count * percentile / 100) - 1)
            row = connection.execute('SELECT duration_ms FROM requests' + where +
                                     ' ORDER BY duration_ms,id LIMIT 1 OFFSET ?', [*values, offset]).fetchone()
            totals['p{}_ms'.format(percentile)] = round(row[0], 2) if row else None
        statuses = [dict(row) for row in connection.execute(
            'SELECT status,count(*) requests FROM requests' + where + ' GROUP BY status ORDER BY status', values)]
        routes = [dict(row) for row in connection.execute('''SELECT method,path,count(*) requests,
            sum(status>=400) errors,round(avg(duration_ms),2) avg_ms FROM requests''' + where +
            ' GROUP BY method,path ORDER BY requests DESC,method,path LIMIT 10', values)]
        hours = [dict(row) for row in connection.execute('''SELECT substr(ts,1,13)||':00:00Z' hour,
            count(*) requests,sum(status>=500) server_errors FROM requests''' + where +
            ' GROUP BY hour ORDER BY hour', values)]
    return {'totals': totals, 'statuses': statuses, 'top_routes': routes, 'hourly': hours,
            'filters': {key: value for key, value in options.items() if value is not None}}


def search(connection, limit=20, **options):
    if not 1 <= limit <= 100:
        raise ValueError('--limit must be between 1 and 100')
    where, values = filters(**options)
    return [dict(row) for row in connection.execute(
        'SELECT ts,method,path,status,duration_ms FROM requests' + where +
        ' ORDER BY ts DESC,id DESC LIMIT ?', [*values, limit])]


def render_report(data):
    escape = lambda value: html.escape(str(value), quote=True)
    totals = data['totals']
    metric = lambda key: 'n/a' if totals[key] is None else escape(totals[key])
    max_count = max((row['requests'] for row in data['hourly']), default=1)
    bars = ''.join('<div class="bar" title="{}: {} requests, {} server errors"><i style="height:{}%"><b style="height:{}%"></b></i></div>'.format(
        escape(row['hour']), row['requests'], row['server_errors'],
        max(2, round(100 * row['requests'] / max_count, 2)),
        round(100 * row['server_errors'] / row['requests'], 2)) for row in data['hourly'])
    routes = ''.join('<tr><td><span class="method">{}</span> {}</td><td>{}</td><td>{}</td><td>{} ms</td></tr>'.format(
        escape(row['method']), escape(row['path']), row['requests'], row['errors'], row['avg_ms'])
        for row in data['top_routes'])
    statuses = ''.join('<div class="status"><span>{}</span><b>{}</b></div>'.format(
        row['status'], row['requests']) for row in data['statuses'])
    first = escape(totals['first_request'] or 'No data')
    last = escape(totals['last_request'] or 'No data')
    active_filters = escape(json.dumps(data['filters'], sort_keys=True))
    template = '''<!doctype html><html lang="en"><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>LogLens Traffic Report</title>
<style>
*{box-sizing:border-box}body{margin:0;background:#f4f6fa;color:#15223a;font:14px/1.6 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif}main{max-width:1120px;margin:auto;padding:38px 30px}header{display:flex;justify-content:space-between;align-items:center;margin-bottom:30px}.brand{font-weight:800;font-size:24px;letter-spacing:-.8px}.badge{color:#5e6e87;font-size:11px;background:#e5ebf4;padding:7px 12px;border-radius:20px}.eyebrow{color:#4669d5;font-size:10px;font-weight:800;letter-spacing:1.5px}h1{font-size:35px;letter-spacing:-1px;margin:5px 0 7px}h2{font-size:16px;margin:0 0 16px}p{color:#78859a;margin:0}.metrics{display:grid;grid-template-columns:repeat(4,1fr);gap:16px;margin:25px 0}.card{background:white;border:1px solid #e0e6ef;border-radius:13px;padding:22px}.label{color:#7d8ca3;font-size:10px;letter-spacing:1px;font-weight:700}.value{font-size:29px;font-weight:750;letter-spacing:-1px;margin:7px 0}.note{font-size:11px;color:#8a96a8}.grid{display:grid;grid-template-columns:3fr 1fr;gap:18px;margin-bottom:18px}.chart{display:flex;gap:3px;height:146px;align-items:end;padding-top:8px}.bar{flex:1;height:100%;display:flex;align-items:end;min-width:1px}.bar i{display:block;width:100%;background:#5177e6;border-radius:3px 3px 0 0;position:relative;overflow:hidden}.bar b{display:block;position:absolute;top:0;width:100%;background:#ed8c77}.axis{display:flex;justify-content:space-between;color:#8e9ab0;font-size:9px;margin-top:9px}.status{display:flex;justify-content:space-between;border-bottom:1px solid #edf0f6;padding:7px 0;font:12px ui-monospace,monospace}.status:last-child{border:0}table{width:100%;border-collapse:collapse;text-align:left;font-size:12px}th{color:#8a96a9;font-size:10px;letter-spacing:.5px;padding-bottom:11px}td{padding:11px 0;border-top:1px solid #edf0f5;overflow-wrap:anywhere}td:not(:first-child),th:not(:first-child){text-align:right}.method{display:inline-block;font:9px ui-monospace,monospace;color:#4668ba;background:#edf2ff;border-radius:4px;padding:3px 5px;margin-right:5px}footer{font-size:10px;color:#919caf;padding-top:16px}.small{font-size:11px;color:#8c98a9;margin-top:14px}@media(max-width:700px){main{padding:24px 16px}.metrics{grid-template-columns:repeat(2,1fr)}.grid{grid-template-columns:1fr}.card{padding:16px}h1{font-size:29px}}@media print{body{background:white}main{padding:0}.card{break-inside:avoid}}
</style><main><header><div class="brand">◉ LogLens</div><span class="badge">Python · SQLite · Offline report</span></header>
<div class="eyebrow">HTTP LOG ANALYTICS</div><h1>Traffic, with context.</h1><p>A local snapshot of request volume, errors, and response times.</p>
<div class="metrics">
<section class="card"><div class="label">TOTAL REQUESTS</div><div class="value">$requests</div><div class="note">Matching selected filters</div></section>
<section class="card"><div class="label">SERVER ERROR RATE</div><div class="value">$server_rate%</div><div class="note">5xx responses · $server_errors requests</div></section>
<section class="card"><div class="label">P95 RESPONSE TIME</div><div class="value">$p95 <small>ms</small></div><div class="note">Nearest-rank percentile</div></section>
<section class="card"><div class="label">AVERAGE RESPONSE TIME</div><div class="value">$average <small>ms</small></div><div class="note">4xx + 5xx error rate: $error_rate%</div></section>
</div><div class="grid"><section class="card"><h2>Request volume by hour</h2><p class="note">Blue: other responses · Coral: 5xx server errors</p><div class="chart">$bars</div><div class="axis"><span>$first</span><span>$last</span></div></section><section class="card"><h2>HTTP status codes</h2>$statuses</section></div>
<section class="card"><h2>Top routes by request volume</h2><table><thead><tr><th>METHOD AND PATH</th><th>REQUESTS</th><th>4XX + 5XX</th><th>AVG LATENCY</th></tr></thead><tbody>$routes</tbody></table><div class="small">Paths exclude query strings and fragments. Dynamic IDs remain distinct paths.</div></section>
<footer>Read-only snapshot · Times are UTC · Filters: $filters<br>Generated locally by LogLens. This report uses no external scripts, fonts, analytics, or network requests.</footer></main></html>'''
    return Template(template).substitute(
        requests=metric('requests'), server_errors=metric('server_errors'),
        error_rate=metric('error_rate_percent'), server_rate=metric('server_error_rate_percent'),
        p95=metric('p95_ms'), average=metric('avg_ms'), bars=bars,
        first=first, last=last, statuses=statuses, routes=routes, filters=active_filters)



def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--db', default='loglens.db', help='SQLite file (default: loglens.db)')
    commands = parser.add_subparsers(dest='command', required=True)
    importer = commands.add_parser('ingest', help='Import a JSONL or .jsonl.gz file')
    importer.add_argument('file')
    importer.add_argument('--strict', action='store_true', help='Roll back the import on its first invalid line')
    commands.add_parser('imports', help='List imported file metadata')
    for command in ('summary', 'search', 'report'):
        sub = commands.add_parser(command)
        sub.add_argument('--since', help='Inclusive timestamp with timezone')
        sub.add_argument('--until', help='Exclusive timestamp with timezone')
        sub.add_argument('--path-prefix', help='Literal URL path prefix')
        sub.add_argument('--min-status', type=int)
        if command == 'search':
            sub.add_argument('--limit', type=int, default=20)
        if command == 'report':
            sub.add_argument('--out', required=True, help='Output HTML file')
    args = parser.parse_args(argv)
    connection = None
    try:
        connection = connect(args.db)
        options = {key: getattr(args, key, None) for key in ('since', 'until', 'path_prefix', 'min_status')}
        if args.command == 'ingest':
            result = ingest(connection, args.file, args.strict)
        elif args.command == 'imports':
            result = [dict(row) for row in connection.execute('SELECT * FROM imports ORDER BY id')]
        elif args.command == 'search':
            result = search(connection, args.limit, **options)
        else:
            result = summary(connection, **options)
            if args.command == 'report':
                output = Path(args.out)
                if output.resolve() == Path(args.db).resolve():
                    raise ValueError('Report output must not overwrite the database')
                output.write_text(render_report(result), encoding='utf-8')
                result = {'report': str(output.resolve()), 'requests': result['totals']['requests']}
        print(json.dumps(result, indent=2, allow_nan=False))
        return 0
    except (OSError, ValueError, sqlite3.Error, EOFError) as error:
        print('LogLens: {}'.format(error), file=sys.stderr)
        return 1
    finally:
        if connection:
            connection.close()


if __name__ == '__main__':
    sys.exit(main())
