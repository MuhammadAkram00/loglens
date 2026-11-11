"""Behavioral tests for LogLens; no third-party packages or network access."""
import gzip
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

from generate_sample import generate
import loglens


def record(**changes):
    return {'ts': '2026-11-01T12:00:00Z', 'method': 'GET', 'path': '/api/items',
            'status': 200, 'duration_ms': 30, **changes}


class LogLensTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='loglens-test-')
        self.root = Path(self.temp.name)
        self.db = self.root / 'test.db'
        self.connection = loglens.connect(self.db)

    def tearDown(self):
        self.connection.close()
        self.temp.cleanup()

    def source(self, rows, name='input.jsonl'):
        path = self.root / name
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows), encoding='utf-8')
        return path

    def test_timezone_normalization(self):
        row = loglens.parse_record(json.dumps(record(ts='2026-11-01T05:00:00-07:00')))
        self.assertEqual(row[0], '2026-11-01T12:00:00.000Z')
        for value in ('not-a-date', '2026-11-01T12:00:00', None):
            with self.subTest(value=value), self.assertRaises(ValueError):
                loglens.parse_record(json.dumps(record(ts=value)))

    def test_field_validation(self):
        invalid = [{'method': 'get'}, {'method': 1}, {'path': 'https://example.com/'},
                   {'path': '//example.com/'}, {'path': '/bad\npath'}, {'path': '/' * 2049},
                   {'status': True}, {'status': 99}, {'status': 600}, {'status': '200'},
                   {'duration_ms': -1}, {'duration_ms': True}, {'duration_ms': '30'},
                   {'duration_ms': float('inf')}, {'duration_ms': float('nan')},
                   {'duration_ms': 10 ** 1000}]
        for changes in invalid:
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                loglens.parse_record(json.dumps(record(**changes)))

    def test_strict_utf8_and_object_shape(self):
        for raw in (b'\xff', b'[]', b'null', b'{', json.dumps(record()).encode('utf-16')):
            with self.subTest(raw=raw), self.assertRaises(ValueError):
                loglens.parse_record(raw)

    def test_query_strings_fragments_and_extra_fields_are_not_retained(self):
        source = self.source([record(path='/api/items?token=secret#fragment', ip='192.0.2.1')])
        loglens.ingest(self.connection, source)
        rows = loglens.search(self.connection)
        self.assertEqual(rows[0]['path'], '/api/items')
        self.assertNotIn('secret', json.dumps(rows))
        self.assertNotIn('192.0.2.1', json.dumps(rows))

    def test_lenient_import_with_error_examples(self):
        source = self.source([record()])
        with source.open('a') as stream:
            stream.write('{\n' * 8)
        result = loglens.ingest(self.connection, source)
        self.assertEqual(result['inserted'], 1)
        self.assertEqual(result['invalid_lines'], 8)
        self.assertEqual(len(result['errors']), 5)
        self.assertEqual(result['errors'][0]['line'], 2)

    def test_strict_import_rolls_back_flushed_batches(self):
        loglens.ingest(self.connection, self.source([record(path='/existing')], 'existing.jsonl'))
        source = self.source([record()] * 501)
        with source.open('a') as stream:
            stream.write('invalid\n')
        with self.assertRaisesRegex(ValueError, 'Line 502'):
            loglens.ingest(self.connection, source, strict=True)
        self.assertEqual(loglens.summary(self.connection)['totals']['requests'], 1)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM imports').fetchone()[0], 1)

    def test_duplicate_file_with_different_name(self):
        source = self.source([record(), record(status=500)])
        first = loglens.ingest(self.connection, source)
        renamed = self.root / 'renamed.jsonl'
        renamed.write_bytes(source.read_bytes())
        duplicate = loglens.ingest(self.connection, renamed)
        self.assertEqual(duplicate['status'], 'duplicate')
        self.assertEqual(duplicate['import_id'], first['import_id'])
        self.assertEqual(duplicate['inserted'], 0)
        self.assertEqual(loglens.summary(self.connection)['totals']['requests'], 2)

    def test_gzip_and_plain_content_share_fingerprint(self):
        source = self.source([record()])
        compressed = self.root / 'input.jsonl.gz'
        with gzip.open(compressed, 'wb') as stream:
            stream.write(source.read_bytes())
        self.assertEqual(loglens.ingest(self.connection, compressed)['inserted'], 1)
        self.assertEqual(loglens.ingest(self.connection, source)['status'], 'duplicate')

    def test_corrupt_gzip_rolls_back(self):
        source = self.root / 'bad.gz'
        source.write_bytes(b'not gzip data')
        with self.assertRaises(OSError):
            loglens.ingest(self.connection, source)
        self.assertEqual(self.connection.execute('SELECT count(*) FROM imports').fetchone()[0], 0)

    def test_oversized_line_is_skipped_as_one_record(self):
        source = self.root / 'large.jsonl'
        source.write_bytes(b'x' * (loglens.MAX_LINE_BYTES * 2 + 10) + b'\n' +
                           json.dumps(record()).encode() + b'\n')
        result = loglens.ingest(self.connection, source)
        self.assertEqual((result['inserted'], result['invalid_lines']), (1, 1))
        self.assertEqual(result['errors'][0]['line'], 1)

    def test_metrics_and_nearest_rank_percentiles(self):
        rows = [record(duration_ms=value, status=status) for value, status in
                zip([10, 20, 30, 40, 50], [200, 200, 404, 500, 503])]
        loglens.ingest(self.connection, self.source(rows))
        data = loglens.summary(self.connection)
        total = data['totals']
        self.assertEqual((total['requests'], total['client_errors'], total['server_errors']), (5, 1, 2))
        self.assertEqual((total['avg_ms'], total['p50_ms'], total['p95_ms']), (30.0, 30.0, 50.0))
        self.assertEqual(total['error_rate_percent'], 60.0)
        self.assertEqual(total['server_error_rate_percent'], 40.0)
        self.assertEqual(data['hourly'][0]['requests'], 5)
        self.assertEqual(data['top_routes'][0]['errors'], 3)

    def test_empty_summary_and_report(self):
        data = loglens.summary(self.connection)
        self.assertEqual(data['totals']['requests'], 0)
        self.assertIsNone(data['totals']['p95_ms'])
        self.assertIn('n/a', loglens.render_report(data))
        self.assertEqual(loglens.search(self.connection), [])

    def test_time_bounds_and_literal_prefix(self):
        rows = [record(ts='2026-11-01T{}:00:00Z'.format(hour), path=path) for hour, path in
                [('10', '/api/100%'), ('11', '/api/100%'), ('12', '/api/100x')]]
        loglens.ingest(self.connection, self.source(rows))
        selected = loglens.summary(self.connection, since='2026-11-01T11:00:00Z',
                                  until='2026-11-01T12:00:00Z', path_prefix='/api/100%')
        self.assertEqual(selected['totals']['requests'], 1)
        self.assertEqual(len(loglens.search(self.connection, path_prefix='/api/100%')), 2)

    def test_search_order_and_status_filter(self):
        loglens.ingest(self.connection, self.source([record(status=500), record(status=404), record()]))
        rows = loglens.search(self.connection, min_status=400, limit=1)
        self.assertEqual(rows[0]['status'], 404)
        self.assertEqual(len(loglens.search(self.connection)), 3)

    def test_filter_validation_and_sql_like_values(self):
        for options in ({'since': 'yesterday'}, {'since': '2026-11-02T00:00:00Z',
                        'until': '2026-11-01T00:00:00Z'}, {'path_prefix': 'api'}, {'min_status': 600}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                loglens.summary(self.connection, **options)
        for limit in (0, 101):
            with self.assertRaises(ValueError):
                loglens.search(self.connection, limit=limit)
        loglens.ingest(self.connection, self.source([record(path="/api/'; DROP TABLE requests;--")]))
        self.assertEqual(loglens.summary(self.connection, path_prefix="/api/';")['totals']['requests'], 1)

    def test_report_escapes_untrusted_paths_without_recursive_substitution(self):
        loglens.ingest(self.connection, self.source([record(path='/<script>alert(1)</script>/$filters')]))
        output = loglens.render_report(loglens.summary(self.connection))
        self.assertNotIn('<script>', output)
        self.assertIn('&lt;script&gt;', output)
        self.assertIn('$filters', output)
        self.assertNotIn('ALL_RATE', output)

    def test_generator_is_deterministic(self):
        self.assertEqual(list(generate(10)), list(generate(10)))
        self.assertEqual(len(list(generate(10))), 10)
        for row in generate(10):
            loglens.parse_record(json.dumps(row))

    def test_cli_ingest_summary_and_html_report(self):
        script = str(Path(loglens.__file__).resolve())
        source = self.source([record()])
        base = [sys.executable, script, '--db', str(self.db)]
        imported = subprocess.run([*base, 'ingest', str(source)], capture_output=True, text=True)
        self.assertEqual(imported.returncode, 0, imported.stderr)
        self.assertEqual(json.loads(imported.stdout)['inserted'], 1)
        output = self.root / 'report.html'
        report = subprocess.run([*base, 'report', '--out', str(output)], capture_output=True, text=True)
        self.assertEqual(report.returncode, 0, report.stderr)
        self.assertIn('LogLens Traffic Report', output.read_text())
        invalid = subprocess.run([*base, 'summary', '--min-status', '999'], capture_output=True, text=True)
        self.assertEqual(invalid.returncode, 1)
        overwrite = subprocess.run([*base, 'report', '--out', str(self.db)], capture_output=True, text=True)
        self.assertEqual(overwrite.returncode, 1)


if __name__ == '__main__':
    unittest.main(verbosity=2)
