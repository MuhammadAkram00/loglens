#!/usr/bin/env python3
"""Generate deterministic synthetic HTTP logs; no real users or production data."""
import argparse
from datetime import datetime, timedelta, timezone
import json
from pathlib import Path
import random


def generate(count, seed=42):
    rng = random.Random(seed)
    start = datetime(2026, 11, 1, tzinfo=timezone.utc)
    routes = ['/api/products', '/api/orders', '/api/search', '/health', '/api/cart', '/api/login']
    for index in range(count):
        moment = start + timedelta(seconds=round(index * 172800 / max(count, 1)))
        path = rng.choices(routes, weights=[30, 18, 24, 7, 14, 7])[0]
        spike = 29 <= (moment - start).total_seconds() / 3600 < 32
        status = rng.choices([200, 201, 400, 404, 500, 503],
                             weights=[80, 5, 3, 4, 15 if spike else 2, 8 if spike else 1])[0]
        duration = rng.lognormvariate(4.0, 0.6) + (450 if status >= 500 else 0)
        yield {'ts': moment.isoformat().replace('+00:00', 'Z'),
               'method': 'POST' if path in ('/api/orders', '/api/cart', '/api/login') else 'GET',
               'path': path + ('?q=demo' if path == '/api/search' else ''),
               'status': status, 'duration_ms': round(duration, 2)}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--count', type=int, default=12000)
    parser.add_argument('--seed', type=int, default=42)
    parser.add_argument('--out', default='sample.jsonl')
    args = parser.parse_args()
    if not 1 <= args.count <= 1000000:
        parser.error('--count must be between 1 and 1000000')
    with Path(args.out).open('w', encoding='utf-8') as stream:
        for record in generate(args.count, args.seed):
            stream.write(json.dumps(record) + '\n')
    print('Generated {} synthetic requests in {}'.format(args.count, args.out))


if __name__ == '__main__':
    main()
