#!/usr/bin/env python3
"""Daily FinEdge price ratios -> separate R2 file. No fundamentals mutation."""
import argparse
import asyncio
import json
import logging
import math
import os
from datetime import date, datetime
from pathlib import Path
from urllib.parse import quote
from zoneinfo import ZoneInfo
import httpx

LOG = logging.getLogger('price_ratios_daily')
BASE = 'https://data.finedgeapi.com/api/v1'
IST = ZoneInfo('Asia/Kolkata')
FIELDS = ('pe', 'pb', 'ps', 'pfcf', 'ptb')


def latest_row(payload):
    if not isinstance(payload, dict) or not isinstance(payload.get('price_ratios'), list):
        raise ValueError('Invalid price_ratios response')
    valid = []
    for row in payload['price_ratios']:
        if not isinstance(row, dict):
            continue
        try:
            stamp = date.fromisoformat(str(row.get('quote_date', ''))[:10])
        except ValueError:
            continue
        if stamp > datetime.now(IST).date():
            continue
        valid.append((stamp, row))
    if not valid:
        return None
    stamp, raw = max(valid, key=lambda item: item[0])
    result = {'quote_date': stamp.isoformat()}
    for field in FIELDS:
        value = raw.get(field)
        try:
            number = float(value) if value is not None and not isinstance(value, bool) else None
        except (TypeError, ValueError):
            number = None
        result[field] = number if number is not None and math.isfinite(number) else None
    # pb and ptb are both returned in the example; preserve each without
    # assuming they are equivalent. Missing values remain null; zero is valid.
    return result


class Pipeline:
    def __init__(self, client, concurrency=4):
        self.client = client
        self.token = os.environ['FINEDGE_TOKEN']
        self.worker = os.environ['WORKER_URL'].rstrip('/')
        self.headers = {'X-Secret-Token': os.environ['WORKER_TOKEN']}
        self.sem = asyncio.Semaphore(concurrency)
        self.rate_lock = asyncio.Lock()
        self.last_request = 0
        self.errors = []

    async def read(self, name):
        r = await self.client.get(f'{self.worker}/{quote(name, safe="/")}', headers=self.headers, timeout=90)
        if r.status_code == 404:
            return None
        if r.status_code != 200:
            raise RuntimeError(f'R2 read {name}: HTTP {r.status_code}')
        return r.json()

    async def fetch(self, symbol, basis, from_year):
        async with self.sem:
            for attempt in range(5):
                async with self.rate_lock:
                    loop = asyncio.get_running_loop()
                    await asyncio.sleep(max(0, .25 - (loop.time() - self.last_request)))
                    self.last_request = loop.time()
                try:
                    r = await self.client.get(f'{BASE}/daily-price-ratios/{quote(symbol, safe="")}',
                        params={'token': self.token, 'statement_type': basis, 'from': from_year}, timeout=45)
                except httpx.RequestError:
                    # Never log request objects/URLs: token is a query parameter.
                    if attempt == 4:
                        raise RuntimeError('Network failure') from None
                    await asyncio.sleep(2 ** attempt)
                    continue
                if r.status_code in (401, 403):
                    raise PermissionError('FinEdge authentication/access rejected')
                if r.status_code == 429 or r.status_code >= 500:
                    if attempt == 4:
                        raise RuntimeError(f'API HTTP {r.status_code}')
                    await asyncio.sleep(20 if r.status_code == 429 else 2 ** attempt)
                    continue
                if r.status_code != 200:
                    raise RuntimeError(f'API HTTP {r.status_code}')
                try:
                    return latest_row(r.json())
                except (ValueError, TypeError):
                    raise RuntimeError('Invalid API response') from None

    async def stock(self, symbol, previous, preferred, from_year):
        old = previous if isinstance(previous, dict) else {}
        result = dict(old)
        result['symbol'] = symbol
        status = {}
        for basis in ('c', 's'):
            try:
                row = await self.fetch(symbol, basis, from_year)
                prior = old.get(basis)
                if row is not None:
                    if isinstance(prior, dict) and prior.get('quote_date', '') > row['quote_date']:
                        status[basis] = 'retained_newer'
                    else:
                        result[basis] = row
                        status[basis] = 'ok'
                else:
                    # Empty consolidated response is documented. Do not invent ratios.
                    status[basis] = 'empty'
            except PermissionError:
                raise
            except RuntimeError as exc:
                status[basis] = 'failed'
                self.errors.append({'symbol': symbol, 'statement_type': basis, 'error': str(exc)})
        result['fetch_status'] = status
        result['last_attempt'] = datetime.now(IST).isoformat(timespec='seconds')
        # Match the fundamentals summary's basis when available. Only fall
        # back if no value exists for that basis, and label the selected basis.
        chosen = preferred if preferred in ('c', 's') else 'c'
        if not isinstance(result.get(chosen), dict):
            chosen = 's' if chosen == 'c' else 'c'
        selected = result.get(chosen)
        result['statement_type'] = chosen if isinstance(selected, dict) else None
        result['latest'] = selected if isinstance(selected, dict) else None
        result['stale'] = not selected or selected['quote_date'] < datetime.now(IST).date().isoformat()
        return symbol, result


async def run(args):
    now = datetime.now(IST)
    holiday_file = Path(args.holidays)
    holidays = json.loads(holiday_file.read_text()) if holiday_file.exists() else []
    if not args.force and (now.weekday() >= 5 or now.date().isoformat() in holidays):
        LOG.info('Weekend/holiday: skipped')
        return
    async with httpx.AsyncClient() as client:
        pipe = Pipeline(client, args.concurrency)
        classification = await pipe.read('classification.json')
        if not isinstance(classification, list) or not classification:
            raise RuntimeError('classification.json missing/invalid')
        universe = sorted({str(r['symbol']).strip().upper() for r in classification
            if isinstance(r, dict) and r.get('symbol') and str(r.get('exchange', '')).upper() in ('NSE', 'BSE')})
        if args.symbols:
            requested = {s.strip().upper() for s in args.symbols.split(',') if s.strip()}
            unknown = requested - set(universe)
            if unknown:
                raise ValueError('Symbols outside classification universe: ' + ', '.join(sorted(unknown)))
            universe = sorted(requested)
        prior = await pipe.read(args.key) or {}
        previous = prior.get('stocks', {})
        if not isinstance(previous, dict):
            raise RuntimeError('Existing daily file has invalid stocks structure')
        summary = await pipe.read('fundamentals_summary.json') or {}
        summaries = summary.get('stocks', {}) if isinstance(summary, dict) else {}
        stocks = dict(previous) if args.symbols else {s: previous[s] for s in universe if s in previous}
        # Batches bound the number of live tasks; API request starts are paced globally.
        for offset in range(0, len(universe), 40):
            batch = universe[offset:offset+40]
            rows = await asyncio.gather(*(pipe.stock(s, previous.get(s),
                (summaries.get(s) or {}).get('stype'), args.from_year or now.year) for s in batch))
            stocks.update(rows)
            LOG.info('Processed %s/%s symbols', min(offset+40, len(universe)), len(universe))
        if universe and len(pipe.errors) == len(universe)*2:
            raise RuntimeError('All requests failed; existing R2 file left untouched')
        payload = {'schema_version': 1, 'updated_at': datetime.now(IST).isoformat(timespec='seconds'),
            'source': 'FinEdge daily-price-ratios', 'stocks': stocks,
            'run': {'symbols_attempted': len(universe), 'request_failures': len(pipe.errors), 'errors': pipe.errors}}
        out = Path(args.output)
        out.parent.mkdir(parents=True, exist_ok=True)
        data = json.dumps(payload, separators=(',', ':'), allow_nan=False)
        out.write_text(data)
        if not args.no_upload:
            r = await client.post(pipe.worker, params={'file': args.key}, headers={**pipe.headers,
                'Content-Type': 'application/json'}, content=data.encode(), timeout=90)
            if r.status_code != 200:
                raise RuntimeError(f'R2 upload: HTTP {r.status_code}')
        LOG.info('Saved %s; request failures: %s', args.key, len(pipe.errors))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--symbols', help='Optional comma-separated subset, e.g. SBIN,ITC')
    parser.add_argument('--from-year', type=int, help='API from year; default current IST year')
    parser.add_argument('--concurrency', type=int, default=4)
    parser.add_argument('--holidays', default='nse_holidays.json')
    parser.add_argument('--key', default='price_ratios_daily.json')
    parser.add_argument('--output', default='output/price_ratios_daily.json')
    parser.add_argument('--force', action='store_true', help='Run on weekend/holiday')
    parser.add_argument('--no-upload', action='store_true', help='Fetch and write locally only')
    args = parser.parse_args()
    if args.concurrency < 1:
        parser.error('--concurrency must be positive')
    logging.basicConfig(level=logging.INFO, format='%(levelname)s %(message)s')
    logging.getLogger('httpx').setLevel(logging.WARNING)
    try:
        asyncio.run(run(args))
    except Exception as exc:
        # Only safe messages; do not print HTTP tracebacks containing secrets.
        LOG.error('Run failed (%s)', type(exc).__name__)
        raise SystemExit(1) from None


if __name__ == '__main__':
    main()
