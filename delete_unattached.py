#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
from asnake.logging import get_logger
from asnake.client import ASnakeClient
import csv

ap = ArgumentParser()
ap.add_argument('report', type=FileType('r'), help='Report file to work from')
ap.add_argument('--dry-run', '-n', action='store_true', help='make no changes but print what would happen')

if __name__ == '__main__':
    args = ap.parse_args()
    log = get_logger('uconn_delete_unattached')
    client = ASnakeClient()
    client.authorize()
    with args.report as reportfile:
        reader = csv.DictReader(reportfile, dialect='excel-tab')
        for row in reader:
            url = row.get('api_url')
            if args.dry_run:
                log.info(f'Dry run: checking if container at {url} exists to be deleted')
                try:
                    resp = client.get(url)
                    resp.raise_for_status()
                except:
                    log.error(f'{url} not found, status {resp.status_code}')
            else:
                try:
                    resp = client.delete(url)
                    resp.raise_for_status()
                    log.info(f'container at {url} deleted')
                except:
                    log.error(f'{url} deletion failed', status=resp.status_code, body=resp.json())
