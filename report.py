#!/usr/bin/env python3
from csv import DictWriter
from argparse import ArgumentParser, FileType
from asnake.logging import get_logger
from getpass import getpass
from glob import glob
from os import makedirs
from os.path import basename
import pymysql

reports = {}
for path in sorted(glob('./sql/*.sql')):
    with(open(path) as f):
        reports[basename(path).removesuffix('.sql')] = f.read().strip()
choices = list(reports.keys())
ap = ArgumentParser(description="Script for running various reports, outputting tsv")

ap.add_argument('--reports',
                nargs='*',
                default=choices,
                required=False,
                choices=choices,
                help="Which reports to run, leave empty for all")
ap.add_argument('--dev-fields', '-d', action='store_true', help="Include fields used for development that aren't useful to staff")
ap.add_argument('--dbname', default="uconn", help="override name of database to query")
ap.add_argument('--dbuser', default="root", help="override db username")

dev_fields = {'api_url'}
def remove_dev_fields(row):
    for field in dev_fields:
        try:
            row.pop(field)
        except KeyError:
            pass
    return row

def main():
    args = ap.parse_args()
    conn = pymysql.connect(db=args.dbname, user=args.dbuser, password=getpass("DB Password?: "), cursorclass=pymysql.cursors.DictCursor)
    log = get_logger('uconn_report')

    makedirs('./output', exist_ok=True)
    for report in args.reports:
        with conn.cursor() as cur,\
             open(f'./output/{report}.tsv', 'w') as f:
            log.info(f'executing report {report}')
            cur.execute(reports[report])
            if first := cur.fetchone():
                fields = list(first.keys())
                if not args.dev_fields and 'api_url' in fields:
                    fields.remove('api_url')
                writer = DictWriter(f, fieldnames=fields, dialect='excel-tab')
                writer.writeheader()
                writer.writerow(first if args.dev_fields else remove_dev_fields(first))
                writer.writerows(cur.fetchall() if args.dev_fields else map(remove_dev_fields, cur.fetchall()))
                log.info(f'report {report} completed')
            else:
                log.info('report empty')


if __name__ == "__main__":
    main()
