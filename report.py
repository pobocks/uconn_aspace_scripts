#!/usr/bin/python3
from csv import DictWriter
from argparse import ArgumentParser, FileType
from asnake.logging import get_logger
from getpass import getpass
from glob import glob
from os import makedirs
from os.path import basename
import pymysql

reports = {}
for path in glob('./sql/*.sql'):
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

def main():
    args = ap.parse_args()
    conn = pymysql.connect(db='uconn', user=input('DB Username?: '), password=getpass("DB Password?: "), cursorclass=pymysql.cursors.DictCursor)
    log = get_logger('uconn_report')
    makedirs('./output', exist_ok=True)
    for report in args.reports:
        with conn.cursor() as cur,\
             open(f'./output/{report}.tsv', 'w') as f:
            log.info('executing report {report}')
            cur.execute(reports[report])
            if first := cur.fetchone():
                writer = DictWriter(f, fieldnames=list(first.keys()), dialect='excel-tab')
                writer.writeheader()
                writer.writerow(first)
                writer.writerows(cur.fetchall())
                log.info(f'report {report} completed')
            else:
                log.info('report empty')


if __name__ == "__main__":
    main()
