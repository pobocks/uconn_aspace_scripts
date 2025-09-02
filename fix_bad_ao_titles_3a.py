#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
from asnake.logging import get_logger
from getpass import getpass
from textwrap import dedent
from itertools import batched
import csv
import pymysql

ap = ArgumentParser()
ap.add_argument('--host', '-H', help='mysql host', default='localhost')
ap.add_argument('--port', '-p', help='mysql port', default=3306)
if __name__ == '__main__':
    args = ap.parse_args()
    conn = pymysql.connect(host=args.host, db='uconn', port=args.port, user=input('DB Username?: '), password=getpass("DB Password?: "), cursorclass=pymysql.cursors.DictCursor)
    log = get_logger('uconn_fix_bad_ao_titles')

    fixup_sql = dedent('''\
    UPDATE archival_object
    SET title = regexp_replace(title, ',+\\s*$', ''), system_mtime = NOW()
    WHERE title REGEXP ',+\\s*$' ''')

    with conn.cursor() as update_cursor:
        results = 0
        log.info('Fixing titles for archival objects with trailing commas')
        results = update_cursor.execute(fixup_sql)
        conn.commit()
        log.info("Done", records_updated=results)
