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
    log = get_logger('uconn_fix_indicators')

    top_containers_to_fix = set()
    sub_containers_to_fix = set()
    instances_to_index = set()
    resources_to_index = set()
    aos_to_index = set()

    findem_query = dedent('''\
    SELECT tc.id as container_id,
       tc.indicator,
       sc.id as sub_container_id,
       i.id as instance_id,
       sc.indicator_2,
       sc.indicator_3,
       coalesce(r.id, ao.root_record_id) as resource_id,
       ao.id as archival_object_id
    FROM top_container tc
    JOIN top_container_link_rlshp tclr ON tc.id = tclr.top_container_id
    JOIN sub_container sc ON sc.id = tclr.sub_container_id
    JOIN instance i ON i.id = sc.instance_id
    LEFT JOIN resource r ON r.id = i.resource_id
    LEFT JOIN archival_object ao ON ao.id = i.archival_object_id
    WHERE indicator REGEXP '^[[:space:]]|[[:space:]:]+$'
       OR indicator_2 REGEXP '^[[:space:]]|[[:space:]]$'
       OR indicator_3 REGEXP '^[[:space:]]|[[:space:]]$'
    ORDER BY tc.id, sc.id;''')

    with conn.cursor() as find_cursor:
        find_cursor.execute(findem_query)
        for row in find_cursor.fetchall():
            top_containers_to_fix.add(row['container_id'])
            sub_containers_to_fix.add(row['sub_container_id'])
            instances_to_index.add(row['instance_id'])
            row['resource_id'] and resources_to_index.add(row['resource_id'])
            row['archival_object_id'] and aos_to_index.add(row['archival_object_id'])

    fix_top_containers = dedent('''\
    UPDATE top_container
    SET indicator = REGEXP_REPLACE(indicator, '^[[:space:]]+|[[:space:]]+$|[[:space:]:]+$', ''),
        system_mtime = NOW()
    WHERE id IN ''')

    fix_sub_containers = dedent('''\
    UPDATE sub_container
    SET indicator_2 = REGEXP_REPLACE(indicator_2, '^[[:space:]]+|[[:space:]]+$', ''),
        indicator_3 = REGEXP_REPLACE(indicator_3, '^[[:space:]]+|[[:space:]]+$', ''),
        system_mtime = NOW()
    WHERE id IN ''')

    with conn.cursor() as update_cursor:
        log.info("Fixing top container indicators")

        results = 0
        for batch in batched(top_containers_to_fix, 100):
            results += update_cursor.execute(fix_top_containers + f"({",".join(map(str, batch))})")
        conn.commit()
        log.info("Done", records_updated=results)

        results = 0
        log.info("Fixing sub containers indicators")
        for batch in batched(sub_containers_to_fix, 100):
            results += update_cursor.execute(fix_sub_containers + f"({",".join(map(str, batch))})")
        conn.commit()
        log.info("Done", records_updated=results)

        results = 0
        log.info("Setting resources to index")
        for batch in batched(resources_to_index, 100):
            results += update_cursor.execute(f'UPDATE resource SET system_mtime = NOW() WHERE id IN ({",".join(map(str, batch))})')
        conn.commit()
        log.info("Done", records_updated=results)

        results = 0
        log.info("Setting instances to index")
        for batch in batched(instances_to_index, 100):
            results += update_cursor.execute(f'UPDATE instance SET system_mtime = NOW() WHERE id IN ({",".join(map(str, batch))})')
        conn.commit()
        log.info("Done", records_updated=results)

        results = 0
        log.info("Setting archival objects to index")
        for batch in batched(aos_to_index, 100):
            results += update_cursor.execute(f'UPDATE archival_object SET system_mtime = NOW() WHERE id IN ({",".join(map(str, batch))})')
        conn.commit()
        log.info("Done", records_updated=results)
    conn.close()
