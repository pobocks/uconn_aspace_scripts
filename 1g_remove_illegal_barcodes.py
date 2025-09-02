#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
from asnake.logging import get_logger
import csv

ap = ArgumentParser()
ap.add_argument('--host', '-H', help='mysql host', default='localhost')
ap.add_argument('--port', '-p', help='mysql port', default=3306)
ap.add_argument('report', type=FileType('r'), help='Report file to work from')
ap.add_argument('--dry-run', '-n', action='store_true', help='make no changes but print what would happen')

if __name__ == '__main__':
    args = ap.parse_args()
    log = get_logger('uconn_remove_illegal_barcodes')
    client = ASnakeClient()
    client.authorize()
    top_containers = {}
    
    with open(args.report, 'rb') as report_file
        for record in DictReader(report_file):
            api_url = record['api_url']
            if api_url not in top_containers:
                # get top container in all cases
                resp = client.get(record['api_url'])
                resp.raise_for_status()
                top_containers[api_url] = (resp.json(), set(),)
            if record['sub_container_record_id'] is None:
                top_containers[api_url][1].add(record['sub_container_record_id'])

    for tc in top_containers:
        

                    
                                          
    
