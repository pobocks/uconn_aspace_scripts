#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
import json, pymysql, re
import asnake.logging
from asnake.client import ASnakeClient
from asnake.jsonmodel import JM
from requests.exceptions import RequestException
import csv

ap = ArgumentParser()
ap.add_argument('report', type=FileType('r'), help='Report file to work from')
ap.add_argument('--dry-run', '-n', action='store_true', help='make no changes but print what would happen')
ap.add_argument('--log-config', type=str, help="log constant from asnake.logging", default='INFO_TO_STDOUT')



bad_re = re.compile('^data_value_missing.*')    
if __name__ == "__main__":
    args = ap.parse_args()
    log_config = asnake.logging.__dict__.get(args.log_config, asnake.logging.INFO_TO_STDOUT)
    asnake.logging.setup_logging(config=log_config)
    client = ASnakeClient()
    client.authorize()
    containers_to_delete = set()

    log = asnake.logging.get_logger('uconn_data_value_missing')
    log.info('begin processing')
    with args.report as reportfile:
        reader = csv.DictReader(reportfile, dialect='excel-tab')

        # note: we're skipping top containers with this issue
        records_with_problem = {row['api_url']
                                for row in reader
                                if row['problem_in_record_type'] == 'sub_container_only'}
        for record_uri in records_with_problem:
            try:
                resp = client.get(record_uri)
                resp.raise_for_status()
            except RequestException as e:
                log.error('failure fetching record', record=record_uri, exc_info=e)
                
            record = resp.json()
            record['instances'] = [i for i in record['instances'] if not bad_re.match(i.get('sub_container', {}).get('indicator_2', ''))]
                
            if not args.dry_run:
                log.info('updating instances for record', record=record_uri)
                try:
                    resp = client.post(record['uri'], json=record)
                    resp.raise_for_status()
                    log.info('updated', record=record_uri)
                except RequestException as e:
                    log.error('failure updating record', record_uri=record['uri'], exc_info=e)
                    continue
                
    log.info('finished')
