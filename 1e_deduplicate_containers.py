#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
from itertools import groupby
import json, pymysql
import asnake.logging
from asnake.client import ASnakeClient
from asnake.jsonmodel import JM
from requests.exceptions import RequestException
import csv

ap = ArgumentParser()
ap.add_argument('report', type=FileType('r'), help='Report file to work from')
ap.add_argument('--dry-run', '-n', action='store_true', help='make no changes but print what would happen')
ap.add_argument('--log-config', type=str, help="log constant from asnake.logging", default='INFO_TO_STDOUT')


def fetch_existing_aos(container_id):
    uri = f'/repositories/2/top_containers/{container_id}'

    # returns jsons
    return [json.loads(result['json'])
            for result
            in client.get_paged('/search', params={
                'q': f'top_container_uri_u_sstr:"{uri}"',
                'type': ['archival_object'],
                'fields': ['json']})]

def validate_instance(instance_json, tc_uri_to_exclude=None):
    'prevents instances attached to existing record OR messed up invalid instances from getting attached'
    # instance is marked for exclusion
    if instance_json.get('sub_container', {}).get('top_container', {}).get('ref', None) == tc_uri_to_exclude:
        log.info('excluding top container uri', tc_uri=tc_uri_to_exclude, json=instance_json)
        return False
    # instance is not a digital_object and is missing sub_container
    elif instance_json['instance_type'] != 'digital_object' and not instance_json.get('sub_container', None):
        log.warning('rejecting invalid instance', json=instance_json)
        return False
    # instance is a digital_object but is missing the digital_object field (not seen in production but theoretically possible and will fail)
    elif instance_json['instance_type'] == 'digital_object' and not instance_json.get('digital_object', None):
        log.warning('rejecting invalid instance', json=instance_json)
        return False
    else:
        return True


if __name__ == "__main__":
    args = ap.parse_args()
    log_config = asnake.logging.__dict__.get(args.log_config, asnake.logging.INFO_TO_STDOUT)
    asnake.logging.setup_logging(config=log_config)
    client = ASnakeClient()
    client.authorize()
    containers_to_delete = set()

    root_log = asnake.logging.get_logger('uconn_deduplicate_containers')
    root_log.info('begin processing')
    with args.report as reportfile:
        reader = csv.DictReader(reportfile, dialect='excel-tab')
        for row in reader:
            candidate_ids = row['tc_ids'].split(',')
            top_container_uri = f'/repositories/2/top_containers/{candidate_ids[0]}'
            log = root_log.bind(top_container_uri=top_container_uri)
            try:
                resp = client.get(top_container_uri)
                resp.raise_for_status()
            except RequestException as e:
                log.error('failure fetching container', exc_info=e)
                
            top_container_json = resp.json()
            
            linked_records = row['linked_records'].split(',')
            duplicate_tcs = [f'/repositories/2/top_containers/{dupe_id}' for dupe_id in candidate_ids[1:]]
            merge_into_ref = {'ref': top_container_uri}
            for record_uri in linked_records:
                try:
                    resp = client.get(record_uri)
                    resp.raise_for_status()
                except RequestException as e:
                    log.error('failure fetching record', exc_info=e)
                    continue
                    
                record = resp.json()
                replacement_instances = []
                for instance in record['instances']:
                    inst_ref = instance.get('sub_container', {}).get('top_container', {}).get('ref', None)
                    if inst_ref in duplicate_tcs:
                        instance['sub_container']['top_container'] = merge_into_ref                            
                    replacement_instances.append(instance)
                record['instances'] = replacement_instances
                if not args.dry_run:
                    log.info('updating instances for record', record_uri=record['uri'])
                    try:
                        resp = client.post(record['uri'], json=record)
                        resp.raise_for_status()
                    except RequestException as e:
                        log.error('failure updating record', record_uri=record['uri'], exc_info=e)
                        continue
                    
            log.info('deleting duplicate_containers', dupes=duplicate_tcs)
            if not args.dry_run:
                for dupe in duplicate_tcs:
                    try:
                        resp = client.delete(dupe)
                        resp.raise_for_status()
                        log.info('successfully deleted dupe', dupe_uri=dupe)
                    except RequestException as e:
                        log.error('failure deleting dupe', dupe_uri=dupe, exc_info=e)

    root_log.info('finished')
