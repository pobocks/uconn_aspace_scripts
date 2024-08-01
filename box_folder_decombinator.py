#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
from itertools import groupby
from asnake.logging import get_logger
from asnake.client import ASnakeClient

import csv

ap = ArgumentParser()
ap.add_argument('report', type=FileType('r'), help='Report file to work from')
ap.add_argument('--dry-run', '-n', action='store_true', help='make no changes but print what would happen')

def sort_fn(row):
    return (row['resource_id'], row['candidate_container_ids'],)

def fetch_existing_aos(container_id):
    uri = f'/repositories/2/top_containers/{container_id}'

    return {result['uri'].split('/')[-1]:json.loads(result['json']) for result in client.get_paged('/search', params={'q': f'top_container_uri_u_sstr:"{uri}"', 'type': ['archival_object'], 'fields': ['uri', 'json']})}

if __name__ == '__main__':
    args = ap.parse_args()
    log = get_logger('uconn_box_folder_decombinator')
    client = ASnakeClient()
    client.authorize()
    with args.report as reportfile:
        reader = csv.DictReader(reportfile, dialect='excel-tab')
        for key, group in groupby(sorted(reader, key=sort_fn), key=sort_fn):
            try:
                # we gotta do a bit of stuff with the group so turn it into a stable object
                group = list(group)
                log.debug('processing containers', ids=[row['container_record_id'] for row in group], indicators=[row['container_indicator'] for row in group])

                # if no resource_id it's attached to an accession,
                # there's one of these, we don't care
                if not key[0]:
                    log.warning('no resource_id, attached to accession')
                    continue

                group_ao_ids = {row['archival_object_id'] for row in group if row.get('archival_object_id', False)}
                group_ao_jsons = {ao_id:client.get(f'/repositories/2/archival_objects/{ao_id}').json() for ao_id in group_ao_ids}
                barcodes = {row['container_barcode'] for row in group if row['container_barcode']}
                candidate_ids = key[1].split(',') if key[1] else []
                number_of_candidates = len(candidate_ids)

                # multiple candidate keys, merge candidates into first candidate and then merge into candidate
                if  number_of_candidates > 1:
                    log.warning('multiple candidate keys', key = key)
                    candidate_aos = {}
                    for container_id in candidate_ids:
                        candidate_aos = {**candidate_aos, **fetch_existing_aos(container_id)}
                    # YOU ARE HERE

                # one candidate, fetch any existing sub containers and add these guys
                if number_of_candidates == 1:
                    log.info('merging into existing container', key=key)
                    continue

                # no candidate, create new container
                if number_of_candidates == 0:
                    log.info('creating new top container', key=key)
                    continue

            except Exception as e:
                log.error('encountered error', error=e)
                continue
