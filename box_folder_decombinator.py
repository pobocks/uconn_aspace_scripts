#!/usr/bin/env python3
from argparse import ArgumentParser, FileType
from itertools import groupby
import json
from asnake.logging import get_logger
from asnake.client import ASnakeClient
from asnake.jsonmodel import JM

import csv

ap = ArgumentParser()
ap.add_argument('report', type=FileType('r'), help='Report file to work from')
ap.add_argument('--dry-run', '-n', action='store_true', help='make no changes but print what would happen')

def sort_fn(row):
    return (row['resource_id'], row['candidate_container_ids'],)

def fetch_existing_aos(container_id):
    uri = f'/repositories/2/top_containers/{container_id}'

    return {container_id:json.loads(result['json']) for result in client.get_paged('/search', params={'q': f'top_container_uri_u_sstr:"{uri}"', 'type': ['archival_object'], 'fields': ['json']})}

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

                # our SQL is grouping these by this so this should be a safe way to get the indicator
                # for candidates or new top_containers
                top_container_indicator = group[0]['container_indicator'].split(':')[0]
                top_container_uri = None
                aos_by_candidate = {}

                # multiple candidate keys, merge candidates into first candidate and set top_container_uri
                if  number_of_candidates > 1:
                    log.warning('multiple candidate keys', key = key)
                    top_container_uri = f'/repositories/2/top_containers/{candidate_ids[0]}'
                    merge_into_ref = {'ref': top_container_uri}
                    for container_id in candidate_ids:
                        aos_by_candidate.update(fetch_existing_aos(container_id))

                    victim_aos = {cand_id:aos_by_candidate[cand_id] for cand_id in candidate_ids[1:]}

                    for cand_id, ao in victim_aos.items():
                        fresh_ao = client.get(ao['uri']).json()
                        replacement_instances = []
                        for instance in fresh_ao.get('instances', []):
                            # not sub_container or not our boy, leave alone
                            if 'sub_container' not in instance or instance['sub_container']['top_container']['ref'].split('/')[-1] != cand_id:
                                replacement_instances.append(instance)
                            else:
                                instance['sub_container']['top_container'] = merge_into_ref
                                replacement_instances.append(instance)
                                fresh_ao['instances'] = replacement_instances
                        log.info('merging multiple candidates into one', candidate_uri=merge_into_ref['ref'], victims=list(victim_aos.keys()))
                        if not args.dry_run:
                            result = client.post(fresh_ao['uri'], json=fresh_ao)
                            if result.status_code != 200:
                                log.error('error merging candidates',  candidate_uri=merge_into_ref['ref'], victims=list(victim_aos.keys()), status_code=result.status_code, body=result.text)
                            else:
                                log.info('merged multiple candidates into one', candidate_uri=merge_into_ref['ref'], victims=list(victim_aos.keys()))


                # one candidate, fetch any existing sub containers and add these guys
                elif number_of_candidates == 1:
                    log.info('merging into existing container', key=key)
                    aos_by_candidate = fetch_existing_aos(candidate_ids[0])

                    top_container_uri = f'/repositories/2/top_containers/{candidate_ids[0]}'
                    merge_into_ref = {'ref': top_container_uri}


                # no candidate, create new container
                else:
                    log.info('creating new top container', key=key)
                    # aos_by_candidate is always empty bc brand new container
                    top_container_to_create = JM.top_container(indicator=top_container_indicator, type='box')

                    if not args.dry_run:
                        result = client.post('repositories/2/top_containers', json=top_container_to_create)
                        if result.status_code != 200:
                            log.error('error creating top container', top_container_indicator=top_container_indicator)
                        else:
                            top_container_uri = result.json()['uri']
                            merge_into_ref = {'ref': top_container_uri}

                # At this point we have ONE target container, which definitely exists, to merge this group into.


            except Exception as e:
                log.error('encountered error', error=e)
                continue
