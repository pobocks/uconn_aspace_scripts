#!/usr/bin/env python3
import asnake.logging
from asnake.client import ASnakeClient

if __name__ == "__main__":
    log_config = asnake.logging.INFO_TO_STDOUT
    asnake.logging.setup_logging(config=log_config)
    log = asnake.logging.get_logger('uconn_timewalk_objectsaver')
    log.info('starting')

    client = ASnakeClient()
    client.authorize()
    log.info('authorized')
    for ao in client.get_paged('repositories/2/archival_objects'):
        log.info('trying to save', uri=ao['uri'])
        try:
            fresh = client.get(ao['uri']).json()
            resp = client.post(ao['uri'], json=fresh)
            resp.raise_for_status()
            log.info('successfully saved', uri=ao['uri'])
        except Exception as e:
            log.error('Something went wrong saving this record', uri=ao['uri'], content=resp.json(), error=e)
