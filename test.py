#!/usr/bin/env python3
"""Preflight check for the ShipBob API credentials and version.

Confirms the PAT authenticates against the pinned API version and that both
inventory endpoints return data, without writing anything to S3 or Athena.

    SHIPBOB_API_SECRET=... python3 test.py
"""
import os
import sys

sys.path.append('src/')  # updating path back to root for importing modules

from shipbob.client import API_VERSION, ShipBobClient, ShipBobError
from shipbob.extract import fetch_inventory


def main():
    token = os.getenv('SHIPBOB_API_SECRET')
    if not token:
        sys.exit('SHIPBOB_API_SECRET environment variable is not set')

    client = ShipBobClient(token)
    print(f'Checking ShipBob API version {API_VERSION}...')

    try:
        channels = client.get('channel').get('items') or []
    except ShipBobError as error:
        sys.exit(f'Authentication failed: {error}')

    for channel in channels:
        print(f"  channel {channel.get('id')}: {channel.get('name')} "
              f"scopes={','.join(channel.get('scopes') or [])}")

    inventory = fetch_inventory(client)
    print(f'\n{len(inventory)} inventory item(s), '
          f"{int(inventory['total_fulfillable_quantity'].sum())} units fulfillable")
    print(inventory.head(10).to_string(index=False))


if __name__ == '__main__':
    main()
