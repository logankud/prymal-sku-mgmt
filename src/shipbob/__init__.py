"""ShipBob extraction for the Prymal data pipelines (API version 2026-07).

    client.py   HTTP transport: auth, version pinning, pagination, retries
    extract.py  API JSON -> the DataFrames the Glue tables expect
    load.py     validate -> CSV -> S3 -> MSCK REPAIR
    jobs.py     the two daily jobs, wired together
"""
from shipbob.client import ShipBobClient, ShipBobError

__all__ = ['ShipBobClient', 'ShipBobError']
