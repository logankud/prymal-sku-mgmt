"""Daily snapshot of ShipBob inventory into prymal.shipbob_inventory_details."""
import argparse
import sys

from loguru import logger

sys.path.append('src/')  # updating path back to root for importing modules

from shipbob.jobs import build_context, local_context, run_inventory


def main():
    parser = argparse.ArgumentParser(
        description='Extract ShipBob inventory details snapshot')
    parser.add_argument(
        '--partition_date',
        type=str,
        default=None,
        help='Partition date for output in YYYY-MM-DD format '
             '(defaults to today, US/Eastern)')
    parser.add_argument(
        '--dry-run',
        metavar='DIR',
        nargs='?',
        const='dryrun',
        default=None,
        help='Extract and validate against the live API but write CSVs to DIR '
             '(default: ./dryrun) instead of S3, and skip the table repair')
    args = parser.parse_args()

    context = local_context(args.dry_run) if args.dry_run else build_context()
    written = run_inventory(partition_date=args.partition_date, **context)
    logger.info(f'Finished extracting inventory - {written} row(s) written')


if __name__ == '__main__':
    main()
