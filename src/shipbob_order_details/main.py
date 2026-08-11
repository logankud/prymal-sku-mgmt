"""Daily extract of ShipBob orders into prymal.shipbob_order_details.

Orders are requested by the date ShipBob received them and written to one
partition per purchase date within that window.
"""
import argparse
import sys

from loguru import logger

sys.path.append('src/')  # updating path back to root for importing modules

from shipbob.jobs import (build_context, default_order_window, local_context,
                          run_orders)


def main():
    start_default, end_default = default_order_window()

    parser = argparse.ArgumentParser(description='Extract ShipBob order details')
    parser.add_argument(
        '--start_date',
        type=str,
        default=start_default,
        help='Extract orders received from 00:00:00 UTC on this date (YYYY-MM-DD)')
    parser.add_argument(
        '--end_date',
        type=str,
        default=end_default,
        help='Extract orders received up to 00:00:00 UTC on this date (YYYY-MM-DD)')
    parser.add_argument(
        '--dry-run',
        metavar='DIR',
        nargs='?',
        const='dryrun',
        default=None,
        help='Extract and validate against the live API but write CSVs to DIR '
             '(default: ./dryrun) instead of S3, and skip the table repair')
    args = parser.parse_args()
    logger.info(f'Args: {args}')

    context = local_context(args.dry_run) if args.dry_run else build_context()
    written = run_orders(start_date=args.start_date, end_date=args.end_date,
                         **context)
    logger.info(f'Finished extracting orders for {args.start_date}..{args.end_date} '
                f'- {written} row(s) written')


if __name__ == '__main__':
    main()
