import argparse
import sys

from pipeline import execute_etl
from delete_log import delete_log

parser = argparse.ArgumentParser(description='ETL process for excel data')
parser.add_argument('-l', '--level', type=str, metavar='', required=True,
                    choices=['bronze', 'silver', 'gold', 'all'], help='Level of ETL (bronze, silver, gold, all)')
group = parser.add_mutually_exclusive_group()
group.add_argument('-q', '--quiet', action='store_true', help='print quiet')
group.add_argument('-v', '--verbose', action='store_true',
                   help='print verbose')
args = parser.parse_args()


if __name__ == '__main__':
    try:
        execute_etl(args.level, args.verbose)
    except Exception as e:
        sys.out.println(e.with_traceback())
    finally:
        delete_log()
