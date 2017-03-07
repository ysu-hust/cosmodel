#!/usr/bin/env python
from estimators.fit_diskservicetime import fit_diskst
from estimators.disk_bench import disk_bench
from estimators.fit_processinglatency import fit_proclat
from estimators.proclat_preprocess import proclat_pp
import argparse
import sys


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="get the device performance properties")
    subparsers = parser.add_subparsers(help='choose running mode')

    fitdst_parser = subparsers.add_parser('fitdiskst', help='fitting the distribution of disk service times')
    fitdst_parser.set_defaults(func=fit_diskst)

    benchdisk_parser = subparsers.add_parser('diskbench', help='pick n random data objects and then perform open, read metadata, read data')
    benchdisk_parser.set_defaults(func=disk_bench)

    fitproclat_parser = subparsers.add_parser('fitproclat', help='fitting the distribution of processing latency at frontend and backend tier')
    fitproclat_parser.set_defaults(func=fit_proclat)

    ppproclat_parser = subparsers.add_parser('ppproclat', help='preprocess the recorded processing latencies at frontend and backend tier before fitting')
    ppproclat_parser.set_defaults(func=fit_proclat)

    args = parser.parse_args(sys.argv[1:])
    args.func({"args":args})