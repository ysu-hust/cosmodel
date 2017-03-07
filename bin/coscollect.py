#!/usr/bin/env python
from collectors.collectors import collect_online_metrics, collect_proclat
from collectors.onlinemetrics import online_metrics
from utils import default_config
import argparse
import sys

if __name__ == "__main__":
    metrics = online_metrics(default_config.influxdb_info, default_config.ip2hostname, default_config.port2devicename)
    parser = argparse.ArgumentParser(description="collectors for online metrics")
    subparsers = parser.add_subparsers(help='choose running mode')

    metrics_parser = subparsers.add_parser('online', help='collect the online metrics')
    metrics_parser.set_defaults(func=collect_online_metrics)

    proclat_parser = subparsers.add_parser('proclat', help='collect processing latency of frontend and backend servers')
    proclat_parser.set_defaults(func=collect_proclat)

    args = parser.parse_args(sys.argv[1:])
    args.func({"args":args, "metrics":metrics})
