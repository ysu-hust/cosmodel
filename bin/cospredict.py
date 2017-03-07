#!/usr/bin/env python
import sys
import argparse
from predictor.predictor import preprocessing_sysstatus_for_models, load_systemstatus_for_models


def preprocessing_sysstatus(args):
    input_file = args.input_file
    output_file = args.output_file
    preprocessing_sysstatus_for_models(input_file, output_file)


def loadss_log(args):
    slolatencys = args.slo_latencys
    systemstatus_files = args.system_status
    step_index = args.step_index
    proc_num = args.proc_num
    cut_start_point = args.cut_start_point
    cut_end_points_file = args.cut_end_points_file
    draw_without_accept = args.draw_without_accept
    draw_onediskop = args.draw_onediskop
    timeout_range_file = args.timeout_range_file
    workload_counts = args.workload_counts
    if len(slolatencys) != len(systemstatus_files):
        print "mismatching length of slolatency and systemstatus_files"
        return -1
    ssfiles = dict(zip(slolatencys, systemstatus_files))
    load_systemstatus_for_models(ssfiles, step_index, proc_num, cut_start_point=cut_start_point, cut_end_points_file=cut_end_points_file, draw_without_accept=draw_without_accept, draw_onediskop=draw_onediskop, timeout_range_file=timeout_range_file, workload_counts=workload_counts)


if __name__ == "__main__":
    # lambd_factor = default_config.lambd_factor
    parser = argparse.ArgumentParser(description="predict with different performance models")
    subparsers = parser.add_subparsers(help='choose running mode')

    sysstatusproc_parser = subparsers.add_parser('sysstatusproc', help='convert system status log file to pandas dataframe and save')
    sysstatusproc_parser.add_argument('-i', '--input-file', type=str,
                                      help='system status log file')
    sysstatusproc_parser.add_argument('-o', '--output-file', type=str, default='systemstatus.log.h5',
                                      help='hdf file for pandas dataframe')
    sysstatusproc_parser.set_defaults(func=preprocessing_sysstatus)

    loadss_parser = subparsers.add_parser('loadss', help='load sysstatus hdf5 files and display measured '
                                                       'and estimated slo meet percentage')
    loadss_parser.add_argument('-s', '--system-status', type=str, action='append', default=[],
                               help='system status log file')
    loadss_parser.add_argument('-p', '--period-duration', type=long, default=60*10*100,
                               help='change step duration default is 10 mins((60000 in 10 ms))')
    loadss_parser.add_argument('-l', '--slo-latencys', action='append', default=[], type=float,
                               help='the slo latency requirements list, in second')
    loadss_parser.add_argument('-i', '--step-index', type=long, default=10,
                               help='smooth the data with multiple collection, this is the smooth window')
    loadss_parser.add_argument('-n', '--proc-num', type=long, default=16,
                               help='how many object server processes, workers conf in object-server.conf')
    loadss_parser.add_argument('-b', '--cut-start-point', type=float, default=250.0,
                               help='points (1 point is 1 minute) before this start point will be omitted and not be drawn')
    loadss_parser.add_argument('-a', '--cut-end-points-file', type=str, default=None,
                               help='(Used for data clean) This is a json file, which records the end points (1 point is 5 minutes) for each figure. Points after the end point will be omitted and not be drawn. If not set this argument, data cleaning will not be performed.')
    loadss_parser.add_argument('-w', '--draw-without-accept', default=False, action='store_true',
                               help='set this flag will draw the estimated result without considering waiting time for accept()')
    loadss_parser.add_argument('-g', '--draw-onediskop', default=False, action='store_true',
                               help='set this flag will draw the estimated result with one disk operation per request')
    loadss_parser.add_argument('-o', '--timeout-range-file', default=None, type=str,
                               help='for evaluation using wikipedia trace, this json file gives the points range (1 point is 5 minutes) that timeouts occur. We set the model predicting result as 0 for the points in this range.')
    loadss_parser.add_argument('-r', '--workload-counts', default=None, type=str,
                               help='workload counts file, to draw workloads in the system prediction figure.')
    loadss_parser.set_defaults(func=loadss_log)

    args = parser.parse_args(sys.argv[1:])
    args.func(args)