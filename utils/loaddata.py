# -*- coding: utf-8 -*-
"""
Created on Mon Aug 29 13:25:43 2016

@author: lav
"""

import pandas as pd
import json
from utils import default_config


def load_randomwalk_data(disk_id, count, readahead=False):
    if readahead:
        path = default_config.diskst_path+".withreadahead" % (disk_id, count)
    else:
        path = default_config.diskst_path % (disk_id, count)
    with open(path, 'r') as f:
        data = json.load(f)
    df = pd.DataFrame()
    for t, v in data.items():
        for opt, vv in v.items():
            key = '%s%s' % (t, opt)
            df[key] = vv
    df = df.apply(lambda x: x*1000.0)
    df = df[df>0.15]
    print(df.count())
    return df


def load_proclat_data():
    log_path = default_config.proclat_path % (default_config.proclat_file_h5)
    df = pd.read_hdf(log_path)
    df = df.apply(lambda x: x*1000)
    sizes = set([])
    nodes = set([])
    for c in df.columns:
        size, node, t = c.split('_')
        sizes.add(size)
        nodes.add(node)
    for s in sizes:
        for n in nodes:
            t = 'pp'
            k = '%s_%s_%s' % (s, n, t)
            k1 = '%s_%s_%s' % (s, n, 'proxy')
            k2 = '%s_%s_%s' % (s, n, 'obj')
            df[k] = df[k1] - df[k2]
    # print(df.describe())
    return df