# -*- coding: utf-8 -*-
"""
Created on Mon Sep 19 22:40:53 2016

@author: lav
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
import json
from utils import default_config
os.environ['R_HOME'] = default_config.r_home
from utils import DistributionFitting
from utils.distribution_functions import degenerate_cdf
from utils.loaddata import load_proclat_data


def combine_data(df, level='node'):
    data_all = {}
    for c in df.columns:
        size, node, t = c.split('_')
        if level == 'node':
            key = '%s_%s' % (size, t)
        elif level == 'size':
            key = '%s_%s' % (node, t)
        elif level == 'combine':
            key = t
        else:
            continue
        data = data_all.get(key, [])
        data += list(df[c])
        data_all[key] = data
    dfa = pd.DataFrame(data_all)
    # print(dfa.describe())
    return dfa
    

def fit_proclat():
    df = load_proclat_data()
    df = combine_data(df, level='combine')
    distritest = DistributionFitting.DistFittest()
    distri = DistributionFitting.Distributions()
    mode = 'ks'
    res_ks = {}
    for k in df.columns:
        if mode == 'ks':
            res_k = distritest.ks_test(df[k].dropna())
        # print(k, res_k)
        res_ks[k] = res_k
    dfq = df.quantile([0.001*i for i in range(1001)])
    dfq['p'] = dfq.index
    for k in dfq.columns:
        if k == 'p' or 'proxy' in k:
            continue
        lambd = df[k].mean()
        d = df[k].mean()
        print "%s: %f" % (str(k), float(d))
        dfq['degenerate_%s' % (k)] = dfq[k].apply(lambda x: degenerate_cdf(x, d))
        dfq.plot(x=k, y=['p', 'degenerate_%s' % (k)], logy=False, logx=False)
    plt.show()
