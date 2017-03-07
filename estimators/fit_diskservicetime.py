# -*- coding: utf-8 -*-
"""
Created on Fri Sep 09 16:59:56 2016

@author: lav
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import os
from utils import default_config
os.environ['R_HOME'] = default_config.r_home
from utils import DistributionFitting
from utils.loaddata import load_randomwalk_data
from utils.distribution_functions import fio_params, weibull_cdf, normal_cdf, gamma_cdf


def calculate_unified_alpha(lambd_disk_open, lambd_disk_meta, lambd_disk_data, b2, alpha_open, alpha_meta, alpha_data, b2f_o, b2f_m, b2f_d):
    # print lambd_disk_open, lambd_disk_meta, lambd_disk_data, b2, alpha_open, alpha_meta, alpha_data, b2f_o, b2f_m, b2f_d
    lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
    atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)
    b2_o = b2f_o * atom_b2 / b2f_d
    sigma_o = b2_o**2 / alpha_open
    b2_m = b2f_m * atom_b2 / b2f_d
    sigma_m = b2_m**2 / alpha_meta
    b2_d = atom_b2
    sigma_d = b2_d**2 / alpha_data
    print(b2_o, b2_m, b2_d)
    sigma_sqr = (lambd_disk_open * (sigma_o**2 + b2_o**2) + lambd_disk_meta * (sigma_m**2 + b2_m**2) + lambd_disk_data * (sigma_d**2 + b2_d**2)) / lambd_disk - b2**2
    sigma = sigma_sqr**0.5
    mu = b2
    alpha = mu**2 / sigma
    print(alpha)
    return alpha


def get_latency_distribution(df, disk_id, count, readahead=False, mode='gamma'):
    total_lats = []
    for k in df.columns:
        total_lats += list(df[k])
    dft = pd.DataFrame({'total':total_lats})
    
    def check_distri(df, mode):
        lambd_disk_open = None
        lambd_disk_meta = None
        lambd_disk_data = None
        alpha_open = None
        alpha_meta = None
        alpha_data = None
        b2f_o = None
        b2f_m = None
        b2f_d = None
        distritest = DistributionFitting.DistFittest()
        distri = DistributionFitting.Distributions()
        res_ks = {}
        for k in df.columns:
            if mode is None:
                break
            if mode == 'ks':
                res_k = distritest.ks_test(df[k].dropna())
            if mode == 'weibull':
                res_k = distri.Weibull_distrfit(df[k].dropna())
        #        res_k = {'distributionType':'Weibull','shape':distri.Weib[0][0],'scale':distri.Weib[0][1]}
            if mode == 'gamma':
                res_k = distri.Gamma_distrfit(df[k].dropna())
        #        res_k = {'shape':distri.Gam[0][0],'rate':distri.Gam[0][1], 'distributionType': 'Gamma'}
                print('mean: ', res_k['shape'] / res_k['rate'], df[k].mean())
                if 'open' in k:
                    lambd_disk_open = df[k].count()
                    alpha_open = res_k['shape']
                    b2f_o = df[k].mean()
                if 'meta' in k:
                    lambd_disk_meta = df[k].count()
                    alpha_meta = res_k['shape']
                    b2f_m = df[k].mean()
                if 'data' in k:
                    lambd_disk_data = df[k].count()
                    alpha_data = res_k['shape']
                    b2f_d = df[k].mean()
            if mode == 'normal':
                res_k = distri.Normal_distrfit(df[k].dropna())
            if 'open' in k:
                print('\t', k, res_k)
            else:
                print('\t', k, res_k)
            res_ks[k] = res_k
        return (res_ks, lambd_disk_open, lambd_disk_meta, lambd_disk_data, alpha_open, alpha_meta, alpha_data, b2f_o, b2f_m, b2f_d)

    res_ks, lambd_disk_open, lambd_disk_meta, lambd_disk_data, alpha_open, alpha_meta, alpha_data, b2f_o, b2f_m, b2f_d = check_distri(df, mode)
    check_distri(dft, mode)
    b2 = dft['total'].mean()
    calculate_unified_alpha(lambd_disk_open, lambd_disk_meta, lambd_disk_data, b2, alpha_open, alpha_meta, alpha_data, b2f_o, b2f_m, b2f_d)

    return res_ks


def check_cache(cache_file, cache_path):
    from glob import glob
    cache_list = glob('%s/*' % (cache_path))
    # print cache_list
    for cache_file_path in cache_list:
        if cache_file in cache_file_path:
            return False
    return False
    

def plot_distribution(df, disk_id, count, readahead, res_ks=None):
    import seaborn as sns
    sns.set_context("paper")
    sns.set(style="ticks",
            rc={"xtick.major.size": 1,
                "ytick.major.size": 1,
                "xtick.direction":"in",
                "ytick.direction":"in",
                "font.size": 7,
                "axes.labelsize": 7,
                "axes.titlesize": 8,
                "xtick.labelsize": 6,
                "ytick.labelsize": 6,
                "legend.fontsize": 7,
                "lines.markersize": 4,
                # "lines.markeredgewidth": 25,
                # "lines.fillstyle": 'none',
                "lines.linewidth": 1,
                # "figure.autolayout": True,
                "figure.figsize": np.array([3.25, 1.4])})
    if readahead:
        cache_file = 'randomwalk_%s_%d_lat_distribution.h5.withreadahead' % (disk_id, count)
    else:
        cache_file = 'randomwalk_%s_%d_lat_distribution.h5' % (disk_id, count)
    cache_path = './tmp'
    
    if check_cache(cache_file, cache_path):
        dfq = pd.read_hdf('%s/%s' % (cache_path, cache_file))
    else:
        quantile_precision = 1000
        dfq = df.quantile([float(i) / quantile_precision for i in range(quantile_precision+1)])
        dfq.to_hdf('%s/%s' % (cache_path, cache_file), 'df')
    
    # print dfq.head()
    dfq['percentage'] = dfq.index
    df_keys = dfq.columns
    for k in df_keys:
        print(k)
        try:
            res_k = res_ks[k]
            shape = res_k['shape']
            rate = res_k['rate']
        except:
            continue
        if 'open' in k:
            dfq['measure_open'] = dfq[k]
            dfq['gamma_open'] = dfq['measure_open'].apply(lambda x: gamma_cdf(x, shape, rate))
            dfq['recorded_open'] = dfq['percentage']
        if 'readmeta' in k:
            dfq['measure_readmeta'] = dfq[k]
            dfq['gamma_readmeta'] = dfq['measure_readmeta'].apply(lambda x: gamma_cdf(x, shape, rate))
            dfq['recorded_readmeta'] = dfq['percentage']
        if 'readdata' in k:
            dfq['measure_readdata'] = dfq[k]
            dfq['gamma_readdata'] = dfq['measure_readdata'].apply(lambda x: gamma_cdf(x, shape, rate))
            dfq['recorded_readdata'] = dfq['percentage']
    
        
    figsum, axessum = plt.subplots(nrows=1, ncols=1)
    dfq.plot(label='gamma_index_lookup', style=':', ax=axessum, x='measure_open', y='gamma_open', logx=False, logy=False)
    dfq.plot(label='recorded_index_lookup', markevery=50, marker='o', ax=axessum, x='measure_open', y='recorded_open', logx=False, logy=False)
    dfq.plot(label='gamma_meta_read', style='-.', ax=axessum, x='measure_readmeta', y='gamma_readmeta', logx=False, logy=False)
    dfq.plot(label='recorded_meta_read', markevery=50, marker='*', ax=axessum, x='measure_readmeta', y='recorded_readmeta', logx=False, logy=False)
    dfq.plot(label='gamma_data_read', style='--', ax=axessum, x='measure_readdata', y='gamma_readdata', logx=False, logy=False)
    dfq.plot(label='recorded_data_read', markevery=50, marker='d', ax=axessum, x='measure_readdata', y='recorded_readdata', logx=False, logy=False)
    axessum.set_ylabel('Percentile')
    axessum.set_xlabel('Service Time (ms)')
    axessum.set_ylim([0.0, 1.10])
    plt.legend(loc='best')
    figsum.tight_layout(pad=0.17)
    figsum.savefig('measure_all_%s'%(disk_id)+'.pdf', format='pdf')
    plt.show()


def fit_diskst():
    readahead = False
    #readahead = True
    mode = 'ks'
    mode = 'gamma'
    #mode = 'weibull'
    #mode = 'normal'
    #mode = None # do not find distribution

    disk_id_list = default_config.disk_id_list
    count_list = default_config.count_list

    for disk_id in disk_id_list:
        for count in count_list:
            print(disk_id)
            print('\t(%d, without readahead, 0.0)' % (count))
            df = load_randomwalk_data(disk_id, count, readahead)
            step_size = int(df.index.size / 4)
            for i in range(df.index.size - step_size, df.index.size, step_size):
                print("\t###")
                dfs = df.iloc[i : (i+step_size)]
                print(dfs.describe())
                res_k = get_latency_distribution(dfs, disk_id, count, readahead, mode)
                
            plot_distribution(dfs, disk_id, count, readahead, res_k)