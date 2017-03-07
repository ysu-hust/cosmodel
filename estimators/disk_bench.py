#!/usr/bin/env python
# /mnt/objects-4/0/001/00155908c2b1f1482b31c4793278f001
from glob import glob
import sys
import json
import random
import time
import cPickle as pickle
from xattr import getxattr, setxattr
from utils import default_config

METADATA_KEY = 'user.swift.metadata'
DATAPATH = default_config.diskbench_result_path
root = default_config.device_mountpoint
policy = default_config.policy_name


def read_metadata(fd):
    """
    Helper function to read the pickled metadata from an object file.

    :param fd: file descriptor or filename to load the metadata from

    :returns: dictionary of metadata
    """
    metadata = ''
    key = 0
    try:
        while True:
            metadata += getxattr(fd, '%s%s' % (METADATA_KEY, (key or '')))
            key += 1
    except IOError:
        pass
    return pickle.loads(metadata)


def generate_file_list(count):
    path = '%s/%s' % (root, policy)
    print path
    path_tree = {}
    if count == -1:
        i = count - 1
    else:
        i = 0
    for part in glob('%s/*' % (path)):
        suffixes = {}
        part = part.split('/')[-1]
        print part
        for suff in glob('%s/%s/*' % (path, part)):
            hashes = {}
            suff = suff.split('/')[-1]
            for h in glob('%s/%s/%s/*' % (path, part, suff)):
                h = h.split('/')[-1]
                filenames = []
                for d in glob('%s/%s/%s/%s/*' % (path, part, suff, h)):
                    filenames.append(d.split('/')[-1])
                if count == -1:
                    i = count - 1
                else:
                    i += 1
                hashes[h] = filenames
                if i > count:
                    break
            suffixes[suff] = hashes
            if i > count:
                break
        path_tree[part] = suffixes
        if i > count:
            break
        
    with open('%s/obj4_files.json' % (DATAPATH), 'w') as f:
        f.write(json.dumps(path_tree))
    return path_tree

def load_cache(count):
    cache_name = 'obj4_files.json'
    cache_path = DATAPATH
    cache_list = glob('%s/*' % (DATAPATH))
    print cache_list
    print cache_name
    for cache_n in cache_list:
        if cache_name in cache_n:
            print 'load data from file'
            with open('%s/%s' % (DATAPATH, cache_name), 'r') as f:
                path_tree = json.load(f)
            return path_tree
    print 'generate file list'
    path_tree = generate_file_list(count)
    return path_tree

def generate_access_list(path_tree, count):
    access_list = set([])
    access_lf = 'access_list_%d.log' % (count)
    for alf in glob('%s/*' % (DATAPATH)):
        if access_lf in alf:
            print 'load access list from file'
            with open('%s/%s' % (DATAPATH, access_lf), 'r') as f:
                for line in f:
                    access_list.add(line.split('\n')[0])
            return access_list
    print 'generate access list'
    while len(access_list) < count:
        # print len(access_list), count
        try:
            part = random.choice(path_tree.keys())
            suff = random.choice(path_tree[part].keys())
            h = random.choice(path_tree[part][suff].keys())
            file_name = random.choice(path_tree[part][suff][h])
            file_path = '%s/%s/%s/%s/%s/%s' % (root, policy, part, suff, h, file_name)
            # file_path = file_name
            access_list.add(file_path)
        except:
            pass
    with open('%s/%s' % (DATAPATH, access_lf), 'w') as f:
        for access_line in access_list:
            f.write('%s\n' % (access_line))
    return access_list

def record_access_time(access_list):
    print len(access_list)
    open_ts = []
    readmeta_ts = []
    readdata_ts = []
    for alf in access_list:
        st = time.time()
        f = open(alf, 'rb')
        ot = time.time() - st
        st = time.time()
        meta = read_metadata(f)
        mt = time.time() - st
        st = time.time()
        data = f.read(65536)
        dt = time.time() - st
        open_ts.append(ot)
        readmeta_ts.append(mt)
        readdata_ts.append(dt)
        f.close()
    print len(open_ts), len(readmeta_ts), len(readdata_ts)
    ts = {'pcmiss_latency':{'open_ts':open_ts, 'readmeta_ts':readmeta_ts, 'readdata_ts':readdata_ts}}
    with open('%s/fileop_latency_%d.json' % (DATAPATH, len(access_list)), 'w') as f:
        f.write(json.dumps(ts))
    return ts


def disk_bench():
    path_tree = load_cache(-1)
    access_list = generate_access_list(path_tree, default_config.count)
    ts = record_access_time(access_list)



