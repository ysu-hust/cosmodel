import pandas as pd
import matplotlib.pyplot as plt
import json
import default_config


def change_devicename(device):
    ip, port = device.split(':')
    ip = ''.join(ip.split('.'))
    device_n = 'd'+ip+port
    return device_n


def calculate_system_slometrics(df):
    c = max(df.count())
    system_slostatus_key = 'slostatus_whole'
    system_slomeet_key = 'slomeetcounter_whole'
    system_sloviolate_key = 'sloviolatecounter_whole'
    df[system_slostatus_key] = [0.0] * c
    for device in default_config.devices:
        device_n = change_devicename(device)
        df[system_slomeet_key] = df[system_slomeet_key] + df['%s_slomeetcounter_proxy' % (device_n)]
        df[system_sloviolate_key] = df[system_sloviolate_key] + df['%s_sloviolatecounter_proxy' % (device_n)]
    df[system_slostatus_key] = df[system_slomeet_key] / (df[system_slomeet_key] + df[system_sloviolate_key])
    return df


def data_cleaning_basedon_configfile(df, slolatency, cut_start_point, cut_end_points_file, cleaning_all=False):
    if cut_end_points_file is None:
        return df
    # the slolatency is in millisecond
    key = str(int(slolatency))
    with open(cut_end_points_file, 'r') as f:
        end_points = json.loads(f.read())
    end_idx = end_points.get(key, None)
    if end_idx is not None:
        end_idx = int((int(end_idx) * 5 - cut_start_point) / 5.0) # we calculate timeout in 5 mins, however start_point is in 1 min and df idx is in 5 mins.
        print "end index is %d" % (end_idx)
        if cleaning_all is True:
            return df[:end_idx]
        for key in df.columns:
            if 'whole' in key:
                df[key] = df[key][:end_idx]
    return df
