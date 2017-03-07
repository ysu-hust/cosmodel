import scipy
import numpy as np
import json
import pandas as pd
from utils import default_config
from utils.datacleaning import calculate_system_slometrics, data_cleaning_basedon_configfile
from cosplot import plot_estimation_res
from cosmodel import build_cosmodel
from noWTAmodel import build_nowta_model
from ODOPRmodel import build_odopr_model


def change_devicename(device):
    ip, port = device.split(':')
    ip = ''.join(ip.split('.'))
    device_n = 'd'+ip+port
    return device_n


def preprocessing_sysstatus_for_models(input_file, output_file='systemstatus.log.h5'):
    output_keys = []
    data_list = []
    last_status_collecting_time = None
    with open(input_file, 'r') as f:
        f.readline()
        for l in f:
            try:
                data_one_line = json.loads(l)
            except Exception as e:
                print str(e)
                continue
            data_one = {}
            try:
                data_one['status_collecting_time'] = data_one_line['status_collecting_time']
            except Exception as e:
                print str(e)
                continue
            if last_status_collecting_time is None:
                collect_interval = None
            else:
                collect_interval = data_one_line['status_collecting_time'] - last_status_collecting_time
            last_status_collecting_time = data_one_line['status_collecting_time']
            # get cache status for devices
            key_list = {'partitions_devices_miss_count':'_miss_count',
                        'partitions_devices_hit_count':'_hit_count',
                        'partitions_devices_metahit_count':'_metahit_count',
                        'partitions_devices_metamiss_count':'_metamiss_count',
                        'partitions_devices_openhit_count':'_openhit_count',
                        'partitions_devices_openmiss_count':'_openmiss_count',}
            device_list = set([])
            try:
                for k in key_list.keys():
                    devs = data_one_line[k].keys()
                    device_list = device_list.union(set(devs))
            except Exception as e:
                print str(e)
                continue
            # get devices slo status
            for k in ['slostatus_proxy', 'slostatus_obj', 'slomeetcounter_obj', 'slomeetcounter_proxy', 'sloviolatecounter_obj', 'sloviolatecounter_proxy']:
                for d in device_list:
                    v = data_one_line[k].get(d, 0.0)
                    d = change_devicename(d)
                    data_one[d+'_'+k] = float(v)
            slomeetcounter_whole = 0
            sloviolatecounter_whole = 0
            for d in device_list:
                m = data_one_line['slomeetcounter_proxy'].get(d, 0.0)
                slomeetcounter_whole += m
                v = data_one_line['sloviolatecounter_proxy'].get(d, 0.0)
                sloviolatecounter_whole += v
            data_one['slomeetcounter_whole'] = slomeetcounter_whole
            data_one['sloviolatecounter_whole'] = sloviolatecounter_whole
            try:
                data_one['slostatus_whole'] = slomeetcounter_whole / (slomeetcounter_whole + sloviolatecounter_whole)
            except Exception as e:
                print 'slomeetcounter_whole, sloviolatecounter_whole is :', slomeetcounter_whole, sloviolatecounter_whole
                print str(e)
                data_one['slostatus_whole'] = 0.0
            for d in device_list:
                d_n = change_devicename(d)
                for k, suffix in key_list.items():
                    data_one[d_n+suffix] = sum(data_one_line[k].get(d, {}).values())
                total_count = 0
                for suffix in key_list.values():
                    try:
                        total_count += data_one[d_n+suffix]
                    except Exception as e:
                        print str(e), "@ calculating %s_total_count" % (d_n)
                data_one[d_n+'_total_count'] = total_count
                data_one[d_n+'_totalmiss_count'] = data_one[d_n+'_miss_count'] + data_one[d_n+'_metamiss_count'] + data_one.get(d_n + '_openmiss_count', 0)
                try:
                    data_one[d_n+'_openmiss_ratio'] = float(data_one.get(d_n+'_openmiss_count', 0)) / (data_one.get(d_n+'_openmiss_count', 0) + data_one.get(d_n+'_openhit_count', 0))
                except:
                    data_one[d_n+'_openmiss_ratio'] = 0.0
                try:
                    data_one[d_n+'_metamiss_ratio'] = float(data_one[d_n+'_metamiss_count']) / (data_one[d_n+'_metamiss_count']+data_one[d_n+'_metahit_count'])
                except:
                    data_one[d_n+'_metamiss_ratio'] = 0.0
                try:
                    data_one[d_n+'_miss_ratio'] = float(data_one[d_n+'_miss_count']) / (data_one[d_n+'_miss_count']+data_one[d_n+'_hit_count'])
                except:
                    data_one[d_n+'_miss_ratio'] = 0.0
                try:
                    data_one[d_n+'_totalmiss_ratio'] = float(data_one[d_n+'_totalmiss_count']) / data_one[d_n+'_total_count']
                except Exception as e:
                    print 'totalmiss_ratio', str(e)
                    data_one[d_n+'_totalmiss_ratio'] = 0.0
                if collect_interval is None:
                    data_one[d_n+'_lowlevel_operation_rate'] = 0.0
                    data_one[d_n+'_lowlevel_diskoperation_rate'] = 0.0
                    data_one[d_n+'_lambda_open'] = 0.0
                    data_one[d_n+'_lambda_meta'] = 0.0
                    data_one[d_n+'_lambda_data'] = 0.0
                    data_one[d_n+'_lambda_proxy'] = 0.0
                else:
                    data_one[d_n+'_lowlevel_operation_rate'] = data_one[d_n+'_total_count'] / collect_interval
                    data_one[d_n+'_lowlevel_diskoperation_rate'] = data_one[d_n+'_totalmiss_count'] / collect_interval
                    data_one[d_n+'_lambda_open'] = (data_one.get(d_n+'_openmiss_count', 0) + data_one.get(d_n+'_openhit_count', 0)) / collect_interval
                    data_one[d_n+'_lambda_meta'] = (data_one[d_n+'_metamiss_count'] + data_one[d_n+'_metahit_count']) / collect_interval
                    data_one[d_n+'_lambda_data'] = (data_one[d_n+'_miss_count'] + data_one[d_n+'_hit_count']) / collect_interval
                    data_one[d_n+'_lambda_proxy'] = (data_one[d_n+'_slomeetcounter_proxy'] + data_one[d_n+'_sloviolatecounter_proxy']) / collect_interval
            total_lambda_proxy = 0.0
            for d in device_list:
                d_n = change_devicename(d)
                total_lambda_proxy += data_one[d_n+'_lambda_proxy']
            data_one['total_lambda_proxy'] = total_lambda_proxy
            # get devices workload
            for d, w in data_one_line['devices_req_count_rate'].items():
                d = change_devicename(d)
                data_one[d+'_req_count_rate'] = float(w)
            for d, w in data_one_line['devices_incoming_req_count_rate'].items():
                d = change_devicename(d)
                data_one[d+'_inreq_count_rate'] = float(w)
            for d, w in data_one_line['devices_req_read_rate'].items():
                d = change_devicename(d)
                data_one[d+'_req_read_rate'] = float(w)
            for d, w in data_one_line['devices_req_size_rate'].items():
                d = change_devicename(d)
                data_one[d+'_req_size_rate'] = float(w)
            # get devices system metrics
            for d, sm in data_one_line['devices_service_time_measure'].items():
                d = change_devicename(d)
                data_one[d+'_service_time_measure'] = float(sm)
            for d, r in data_one_line['devices_diskio_reads'].items():
                d = change_devicename(d)
                data_one[d+'_diskio_reads'] = float(r)
            for d, rk in data_one_line['devices_diskio_readkbs'].items():
                d = change_devicename(d)
                data_one[d+'_diskio_readkbs'] = float(rk)
            data_list.append(data_one)
    df = pd.DataFrame(data_list)
    print df.head()
    print df.count()
    # df.set_index('status_collecting_time')
    df.to_hdf(output_file, 'df')


def load_workload_counts(workload_counts, cut_start_idx, cut_end_points_file, slolatency):
    trace_workload = None
    cut_end_idx = None
    if cut_end_points_file is not None:
        key = str(int(slolatency))
        with open(cut_end_points_file, 'r') as f:
            end_points = json.loads(f.read())
        cut_end_idx = end_points.get(key, None)
    if cut_end_idx is not None:
        cut_end_idx = int(cut_end_idx) # we calculate timeout in 5 mins, however start_point is in 1 min and df idx is
    if workload_counts is not None:
        with open(workload_counts, 'r') as f:
            data = json.loads(f.read())
            rates = data.get('rates', None)
            if cut_end_idx is None:
                trace_workload = rates[cut_start_idx:]
            else:
                trace_workload = rates[cut_start_idx:cut_end_idx]
    return trace_workload


def load_systemstatus_for_models(ssfiles, step_idx=10, proc_num=16, cut_start_point=250.0, cut_end_points_file=None, draw_without_accept=False, draw_onediskop=False, timeout_range_file=None, workload_counts=None):
    dfs = {}
    keysets = {}
    system_status_collect_interval = 60.0 # in second
    # for different experiment, timeouts first occur at different time point, here obtain the max time point for all experiments.
    # max_x is used to make different figures have the same x-axis scale of max_x
    if cut_end_points_file is None:
        max_x = -1
    else:
        x_list = []
        with open(cut_end_points_file, 'r') as f:
            eps_config = json.load(f)
        for v in eps_config.values():
            try:
                v = int((int(v) * 5 - cut_start_point) / 5.0)
                x_list.append(v)
            except:
                pass
        max_x = max(x_list)
    print "max_x is %d" % (max_x)

    for slolatency, ssfile in ssfiles.items():
        trace_workload = load_workload_counts(workload_counts, int(cut_start_point/step_idx), cut_end_points_file, slolatency)
        # for wikipedia evaluation, timeouts occur in a time range. In this time range, the prediction result should be zero.
        timeout_range_lower = 0
        timeout_range_upper = -1
        if timeout_range_file is not None:
            with open(timeout_range_file, 'r') as f:
                timeout_range = json.load(f)
                timeout_range_slolatency = timeout_range.get(str(int(slolatency * 1000)), {})
                timeout_range_lower = int(int(timeout_range_slolatency.get('lower', 0)) * 5.0) # change into 1 point being 1 minute
                timeout_range_upper = int(int(timeout_range_slolatency.get('upper', -1)) * 5.0)
                print timeout_range_lower, timeout_range_upper
        ## for plotting figures in paper(begin)
        key_set_obj = []
        label_set_obj = []
        titles_obj = ['Storage device %d@frontend' % (i) for i in range(len(default_config.devices))]
        key_set_proxy = []
        label_set_proxy = []
        titles_proxy = ['Storage device %d@backend' % (i) for i in range(len(default_config.devices))]
        ## for plotting figures in paper(end)
        df = pd.read_hdf(ssfile)
        dfg = pd.DataFrame()
        start_idx = 0
        next_idx = start_idx + step_idx
        temp_df = df.iloc[start_idx:next_idx].reset_index(drop=True)
        while temp_df.size > 0:
            if start_idx > timeout_range_lower and start_idx < timeout_range_upper:
                temp_df['timeouts'] = 1
            else:
                temp_df['timeouts'] = -1
            dfg = dfg.append(temp_df.mean(), ignore_index=True)
            start_idx = next_idx
            next_idx = start_idx + step_idx
            temp_df = df.iloc[start_idx:next_idx].reset_index(drop=True)
        df = dfg
        if proc_num == 1:
            df = df[int(cut_start_point/step_idx):].reset_index(drop=True)
        else:
            df = df[int(cut_start_point/step_idx):].reset_index(drop=True)
        # calculate system level slo status
        df = calculate_system_slometrics(df)
        slolatency *= 1000 # from s to ms
        k = 'slo latency - %s ms' % (slolatency)
        slostatus_obj_keys = []
        slostatus_proxy_keys = []
        # used to calculate whole system slostatus with model
        estimate_slostatus_whole_key = 'estimate_slostatus_whole'
        reqrateall_whole_key = 'reqrateall_whole'
        reqratemeet_whole_key = 'reqratemeet_whole'
        estimate_slostatus_whole_nwta_key = 'estimate_slostatus_whole_nwta'
        reqrateall_whole_nwta_key = 'reqrateall_whole_nwta'
        reqratemeet_whole_nwta_key = 'reqratemeet_whole_nwta'
        estimate_slostatus_whole_odopr_key = 'estimate_slostatus_whole_odopr'
        reqrateall_whole_odopr_key = 'reqrateall_whole_odopr'
        reqratemeet_whole_odopr_key = 'reqratemeet_whole_odopr'
        system_datacleaning_idx = []

        total_lambda_proxy_lowlevel = 'total_lambda_proxy_lowlevel'
        for device in default_config.devices:
            device_n = change_devicename(device)
            dev_lambda_open_key = '%s_lambda_open' % (device_n)
            if total_lambda_proxy_lowlevel not in df.columns:
                df[total_lambda_proxy_lowlevel] = df[dev_lambda_open_key]
            else:
                df[total_lambda_proxy_lowlevel] = df[dev_lambda_open_key] + df[total_lambda_proxy_lowlevel]
        for device in default_config.devices:
            ## for plotting figures in paper(begin)
            keys_obj = []
            labels_obj = []
            keys_proxy = []
            labels_proxy = []
            ## for plotting figures in paper(end)
            device_n = change_devicename(device)
            keyset = ['%s_slostatus_obj' % (device_n), '%s_slostatus_proxy' % (device_n)]
            dev_cur_servicetm_key = '%s_service_time_measure' % (device_n)
            dev_lowlevel_diskop_key = '%s_lowlevel_diskoperation_rate' % (device_n)
            dev_lambda_open_key = '%s_lambda_open' % (device_n)
            dev_lambda_meta_key = '%s_lambda_meta' % (device_n)
            dev_lambda_data_key = '%s_lambda_data' % (device_n)
            dev_total_missratio_key = '%s_totalmiss_ratio' % (device_n)
            dev_open_missratio_key = '%s_openmiss_ratio' % (device_n)
            dev_meta_missratio_key = '%s_metamiss_ratio' % (device_n)
            dev_data_missratio_key = '%s_miss_ratio' % (device_n)
            obj_mode = 'determin'
            proxy_mode = 'determin'
            b1 = default_config.requests_processing_latency_be[device]
            bp = default_config.requests_processing_latency_fe
            npn = default_config.number_of_fe_server
            npp = default_config.number_of_processes_per_fe_server
            for model_key_type in ['cosmodel']:
                dev_model_key = '%s_%s_%d' % (device_n, '%s_%s' % (model_key_type, str(npp)), proc_num)
                df[dev_model_key] = df.apply(lambda x: eval('build_%s' % (model_key_type))(x[total_lambda_proxy_lowlevel] / 1000.0, x[dev_lambda_open_key] / 1000.0, x[dev_lambda_meta_key] / 1000.0, x[dev_lambda_data_key] / 1000.0, x[dev_total_missratio_key], x[dev_open_missratio_key], x[dev_meta_missratio_key], x[dev_data_missratio_key], bp, b1, x[dev_cur_servicetm_key], npn, npp, proc_num, device, obj_mode=obj_mode, proxy_mode=proxy_mode, timeouts=x['timeouts'], diskio=x[dev_lowlevel_diskop_key])(slolatency), axis=1)
                keyset.append(dev_model_key)
                ## for plotting figures in paper(begin)
                dev_diff_key = '%s_%s_%d_diff' % (device_n, model_key_type, proc_num)
                df[dev_diff_key] = df[dev_model_key] - df['%s_slostatus_proxy' % (device_n)]
                keys_proxy = ['%s_slostatus_proxy' % (device_n), dev_model_key, dev_diff_key]
                labels_proxy = ['measured', 'predicted']
                # draw model without accept() waiting time
                if draw_without_accept is True:
                    dev_model_nowta_key = dev_model_key + '_without_WTA'
                    df[dev_model_nowta_key] = df.apply(lambda x: eval('build_%s_model' % ('nowta'))(x[total_lambda_proxy_lowlevel] / 1000.0, x[dev_lambda_open_key] / 1000.0, x[dev_lambda_meta_key] / 1000.0, x[dev_lambda_data_key] / 1000.0, x[dev_total_missratio_key], x[dev_open_missratio_key], x[dev_meta_missratio_key], x[dev_data_missratio_key], bp, b1, x[dev_cur_servicetm_key], npn, npp, proc_num, device, obj_mode=obj_mode, proxy_mode=proxy_mode, noaccept=True, timeouts=x['timeouts'], diskio=x[dev_lowlevel_diskop_key])(slolatency), axis=1)
                    keyset.append(dev_model_nowta_key)
                    dev_diff_nowta_key = dev_diff_key + "_without_WTA"
                    df[dev_diff_nowta_key] = df[dev_model_nowta_key] - df['%s_slostatus_proxy' % (device_n)]
                    keys_proxy.extend([dev_model_nowta_key, dev_diff_nowta_key])
                    labels_proxy.append('predicted_without_WTA')
                if draw_onediskop is True:
                    dev_model_odopr_key = dev_model_key + '_without_ODOPR'
                    df[dev_model_odopr_key] = df.apply(lambda x: eval('build_%s_model' % ('odopr'))(x[total_lambda_proxy_lowlevel] / 1000.0, x[dev_lambda_open_key] / 1000.0, x[dev_lambda_meta_key] / 1000.0, x[dev_lambda_data_key] / 1000.0, x[dev_total_missratio_key], x[dev_open_missratio_key], x[dev_meta_missratio_key], x[dev_data_missratio_key], bp, b1, x[dev_cur_servicetm_key], npn, npp, proc_num, device, obj_mode=obj_mode, proxy_mode=proxy_mode, noaccept=False, timeouts=x['timeouts'], diskio=x[dev_lowlevel_diskop_key])(slolatency), axis=1)
                    keyset.append(dev_model_odopr_key)
                    dev_diff_odopr_key = dev_diff_key + "_with_ODOPR"
                    df[dev_diff_odopr_key] = df[dev_model_odopr_key] - df['%s_slostatus_proxy' % (device_n)]
                    keys_proxy.extend([dev_model_odopr_key, dev_diff_odopr_key])
                    labels_proxy.append('predicted_with_ODOPR')
            key_set_obj.append(keys_obj)
            label_set_obj.append(labels_obj)
            key_set_proxy.append(keys_proxy)
            label_set_proxy.append(labels_proxy)
            # calculate the whole system status
            if reqrateall_whole_key in df.columns:
                df[reqrateall_whole_key] = df[reqrateall_whole_key] + df[dev_lambda_open_key]
            else:
                df[reqrateall_whole_key] = df[dev_lambda_open_key]
            if reqratemeet_whole_key in df.columns:
                df[reqratemeet_whole_key] = df[reqratemeet_whole_key] + df[dev_model_key] * df[dev_lambda_open_key]
            else:
                df[reqratemeet_whole_key] = df[dev_model_key] * df[dev_lambda_open_key]
            if draw_without_accept is True:
                if reqrateall_whole_nwta_key in df.columns:
                    df[reqrateall_whole_nwta_key] = df[reqrateall_whole_nwta_key] + df[dev_lambda_open_key]
                else:
                    df[reqrateall_whole_nwta_key] = df[dev_lambda_open_key]
                if reqratemeet_whole_nwta_key in df.columns:
                    df[reqratemeet_whole_nwta_key] = df[reqratemeet_whole_nwta_key] + df[dev_model_nowta_key] * df[dev_lambda_open_key]
                else:
                    df[reqratemeet_whole_nwta_key] = df[dev_model_nowta_key] * df[dev_lambda_open_key]
            if draw_onediskop is True:
                if reqrateall_whole_odopr_key in df.columns:
                    df[reqrateall_whole_odopr_key] = df[reqrateall_whole_odopr_key] + df[dev_lambda_open_key]
                else:
                    df[reqrateall_whole_odopr_key] = df[dev_lambda_open_key]
                if reqratemeet_whole_odopr_key in df.columns:
                    df[reqratemeet_whole_odopr_key] = df[reqratemeet_whole_odopr_key] + df[dev_model_odopr_key] * df[dev_lambda_open_key]
                else:
                    df[reqratemeet_whole_odopr_key] = df[dev_model_odopr_key] * df[dev_lambda_open_key]

            keysets_k = keysets.get(k, [])
            keysets_k.append(keyset)
            keysets[k] = keysets_k
            slostatus_obj_keys.append('%s_slostatus_obj' % (device_n))
            slostatus_proxy_keys.append('%s_slostatus_proxy' % (device_n))

        df[estimate_slostatus_whole_key] = df[reqratemeet_whole_key] / df[reqrateall_whole_key]
        if draw_without_accept is True:
            df[estimate_slostatus_whole_nwta_key] = df[reqratemeet_whole_nwta_key] / df[reqrateall_whole_nwta_key]
        if draw_onediskop is True:
            df[estimate_slostatus_whole_odopr_key] = df[reqratemeet_whole_odopr_key] / df[reqrateall_whole_odopr_key]
        df = data_cleaning_basedon_configfile(df, slolatency, cut_start_point, cut_end_points_file, cleaning_all=True)
        dfs[k] = df
        keyset_k = keysets.get(k, [])
        temp_whole_keyset_base = ['slostatus_whole', estimate_slostatus_whole_key]
        if draw_without_accept is True:
            temp_whole_keyset_base.append(estimate_slostatus_whole_nwta_key)
        if draw_onediskop is True:
            temp_whole_keyset_base.append(estimate_slostatus_whole_odopr_key)
        keyset_k += [slostatus_obj_keys, slostatus_proxy_keys, temp_whole_keyset_base]
        keysets[k] = keyset_k
        titles_sys = ['System Response Latency Distribution']
        diff_sys_slo_key = 'diff_sys_slo'
        df[diff_sys_slo_key] = df[estimate_slostatus_whole_key] - df['slostatus_whole']
        temp_key_set_sys_base = ['slostatus_whole', estimate_slostatus_whole_key, diff_sys_slo_key]
        temp_label_set_sys_base = ['Observed', 'Our Model']
        if draw_without_accept is True:
            diff_sys_slo_nwta_key = 'diff_sys_slo_nwta'
            df[diff_sys_slo_nwta_key] = df[estimate_slostatus_whole_nwta_key] - df['slostatus_whole']
            temp_key_set_sys_base += [estimate_slostatus_whole_nwta_key, diff_sys_slo_nwta_key]
            temp_label_set_sys_base.append('noWTA Model')
        if draw_onediskop is True:
            diff_sys_slo_odopr_key = 'diff_sys_slo_odopr'
            df[diff_sys_slo_odopr_key] = df[estimate_slostatus_whole_odopr_key] - df['slostatus_whole']
            temp_key_set_sys_base += [estimate_slostatus_whole_odopr_key, diff_sys_slo_odopr_key]
            temp_label_set_sys_base.append('ODOPR Model')
        key_set_sys = [temp_key_set_sys_base]
        label_set_sys = [temp_label_set_sys_base]
        fig_file_sys = './%s_%s_sys' % (str(slolatency), str(proc_num))
        min_diff, max_diff, mean_diff = plot_estimation_res(df, titles_sys, key_set_sys, label_set_sys, fig_file_sys, fig_size=3.25, max_x=max_x, trace_workload=trace_workload)
        line1 = '%s,%s,%f,%f,%f' % (slolatency, 'system', min_diff, max_diff, mean_diff)
        with open('./prediction_error.csv', 'a+') as f:
            f.write(line1+"\n")