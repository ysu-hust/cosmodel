#!/usr/bin/env python
import redis
import time
from influxdb import InfluxDBClient
from utils import default_config
import math
from swift.common.ring import Ring
import numpy as np


class online_metrics(object):
    def __init__(self, influxdb_info, ip2hostname, port2devicename, mode='run', test_data=None):
        self.mode = mode
        self.test_data = test_data
        self.test_data_one_pack = {}
        self.perfect_prediction_data = None
        self.ssbench_start_time = None
        self.do_not_schedule = False
        i_ip, i_port, i_username, i_pwd, i_dbname = influxdb_info
        if self.mode == 'run':
            self.influxdb_client = InfluxDBClient(i_ip, int(i_port), i_username, i_pwd, i_dbname)
        else:
            self.influxdb_client = None
        self.redis_cli_objstatus = {}
        self.redis_cli_proxystatus = {}
        self.redis_cli_cacheserver = {}

        self.ip2hostname = ip2hostname
        self.port2devicename = port2devicename

        self.devices_incoming_req_count_rate = {}
        self.last_devices_incoming_req_count = {}
        self.devices_req_count_rate = {}
        self.last_devices_req_count = {}
        self.devices_req_size_rate = {}
        self.last_devices_req_size = {}
        self.devices_object_hit_ratio = {}
        self.devices_req_read_rate = {}
        self.last_devices_req_read = {}
        self.last_devices_get_time = None

        self.devices_diskio_reads = {}
        self.devices_diskio_readkbs = {} # actually, this is in bytes
        self.devices_service_time = {}
        self.devices_service_time_measure = {}
        self.devices_diskio_reads_toadd = {}
        self.devices_diskio_reads_toremove = {}

        # self.devices_actparts_req_count_rate = {}
        # self.last_devices_actparts_req_count = {}
        # self.devices_actparts_req_size_rate = {}
        # self.last_devices_actparts_req_size = {}
        # self.devices_actparts_req_read_rate = {}
        # self.last_devices_actparts_req_read = {}
        # self.last_devices_actparts_get_time = {}
        # self.devices_actparts_hit_ratio = {}
        # self.devices_actparts_memory_consumption = {}

        self.partitions_devices_hit_ratio = {}
        self.partitions_devices_hit_count = {}
        self.partitions_devices_miss_count = {}
        self.partitions_devices_metahit_ratio = {}
        self.partitions_devices_metahit_count = {}
        self.partitions_devices_metamiss_count = {}
        self.partitions_devices_openhit_ratio = {}
        self.partitions_devices_openhit_count = {}
        self.partitions_devices_openmiss_count = {}

        # slo related metrics
        self.slostatus_obj = {}
        self.slomeetcounter_obj = {}
        self.sloviolatecounter_obj = {}
        self.history_service_times = {}
        self.current_service_times = {}
        self.delta_service_times = {}
        self.slostatus_proxy = {}
        self.slomeetcounter_proxy = {}
        self.sloviolatecounter_proxy = {}

    def get_object_slostatus(self, redis_clients):
        slostatus = {}
        slomeetcounter = {}
        sloviolatecounter = {}
        for k in self.slostatus_obj.keys():
            slostatus[k] = 0.0
        for k in self.slomeetcounter_obj.keys():
            slomeetcounter[k] = 0.0
        for k in self.sloviolatecounter_obj.keys():
            sloviolatecounter[k] = 0.0
        for rc in redis_clients:
            slomc = rc.hgetall("slomeetcounter")
            # print slomc
            for device_name, c in slomc.items():
                slomeetcounter[device_name] = long(c)
            slovc = rc.hgetall("sloviolatecounter")
            # print slovc
            for device_name, c in slovc.items():
                sloviolatecounter[device_name] = long(c)
            rc.delete("slomeetcounter")
            rc.delete("sloviolatecounter")
        # print "slomeetcounter\t:", slomeetcounter
        # print "sloviolatecounter\t:", sloviolatecounter
        for device_name in set(slomeetcounter.keys() + sloviolatecounter.keys()):
            mc = slomeetcounter.get(device_name, 0)
            vc = sloviolatecounter.get(device_name, 0)
            tc = mc + vc
            if tc != 0:
                slomeetpercentage = float(mc) / (tc)
                slostatus[device_name] = slomeetpercentage
        return (slostatus, slomeetcounter, sloviolatecounter)

    def get_proxy_slostatus(self, redis_clients):
        slostatus = {}
        slomeetcounter = {}
        sloviolatecounter = {}
        for k in self.slostatus_proxy.keys():
            slostatus[k] = 0.0
        for k in self.slomeetcounter_proxy.keys():
            slomeetcounter[k] = 0.0
        for k in self.sloviolatecounter_proxy.keys():
            sloviolatecounter[k] = 0.0
        for rc in redis_clients:
            slomc = rc.hgetall("slomeetcounter")
            # print slomc
            for device_name, c in slomc.items():
                dc = long(slomeetcounter.get(device_name, 0))
                slomeetcounter[device_name] = long(c) + dc
            slovc = rc.hgetall("sloviolatecounter")
            # print slovc
            for device_name, c in slovc.items():
                vc = long(sloviolatecounter.get(device_name, 0))
                sloviolatecounter[device_name] = long(c) + vc
            rc.delete("slomeetcounter")
            rc.delete("sloviolatecounter")
        # print "slomeetcounter\t:", slomeetcounter
        # print "sloviolatecounter\t:", sloviolatecounter
        for device_name in set(slomeetcounter.keys() + sloviolatecounter.keys()):
            mc = slomeetcounter.get(device_name, 0)
            vc = sloviolatecounter.get(device_name, 0)
            tc = mc + vc
            if tc != 0:
                slomeetpercentage = float(mc) / (tc)
                slostatus[device_name] = slomeetpercentage
        return (slostatus, slomeetcounter, sloviolatecounter)

    def update_devices_workload(self, devices):
        if self.mode == 'test':
            # print "update_devices_workload", self.test_data_one_pack.keys()
            self.devices_incoming_req_count_rate = self.test_data_one_pack['devices_incoming_req_count_rate']
            self.devices_req_count_rate = self.test_data_one_pack['devices_req_count_rate']
            self.devices_req_size_rate = self.test_data_one_pack['devices_req_size_rate']
            self.devices_req_read_rate = self.test_data_one_pack['devices_req_read_rate']
            self.devices_object_hit_ratio = self.test_data_one_pack['devices_object_hit_ratio']
            self.slostatus_obj = self.test_data_one_pack['slostatus_obj']
            self.slomeetcounter_obj = self.test_data_one_pack['slomeetcounter_obj']
            self.sloviolatecounter_obj = self.test_data_one_pack['sloviolatecounter_obj']
            self.devices_diskio_reads = self.test_data_one_pack['devices_diskio_reads']
            self.devices_diskio_readkbs = self.test_data_one_pack['devices_diskio_readkbs']
            self.devices_service_time_measure = self.test_data_one_pack['devices_service_time_measure']
            self.devices_req_read_rate_share = {}
            req_read_rate_all_devices = sum(self.devices_req_read_rate.values())
            for device, rate in self.devices_req_read_rate.items():
                if req_read_rate_all_devices > 0:
                    self.devices_req_read_rate_share[device] = rate / float(req_read_rate_all_devices)
                else:
                    self.devices_req_read_rate_share[device] = 0.0
            return
        obj_ips = set([])
        for device in devices:
            obj_ip, obj_port = device.split(":")
            obj_ips.add(obj_ip)
        # one obj server machine may contains multiple devices
        # one device is identified by ip:port
        # get current diskio status
        # [reference](https://docs.influxdata.com/influxdb/v0.10/query_language/functions/)
        # cmd example:
        # select mean(value) from "de11.diskinfo.reads.sdb" where time >= '2016-05-21T16:38:35Z' AND  time <= '2016-05-21T16:38:36Z'

        redis_clients_obj = [] # for get slo status
        # get current requests status
        get_time = time.time()
        for obj_ip in obj_ips:
            # print obj_ip
            redis_c_key = ":".join([obj_ip, "6379", "0"])
            try:
                redis_c = self.redis_cli_objstatus[redis_c_key]
            except:
                redis_c = redis.StrictRedis(host=obj_ip, port=6379, db=0)
                self.redis_cli_objstatus[redis_c_key] = redis_c
            redis_clients_obj.append(redis_c)
            incoming_req_counts = redis_c.hgetall("incoming_req_count")
            return_req_counts = redis_c.hgetall("return_req_count")
            # print return_req_counts
            return_req_sizes = redis_c.hgetall("return_req_size")
            req_read_counts = redis_c.hgetall("readcounts")
            # print return_req_sizes
            for device in return_req_counts.keys():
                last_incoming_req_count = self.last_devices_incoming_req_count.get(device, None)
                last_req_count = self.last_devices_req_count.get(device, None)
                last_req_size = self.last_devices_req_size.get(device, None)
                last_req_read = self.last_devices_req_read.get(device, None)
                incoming_req_count = long(incoming_req_counts[device])
                req_count = long(return_req_counts[device])
                req_size = long(return_req_sizes[device])
                req_read = long(req_read_counts[device])
                if None not in (last_incoming_req_count, last_req_count, last_req_size, last_req_read, self.last_devices_get_time):
                    duration = get_time - self.last_devices_get_time
                    # print incoming_req_count, last_incoming_req_count, duration
                    incoming_req_count_rate = (incoming_req_count - last_incoming_req_count) / duration
                    req_count_rate = (req_count - last_req_count) / duration
                    req_size_rate = (req_size - last_req_size) / duration
                    req_read_rate = (req_read - last_req_read) / duration
                    self.devices_incoming_req_count_rate[device] = incoming_req_count_rate
                    self.devices_req_count_rate[device] = req_count_rate
                    self.devices_req_size_rate[device] = req_size_rate
                    self.devices_req_read_rate[device] = req_read_rate
                    diskio_reads = self.devices_diskio_reads[device]
                    if req_read_rate > 0 and req_read_rate >= diskio_reads:
                        self.devices_object_hit_ratio[device] = (req_read_rate - diskio_reads) / req_count_rate
                    else:
                        self.devices_object_hit_ratio[device] = 0.0
                self.last_devices_incoming_req_count[device] = incoming_req_count
                self.last_devices_req_count[device] = req_count
                self.last_devices_req_size[device] = req_size
                self.last_devices_req_read[device] = req_read
        self.last_devices_get_time = get_time
        self.devices_diskio_reads_toadd = {}
        self.devices_diskio_reads_toremove = {}

        # update devices slo status
        slostatus_obj, slomeetcounter_obj, sloviolatecounter_obj = self.get_object_slostatus(redis_clients_obj)
        self.slostatus_obj.update(slostatus_obj)
        self.slomeetcounter_obj.update(slomeetcounter_obj)
        self.sloviolatecounter_obj.update(sloviolatecounter_obj)

        # update devices status
        def get_data_from_influxdb(field, t = "mean"):
            smooth_interval = default_config.schedule_interval # in seconds
            smooth_t = time.gmtime(time.time() - smooth_interval)
            timestamp = time.strftime('%Y-%m-%dT%H:%M:%SZ', smooth_t)
            query_cmd = "select %s(value) from \"%s\" where time >= '%s'" % (t, field, timestamp)
            results = self.influxdb_client.query(query_cmd)
            data = None
            for res_list in results:
                for res in res_list:
                    data = float(res.get(t))
            return data

        for device in devices:
            obj_ip, obj_port = device.split(":")
            obj_hostname = self.ip2hostname[obj_ip]
            devicename = self.port2devicename[obj_port]
            field_reads = "%s.diskinfo.reads.%s" % (obj_hostname, devicename)
            diskio_reads = get_data_from_influxdb(field_reads)
            if diskio_reads is None:
                diskio_reads = self.devices_diskio_reads.get(device, 0.0)
                self.devices_diskio_reads[device] = diskio_reads
            else:
                self.devices_diskio_reads[device] = diskio_reads
            field_readkbs = "%s.diskinfo.readkbs.%s" % (obj_hostname, devicename)
            diskio_readkbs = get_data_from_influxdb(field_readkbs)
            if diskio_readkbs is None:
                diskio_readkbs = self.devices_diskio_readkbs.get(device, 0.0)
                self.devices_diskio_readkbs[device] = diskio_readkbs
            else:
                self.devices_diskio_readkbs[device] = diskio_readkbs * 1024
            field_service_time = "%s.diskinfo.time.%s" % (obj_hostname, devicename)
            service_time = float(get_data_from_influxdb(field_service_time, "mean"))
            if service_time is None:
                service_time = self.devices_service_time_measure.get(device, 0.0)
                self.devices_service_time_measure[device] = str(service_time)
            else:
                self.devices_service_time_measure[device] = str(service_time)
            # service time smooth
            device_history_service_times = self.history_service_times.get(device, [])
            device_history_service_times.append(service_time)
            device_current_service_time = self.current_service_times.get(device, service_time)
            change_service_time = True
            for h_service_time in device_history_service_times:
                if h_service_time == device_current_service_time:
                    change_service_time = False
            if change_service_time and len(device_history_service_times) == 3:
                max_hst = max(device_history_service_times)
                min_hst = min(device_history_service_times)
                device_current_service_time = sum(device_history_service_times) - max_hst - min_hst
            service_time = device_current_service_time
            self.current_service_times[device] = device_current_service_time
            if len(device_history_service_times) > 2:
                device_history_service_times.pop(0)
            self.history_service_times[device] = device_history_service_times
            # service time correction
            device_delta_service_times = self.delta_service_times.get(device, [])
            slo_metrics = default_config.slo_metrics
            time_slo = slo_metrics['latency']
            try:
                lambd = self.devices_incoming_req_count_rate.get(device, 0)
                theta = float(diskio_reads) / float(lambd)
                b2 = service_time
                t = time_slo
                p = self.slostatus_obj.get(device, -1.0)
                current_delta_service_time = find_deltab2(lambd, theta, b2, t, p)
                device_delta_service_times.append(current_delta_service_time)
                delta_st = sum(device_delta_service_times)/len(device_delta_service_times)
                service_time = service_time + delta_st
                if len(device_delta_service_times) > 1:
                    device_delta_service_times.pop(0)
                self.delta_service_times[device] = device_delta_service_times
            except:
                pass
        # update devices_req_read_rate_share
        self.devices_req_read_rate_share = {}
        req_read_rate_all_devices = sum(self.devices_req_read_rate.values())
        for device, rate in self.devices_req_read_rate.items():
            if req_read_rate_all_devices > 0:
                self.devices_req_read_rate_share[device] = rate / float(req_read_rate_all_devices)
            else:
                self.devices_req_read_rate_share[device] = 0.0

    def update_proxy_slostatus(self, proxy_servers):
        if self.mode == 'test':
            self.slostatus_proxy = self.test_data_one_pack["slostatus_proxy"]
            self.slomeetcounter_proxy = self.test_data_one_pack["slomeetcounter_proxy"]
            self.sloviolatecounter_proxy = self.test_data_one_pack["sloviolatecounter_proxy"]
            return
        redis_clients_proxy = []
        for proxy_server in proxy_servers:
            redis_c_key = ":".join([proxy_server, "6379", "0"])
            try:
                redis_c = self.redis_cli_proxystatus[redis_c_key]
            except:
                redis_c = redis.StrictRedis(host=proxy_server, port=6379, db=0)
                self.redis_cli_proxystatus[redis_c_key] = redis_c
            redis_clients_proxy.append(redis_c)
        # update devices slo status as proxy server
        slostatus_proxy, slomeetcounter_proxy, sloviolatecounter_proxy = self.get_proxy_slostatus(redis_clients_proxy)
        self.slostatus_proxy.update(slostatus_proxy)
        self.slomeetcounter_proxy.update(slomeetcounter_proxy)
        self.sloviolatecounter_proxy.update(sloviolatecounter_proxy)


    def update_partitions_devices_hit_ratio(self, device):
        if self.mode == 'test':
            self.partitions_devices_hit_ratio = self.test_data_one_pack["partitions_devices_hit_ratio"]
            self.partitions_devices_hit_count = self.test_data_one_pack.get("partitions_devices_hit_count", {})
            self.partitions_devices_miss_count = self.test_data_one_pack.get("partitions_devices_miss_count", {})
            self.partitions_devices_metahit_ratio = self.test_data_one_pack.get("partitions_devices_metahit_ratio", {})
            self.partitions_devices_metahit_count = self.test_data_one_pack.get("partitions_devices_metahit_count", {})
            self.partitions_devices_metamiss_count = self.test_data_one_pack.get("partitions_devices_metamiss_count", {})
            self.partitions_devices_openhit_ratio = self.test_data_one_pack.get("partitions_devices_openhit_ratio", {})
            self.partitions_devices_openhit_count = self.test_data_one_pack.get("partitions_devices_openhit_count", {})
            self.partitions_devices_openmiss_count = self.test_data_one_pack.get("partitions_devices_openmiss_count", {})
            return
        object_ip = device.split(":")[0]
        redis_port = 6379
        redis_c_key = ":".join([object_ip, str(redis_port), "0"])
        try:
            redis_c = self.redis_cli_objstatus[redis_c_key]
        except:
            redis_c = redis.StrictRedis(host=object_ip, port=redis_port, db=0)
            self.redis_cli_objstatus[redis_c_key] = redis_c
        pagecache_miss_key = "%s_partitions_pagecache_miss" % (device)
        pagecache_hit_key = "%s_partitions_pagecache_hit" % (device)
        meta_miss_key = "%s_partitions_meta_miss" % (device)
        meta_hit_key = "%s_partitions_meta_hit" % (device)
        open_miss_key = "%s_partitions_open_miss" % (device)
        open_hit_key = "%s_partitions_open_hit" % (device)
        try:
            pagecache_miss_counts = redis_c.hgetall(pagecache_miss_key)
            redis_c.delete(pagecache_miss_key)
            pagecache_hit_counts = redis_c.hgetall(pagecache_hit_key)
            redis_c.delete(pagecache_hit_key)
            meta_miss_counts = redis_c.hgetall(meta_miss_key)
            redis_c.delete(meta_miss_key)
            meta_hit_counts = redis_c.hgetall(meta_hit_key)
            redis_c.delete(meta_hit_key)
            open_miss_counts = redis_c.hgetall(open_miss_key)
            redis_c.delete(open_miss_key)
            open_hit_counts = redis_c.hgetall(open_hit_key)
            redis_c.delete(open_hit_key)
        except Exception as e:
            print "in update_partitions_devices_hit_ratio :"
            print str(e)
            pagecache_miss_counts = {}
            pagecache_hit_counts = {}
            meta_miss_counts = {}
            meta_hit_counts = {}
            open_miss_counts = {}
            open_hit_counts = {}
        partitions = set(pagecache_miss_counts.keys() + pagecache_hit_counts.keys() + meta_miss_counts.keys() + meta_hit_counts.keys())
        partitions_hit_ratio = {}
        partitions_hit_counts = {}
        partitions_miss_counts = {}
        partitions_metahit_ratio = {}
        partitions_metahit_counts = {}
        partitions_metamiss_counts = {}
        partitions_openhit_ratio = {}
        partitions_openhit_counts = {}
        partitions_openmiss_counts = {}
        for part in partitions:
            part = str(part)
            miss = long(pagecache_miss_counts.get(part, 0))
            hit = long(pagecache_hit_counts.get(part, 0))
            metamiss = long(meta_miss_counts.get(part, 0))
            metahit = long(meta_hit_counts.get(part, 0))
            openmiss = long(open_miss_counts.get(part, 0))
            openhit = long(open_hit_counts.get(part, 0))
            total_req = miss + hit
            metatotal_req = metamiss + metahit
            opentotal_req = openmiss + openhit
            partitions_hit_counts[part] = hit
            partitions_miss_counts[part] = miss
            partitions_metahit_counts[part] = metahit
            partitions_metamiss_counts[part] = metamiss
            partitions_openhit_counts[part] = openhit
            partitions_openmiss_counts[part] = openmiss
            if total_req > 0:
                partitions_hit_ratio[part] = hit / float(total_req)
            else:
                partitions_hit_ratio[part] = 0.0
            if metatotal_req > 0:
                partitions_metahit_ratio[part] = metahit / float(metatotal_req)
            else:
                partitions_metahit_ratio[part] = 0.0
            if opentotal_req > 0:
                partitions_openhit_ratio[part] = openhit / float(opentotal_req)
            else:
                partitions_openhit_ratio[part] = 0.0
        if self.partitions_devices_hit_ratio.get(device, None) is None:
            self.partitions_devices_hit_ratio[device] = partitions_hit_ratio
            self.partitions_devices_hit_count[device] = partitions_hit_counts
            self.partitions_devices_miss_count[device] = partitions_miss_counts
        else:
            self.partitions_devices_hit_ratio[device].update(partitions_hit_ratio)
            self.partitions_devices_hit_count[device].update(partitions_hit_counts)
            self.partitions_devices_miss_count[device].update(partitions_miss_counts)
        if self.partitions_devices_metahit_ratio.get(device, None) is None:
            self.partitions_devices_metahit_ratio[device] = partitions_metahit_ratio
            self.partitions_devices_metahit_count[device] = partitions_metahit_counts
            self.partitions_devices_metamiss_count[device] = partitions_metamiss_counts
        else:
            self.partitions_devices_metahit_ratio[device].update(partitions_metahit_ratio)
            self.partitions_devices_metahit_count[device].update(partitions_metahit_counts)
            self.partitions_devices_metamiss_count[device].update(partitions_metamiss_counts)
        if self.partitions_devices_openhit_ratio.get(device, None) is None:
            self.partitions_devices_openhit_ratio[device] = partitions_openhit_ratio
            self.partitions_devices_openhit_count[device] = partitions_openhit_counts
            self.partitions_devices_openmiss_count[device] = partitions_openmiss_counts
        else:
            self.partitions_devices_openhit_ratio[device].update(partitions_openhit_ratio)
            self.partitions_devices_openhit_count[device].update(partitions_openhit_counts)
            self.partitions_devices_openmiss_count[device].update(partitions_openmiss_counts)
