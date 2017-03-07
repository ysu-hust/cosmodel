#!/usr/bin/env python
from utils import default_config
from onlinemetrics import online_metrics
import time
import redis
import json


def save_metrics_measure_and_predict(metrics, f):
    try:
        print "###### start saving current online metrics ######"
        cur_metrics = {}
        cur_metrics["slostatus_obj"] = metrics.slostatus_obj
        cur_metrics["slomeetcounter_obj"] = metrics.slomeetcounter_obj
        cur_metrics["sloviolatecounter_obj"] = metrics.sloviolatecounter_obj
        cur_metrics["slostatus_proxy"] = metrics.slostatus_proxy
        cur_metrics["slomeetcounter_proxy"] = metrics.slomeetcounter_proxy
        cur_metrics["sloviolatecounter_proxy"] = metrics.sloviolatecounter_proxy
        cur_metrics["devices_incoming_req_count_rate"] = metrics.devices_incoming_req_count_rate
        cur_metrics["devices_req_count_rate"] = metrics.devices_req_count_rate
        cur_metrics["devices_req_size_rate"] = metrics.devices_req_size_rate
        cur_metrics["devices_req_read_rate"] = metrics.devices_req_read_rate
        cur_metrics["devices_diskio_reads"] = metrics.devices_diskio_reads
        cur_metrics["devices_diskio_readkbs"] = metrics.devices_diskio_readkbs
        cur_metrics["devices_service_time_measure"] = metrics.devices_service_time_measure
        cur_metrics["partitions_devices_hit_ratio"] = metrics.partitions_devices_hit_ratio
        cur_metrics["partitions_devices_hit_count"] = metrics.partitions_devices_hit_count
        cur_metrics["partitions_devices_miss_count"] = metrics.partitions_devices_miss_count
        cur_metrics["partitions_devices_metahit_ratio"] = metrics.partitions_devices_metahit_ratio
        cur_metrics["partitions_devices_metahit_count"] = metrics.partitions_devices_metahit_count
        cur_metrics["partitions_devices_metamiss_count"] = metrics.partitions_devices_metamiss_count
        cur_metrics["partitions_devices_openhit_ratio"] = metrics.partitions_devices_openhit_ratio
        cur_metrics["partitions_devices_openhit_count"] = metrics.partitions_devices_openhit_count
        cur_metrics["partitions_devices_openmiss_count"] = metrics.partitions_devices_openmiss_count
        cur_metrics["status_collecting_time"] = time.time()
        cur_metrics_json = json.dumps(cur_metrics)
        f.write(cur_metrics_json+"\n")
        print "##### end saving current metrics ########"
    except Exception as e:
        print e


def collect_online_metrics(params, online_metrics_file = "/tmp/online_metrics.log"):
    metrics = params["metrics"]
    with open(online_metrics_file, 'w') as f:
        round_n = 0
        while True:
            try:
                start_t = time.time()
                print "###### start update status ######"
                metrics.update_devices_workload(default_config.devices)
                metrics.update_proxy_slostatus(default_config.proxy_servers)
                for device in default_config.devices:
                    metrics.update_partitions_devices_hit_ratio(device)
                save_metrics_measure_and_predict(metrics, f)
            except Exception as e:
                print 'exception happend at round_n %d' % (round_n)
                print str(e)
            sleep_t = default_config.schedule_interval - (time.time() - start_t)
            if sleep_t > 0:
                time.sleep(sleep_t)
            round_n += 1


def get_proclat(obj_redis_clients, proxy_redis_clients):
    proxy_obj_lats = {}
    for rc in obj_redis_clients:
        for object_s in default_config.object_servers:
            if rc.exists('%s:6000_duration' % (object_s)):
                lats = rc.lrange('%s:6000_duration' % (object_s), 0, -1)
                rc.delete('%s:6000_duration' % (object_s))
                obj_lat = proxy_obj_lats.get(object_s+'_obj', [])
                obj_lat += lats
                proxy_obj_lats[object_s+'_obj'] = obj_lat
    for rc in proxy_redis_clients:
        for object_s in default_config.object_servers:
            if rc.exists('%s:6000_duration' % (object_s)):
                lats = rc.lrange('%s:6000_duration' % (object_s), 0, -1)
                rc.delete('%s:6000_duration' % (object_s))
                proxy_lat = proxy_obj_lats.get(object_s+'_proxy', [])
                proxy_lat += lats
                proxy_obj_lats[object_s+'_proxy'] = proxy_lat
    return proxy_obj_lats


def collect_proclat(params, proclat_file = '/tmp/processing_latencies.log'):
    obj_redis_clients = []
    proxy_redis_clients = []
    try:
        for object_s in default_config.object_servers:
            rc = redis.StrictRedis(host=object_s, port=6379, db=0)
            obj_redis_clients.append(rc)
        for proxy_s in default_config.proxy_servers:
            rc = redis.StrictRedis(host=proxy_s, port=6379, db=0)
            proxy_redis_clients.append(rc)
    except Exception as e:
        print str(e)
        print 'exception during build redis clients'
    try:
        with open(proclat_file, 'w') as f:
            while True:
                start_t = time.time()
                lats = get_proclat(obj_redis_clients, proxy_redis_clients)
                f.write(json.dumps(lats) + '\n')
                sleep_t = default_config.schedule_interval - (time.time() - start_t)
                print sleep_t
                if sleep_t > 0:
                    time.sleep(sleep_t)
    except Exception as e:
        print 'exception during collect data'
        print str(e)