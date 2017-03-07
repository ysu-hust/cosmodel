import json
import redis
import time
import random
from swift.common.swob import Request
# from swift.common.utils import get_logger


class WorkloadCollector(object):
    """
    WorkloadCollector middleware is used to collect
    arriving requests and requests data size.

    Needs to be added to the pipeline and requires a filter
    declaration in the object-server.conf:

    [filter:workloadcollector]
    use = egg:adaptivect#workloadcollector
    """
    def __init__(self, app, conf, *args, **kwargs):
        self.app = app
        self.conf = conf
        self.redis_client = redis.StrictRedis(
            host="127.0.0.1", port=6379, db=0)
        self.incomingreqkey = self.conf.get("incomingreqkey", "incoming_req_count")
        self.returnreqkey = self.conf.get("returnreqkey", "return_req_count")
        self.returnreqsizekey = self.conf.get("returnreqsizekey", "return_req_size")
        self.returnreqlatencykey = self.conf.get("returnreqlatency", "return_req_latency")
        self.slomeetcounter = self.conf.get("slomeetcounter", "slomeetcounter")
        self.sloviolatecounter = self.conf.get("sloviolatecounter", "sloviolatecounter")
        self.slolatency = float(self.conf.get("slolatency", 0.1))
        self.blocksize = long(self.conf.get("blocksize", 65536))
        self.readcountskey = self.conf.get("readcounts", "readcounts")
        self.max_range = long(self.conf.get("max_range", 100))
        self.sample_ratio = long(self.conf.get("sample_ratio", 10))
        self.count_threshold = int(self.max_range / self.sample_ratio)
        ds = self.conf.get("durationswitch", "off")
        if ds == "on":
            self.durationswitch = True
        else:
            self.durationswitch = False

    def __call__(self, env, start_response):
        if env['REQUEST_METHOD'] != 'GET':
            return self.app(env, start_response)
        req = Request(env)
        version, account, container, obj = req.split_path(1, 4, True)
        if None in [version, account, container, obj]:
            return self.app(env, start_response)
        if random.randint(1, self.max_range) > self.count_threshold:
            return self.app(env, start_response)

        return_status = [None, None]
        start_t = time.time()

        def start_response_wc(status, headers):
            return_status[0] = status
            content_len = 0
            for item in headers:
                if item[0] == "Content-Length":
                    content_len = long(item[1])
            headers.append(('objslo_start_time', start_t))
            headers.append(('objslo_end_time', time.time()))
            return_status[1] = content_len
            return start_response(status, headers)

        if env['REQUEST_METHOD'] == 'GET':
            server_ip = env['SERVER_NAME']
            server_port = str(env['SERVER_PORT'])
            device = ":".join([server_ip, server_port])
            self.redis_client.hincrby(self.incomingreqkey, device, 1 * self.sample_ratio)
            data = self.app(env, start_response_wc)
            duration = time.time() - start_t
            if self.durationswitch:
                self.redis_client.lpush("%s_duration" % (device), duration)
            if duration > self.slolatency:
                self.redis_client.hincrby(self.sloviolatecounter, device, 1 * self.sample_ratio)
            else:
                self.redis_client.hincrby(self.slomeetcounter, device, 1 * self.sample_ratio)
            content_len = return_status[1]
            self.redis_client.hincrby(self.returnreqkey, device, 1 * self.sample_ratio)
            self.redis_client.hincrby(self.returnreqsizekey, device, content_len * self.sample_ratio)
            self.redis_client.hincrby(self.returnreqlatencykey, device, int(duration*1000) * self.sample_ratio)
            self.redis_client.hincrby(self.readcountskey, device, (int(content_len/self.blocksize) + 1) * self.sample_ratio)
            return data


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def wc_filter(app):
        return WorkloadCollector(app, conf)
    return wc_filter
