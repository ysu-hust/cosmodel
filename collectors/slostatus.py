import redis
import time
from swift.common.swob import Request

class SloStatus(object):
    """
    Count slo meets and violations
    """

    def __init__(self, app, conf):
        self.app = app
        self.conf = conf
        self.slolatency = float(self.conf.get("slolatency", 0.1))
        self.slomeetcounter = self.conf.get("slomeetcounter", "slomeetcounter")
        self.sloviolatecounter = self.conf.get("sloviolatecounter", "sloviolatecounter")
        ds = self.conf.get("durationswitch", "off")
        if ds == "on":
            self.durationswitch = True
        else:
            self.durationswitch = False
        self.redis_client = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

    def __call__(self, env, start_response):
        if env['REQUEST_METHOD'] != 'GET':
            return self.app(env, start_response)
        req = Request(env)
        version, account, container, obj = req.split_path(1, 4, True)
        if None in [version, account, container, obj]:
            return self.app(env, start_response)
        return_status = [None, None]
        server_ip = env['SERVER_NAME']
        start_time = time.time()

        def start_response_ss(status, headers):
            return_status[0] = status
            device_name = "unknown"
            for item in headers:
                if item[0] == "nodekey".title():
                    device_name = item[1]
                    # headers.remove(item)
                    break
            headers.append(('proxyslo_start_time', start_time))
            headers.append(('proxyslo_end_time', time.time()))
            headers.append(('proxy_nodekey', str(server_ip)))
            return_status[1] = device_name
            return start_response(status, headers)

        data = self.app(env, start_response_ss)
        duration = time.time() - start_time
        if return_status[0] != "200 OK":
            return data
        device_name = return_status[1]
        if self.durationswitch:
            self.redis_client.lpush("%s_duration" % (device_name), duration)
        if duration > self.slolatency:
            self.redis_client.hincrby(self.sloviolatecounter, device_name, 1)
        else:
            self.redis_client.hincrby(self.slomeetcounter, device_name, 1)
        return data


def filter_factory(global_conf, **local_conf):
    conf = global_conf.copy()
    conf.update(local_conf)

    def slostatus_filter(app):
        return SloStatus(app, conf)

    return slostatus_filter
