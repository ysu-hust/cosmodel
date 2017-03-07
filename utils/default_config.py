# general infomation
proxy_servers = ["192.168.3.134", "192.168.3.135", "192.168.3.136"]

object_servers = ["192.168.3.131", "192.168.3.132",
                  "192.168.3.137", "192.168.3.138"]

devices = ["192.168.3.131:6000", "192.168.3.132:6000",
           "192.168.3.137:6000", "192.168.3.138:6000"]

schedule_interval = 60

swiftparts_amount = 1024

swift_replication_count = 3

# for predictor
requests_processing_latency_be = {"192.168.3.131:6000": 1.0264832049361226, "192.168.3.132:6000": 1.0713007276488555,
                                  "192.168.3.137:6000": 1.0405906908529752, "192.168.3.138:6000": 1.0723123197992723}
requests_processing_latency_fe = 7.755805878462008 # in ms
number_of_fe_server = 3.0
number_of_processes_per_fe_server = 16.0

# for collectors
influxdb_info = ('192.168.3.133', 7086, 'root', 'root', 'collectl_graphite')
ip2hostname = {'192.168.3.131': 'de11', '192.168.3.132': 'de12',
               '192.168.3.133': 'de13', '192.168.3.134': 'de14',
               '192.168.3.135': 'de15', '192.168.3.136': 'de16',
               '192.168.3.137': 'de17', '192.168.3.138': 'de18',
               '192.168.3.127': 'de07', '192.168.3.128': 'de08',
               '192.168.3.129': 'de09', '192.168.3.80': 'de80',
               '192.168.3.121': 'de01', '192.168.3.122': 'de02',
               '192.168.3.124': 'de04'}
hostname2ip = {}
for ip, hostname in ip2hostname.items():
    hostname2ip[hostname] = ip
port2devicename = {'6000':'sdb'}

# fitting disk service time
r_home = 'G:/pf/R-3.3.1'
diskst_path = "E:/laven/Desktop/buffer/randomwalk/%s/fileop_latency_%d.json"
disk_id_list = ['de11', 'de12', 'de17', 'de18']
count_list = [500000]
diskbench_result_path = '/root/misc/fileop_lat'
policy_name = 'objects-4'
device_mountpoint = '/srv/node/sdb'
count = 500001

#fitting processing latency
proclat_path = 'E:/laven/Desktop/buffer/duration/%s'
proclat_file_h5 = 'duration.h5'
proclat_raw_file = '/tmp/processing_latencies.log'
proclat_output_file = '/tmp/duration.h5'
