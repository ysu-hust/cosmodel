<!-- TOC depthFrom:1 depthTo:6 withLinks:1 updateOnSave:1 orderedList:0 -->

- [Introduction of COSModel](#introduction-of-cosmodel)
- [Installation](#installation)
- [Basic Usage](#basic-usage)
  - [The Predictor](#the-predictor)
  - [The Collectors](#the-collectors)
  - [The Estimators](#the-estimators)
- [Configurations](#configurations)
- [Future](#future)

<!-- /TOC -->

# Introduction of COSModel

The cosmodel is a queueing-theory based performance model, which predicts the percentiles of requests meeting SLA (Service Level Agreement), for cloud object storage systems (e.g. OpenStack Swift, Ceph, Amazon S3, etc.).

The implementation of cosmodel contains three core components: the _predictor_, the _collectors_, and the _estimators_. The _predictor_ performs the prediction based on several parameters, including the device performance properties and system online metrics. We collect the system online metrics by the _collectors_ and use the _estimators_ to obtain the device performance properties. Moreover, the implementation of cosmodel is based on OpenStack Swift.

# Installation

~~~sh
python setup.py install
~~~

# Basic Usage

## The Predictor

A simple example of using the predictor for predicting the percentiles of requests meeting SLA (response latency requirement) is as follows:

~~~sh
$ cospredict.py predict -n 1 -i 5 -l 0.05 -s onlinemetrics.h5 -w -g
$ # 1 object server process per storage device : -n 1
$ # predict for each 5 minutes : -i 5
$ # use 0.05s as the latency requirement : -l 0.05
$ # the file onlinemetrics.h5 stores the online metrics : -s onlinemetrics.h5
$ # predict with noWTA model (baseline model 1) : -w
$ # predict with ODOPR model (baseline model 2): -g
~~~

The predictor needs to preprocess the online metrics log file, which is provided by the collectors, before taking as an input with the following command.

~~~sh
cospredict.py convert -i onlinemetrics.log -o onlinemetrics.h5
~~~

For more usages of `cospredictor.py`, please refer `cospredictor.py -h`.

## The Collectors

All of the system online metrics, e.g. SLO violations, request arriving rate, are collected by the collectors.

The collectors are daemons, which collect the metrics periodically. Use the following command to start the collector.

~~~sh
$ coscollect.py online
~~~

And the following middlewares should be included in the OpenStack Swift pipeline. These middleware save metrics in the local Redis server and the collector read these metrics from the Redis server of each machine. Beside these middlewares, we add some code in OpenStack Swift to record the cache miss/hit count of different disk operations (open, read metadata, read data).

In the pipeline of proxy servers, add the middleware _cossloc_ with the following configuration:

~~~conf
[filter:cossloc]
use = egg:cosmodel#cossloc
durationswitch = off
slolatency = 0.05
~~~

In the pipeline of object servers, add the middleware _cosworkloadc_ with the following configuration:

~~~conf
[filter:cosworkloadc]
use = egg:cosmodel#cosworkloadc
sample_ratio = 10
durationswitch = off
slolatency = 0.05
~~~

In the above configurations, the _slolatency_ can be set as the value needed.

## The Estimators

The estimators are used to estimate device performance properties.

First, estimating the distribution of disk service time for different operations (index lookup, metadata read and data read).

~~~sh
$ # benchmarking the storage device
$ cosestimate.py diskbench
$ # find the distribution of disk service times with distribution fitting
$ cosestimate.py fitdiskst
~~~

Seconds, estimating the distribution of request processing latency for both frontend servers and backend servers.

~~~sh
$ # use the following command to collect processing latencies
$ coscollect.py proclat
$ # preprocess the processing latencies provided by the collectors
$ cosestimate.py ppproclat
$ # find the distribution of processing latencies with distribution fitting
$ cosestimate.py fitproclat
~~~

In order to collect processing latencies at frontend and backend servers, the middleware _cossloc_ in proxy server pipeline and _cosworkloadc_ in object server pipeline should be configured as following:

~~~conf
[filter:cossloc]
use = egg:cosmodel#cossloc
durationswitch = on
slolatency = 0.05
~~~

~~~conf
[filter:cosworkloadc]
use = egg:cosmodel#cosworkloadc
sample_ratio = 10
durationswitch = on
slolatency = 0.05
~~~

# Configurations

The configurations are in the `utils/default_config.py`. Re-installation is required for making the modification of configurations take effect.

# Future

- The cosmodel currently only support replica-based systems, we will extend it for systems using erasure codes for reliability.

---

Please refer the research paper of COSModel[1] for more information.


[1] Yi Su, Dan Feng, Yu Hua, Zhan Shi, "Predicting Response Latency Percentiles for Cloud Object Storage Systems", Proceedings of the 46th International Conference on Parallel Processing (ICPP), 2017.