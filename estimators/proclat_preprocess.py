import pandas as pd
import matplotlib.pyplot as plt
import json
from glob import glob
from utils import default_config

def proclat_pp():
    with open(proclat_raw_file, 'r') as f:
        all_data = {}
        for line in f:
            try:
                data = json.loads(line)
                # print data.keys()
                for key, lats in data.items():
                    all_lats = all_data.get(key, [])
                    all_lats += lats
                    all_data[key] = all_lats
            except Exception as e:
                print 'load data error'
                print str(e)
                continue
    all_series = {}
    for k, v in all_data.items():
        key = "%s_%s" % ('proclat', k)
        all_series[key] = pd.Series(v)
    df = pd.DataFrame(all_series)

    # print df.describe()
    for c in df.columns:
        df[c] = df[c].apply(lambda x: float(x))
    df.to_hdf(default_config.proclat_output_file, 'df')
    dfq = df.quantile([0.001*i for i in range(1001)])
    # print dfq.describe()
    # dfq.plot()
    # plt.show()
