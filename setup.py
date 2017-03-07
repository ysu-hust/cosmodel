# coding=utf-8
from setuptools import setup, find_packages
from glob import glob
setup(
    name="cosmodel",
    version="1.0",
    author='suyi',
    packages=find_packages(),  # 自动查找并添加当前目录下的所有包
    scripts=glob("bin/*"),
    # install_requires=['redis',
    #                   'eventlet',
    #                   'pandas',
    #                   'numpy',
    #                   'scipy',
    #                   'matplotlib',
    #                   'seaborn'
    #                   ],
                      # yum install hdf5 hdf5-devel
    entry_points={
        'paste.filter_factory': ['cosworkloadc = collectors.workloadcollector:filter_factory',
                                 'cossloc = collectors.slostatus:filter_factory'], }
)