import scipy
import numpy as np
from scipy.optimize import fsolve
from scipy.special import polygamma, erfc
import json
from utils.nilaplace import numerical_inverse_laplace_transform
from scipy import integrate
from scipy.misc import derivative
import numdifftools
from scipy.special import gamma, hyp2f1
import math
from cosmodel import build_cosmodel


def build_nowta_model(lambd_proxy, lambd_open, lambd_meta, lambd_data, theta_total, theta_open, theta_meta, theta_data, bp, b1, b2, npn, npp, no, device_name, obj_mode, proxy_mode, noaccept=True, timeouts=-1, diskio=None, responsedata=True):
    m = build_cosmodel(lambd_proxy, lambd_open, lambd_meta, lambd_data, theta_total, theta_open, theta_meta, theta_data, bp, b1, b2, npn, npp, no, device_name, obj_mode, proxy_mode, noaccept=True, timeouts=timeouts, diskio=diskio, responsedata=responsedata)
    return m
