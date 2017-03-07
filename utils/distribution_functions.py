# -*- coding: utf-8 -*-
def lognormal_cdf(x, logmean, logsd):
    from scipy.special import erf
    import math
    mu = logmean
    sigma = logsd
    return 1/2 + (erf((math.log(x) - mu)/(2**0.5 * sigma)))/2


def normal_cdf(x, mean, stdev):
    from scipy.special import erf
    mu = mean
    sigma = stdev
    p = 0.5 * (1 + erf((x - mu)/(sigma * 2**0.5)))
    return p
    

def weibull_cdf(x, shape, scale):
    import numpy as np
    lambd = scale
    k = shape
    if x < 0:
        p = 0
    else:
        p = 1 - np.exp(-(x/lambd)**k)
    return p
    

def gamma_cdf(x, shape, rate):
    from scipy.special import gamma, gammainc
    from actutils.actutils import numerical_inverse_laplace_transform
    alpha = shape
    beta = rate
#    p = gammainc(alpha, beta*x) / gamma(alpha)
    # gammainc and gamma function introduce a lot inaccuracy 
    def laplace_gamma_cdf(s):
        laplace_p = (beta)**(alpha)*(s+beta)**(-alpha) / s
        return laplace_p
    p = numerical_inverse_laplace_transform(laplace_gamma_cdf, x, n=6)
    return p


def logistic_cdf(x, location, scale):
    import os
    os.environ['R_HOME'] = 'G:/pf/R-3.3.1'
    from rpy2.robjects.packages import importr
    hypergeo = importr('hypergeo')
    from scipy.special import hyp2f1
    import numpy as np
    from actutils.actutils import numerical_inverse_laplace_transform
    
    def laplace_logistic_cdf(s):
        laplace_p = hyp2f1(1, s*scale, 1+s*scale, -np.exp(location/scale)) / s
#        laplace_p = hypergeo.hypergeo(1, s*scale, 1+s*scale, -np.exp(location/scale))[0].real / s
        return laplace_p

    return numerical_inverse_laplace_transform(laplace_logistic_cdf, x, n=6)


def logistic_cdf2(x, location, scale):
    import numpy as np
    p = 1 / (1 + np.exp(-(x - location)/scale))
    return p
    
def logistic_cdf3(x, location, scale):
    from scipy.special import gamma, hyp2f1
    import numpy as np
    from actutils.actutils import numerical_inverse_laplace_transform
    
    def laplace_logistic_cdf(s):
        laplace_p = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
        return laplace_p / s
    
    return numerical_inverse_laplace_transform(laplace_logistic_cdf, x, n=6)


def exp_cdf(x, lambd):
    import numpy as np
    return 1 - np.exp(-lambd * x)
    

def degenerate_cdf(x, d):
    if x < d:
        return 0
    elif x >= d:
        return 1
    else:
        return -1