def numerical_inverse_laplace_transform(F, t, n = 6):
    import scipy.misc
    import numpy as np

    def csteh(n, i):
        fact = scipy.misc.factorial
        acc = 0.0
        for k in range(int(np.floor((i+1)/2.0)), int(min(i, n/2.0))+1):
            num = k**(n/2.0) * fact(2 * k)
            den = fact(i - k) * fact(k -1) * fact(k) * fact(2*k - i) * fact(n/2.0 - k)
            acc += (num /den)
        expo = i+n/2.0
        term = np.power(-1+0.0j,expo)
        res = term * acc
        return res.real

    acc = 0.0
    lton2 = np.log(2) / t
    for i in range(1, n+1):
        a = csteh(n, i)
        b = F(i * lton2)
        acc += (a * b)
    return lton2 * acc