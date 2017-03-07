import scipy
import numpy as np
from scipy.optimize import fsolve
from scipy.special import polygamma, erfc
import json
from scipy import integrate
from scipy.misc import derivative
import numdifftools
from scipy.special import gamma, hyp2f1
import math
from utils.nilaplace import numerical_inverse_laplace_transform


devices_alpha_open_base = {"192.168.3.131:6000": 5.042402011, "192.168.3.132:6000": 3.438135179,
                           "192.168.3.137:6000": 3.904126683, "192.168.3.138:6000": 4.006296259}
devices_alpha_meta_base = {"192.168.3.131:6000": 2.08989357, "192.168.3.132:6000": 3.276573886,
                           "192.168.3.137:6000": 3.119048283, "192.168.3.138:6000": 3.19371288}
devices_alpha_data_base = {"192.168.3.131:6000": 3.23295142, "192.168.3.132:6000": 1.144592591,
                           "192.168.3.137:6000": 1.113174195, "192.168.3.138:6000": 1.119231261}
devices_b2_factors_base = {"192.168.3.131:6000": (16.234306831, 9.68929933739, 6.42678010342),
                           "192.168.3.132:6000": (16.6073951025, 9.72659476662, 5.70571148005),
                           "192.168.3.137:6000": (16.4279713827, 9.63469014359, 5.42385335114),
                           "192.168.3.138:6000": (15.5213973208, 9.25704209328, 5.34988939416)}

usediskmodel_mg1 = False
USE_MMN_FOR_WAITTIME = False


def build_odopr_model(lambd_proxy, lambd_open, lambd_meta, lambd_data, theta_total, theta_open, theta_meta, theta_data, bp, b1, b2, npn, npp, no, device_name, obj_mode, proxy_mode, noaccept=False, timeouts=-1, diskio=None, responsedata=True):
    theta_open_origin = theta_open
    theta_meta_origin = theta_meta
    theta_open = 0.0
    theta_meta = 0.0

    def odopr_model_1obj(slo_latency):
        t = slo_latency
        devices_alpha_open = {"192.168.3.131:6000": 0.328339060497165, "192.168.3.132:6000": 0.33414346518274846,
                              "192.168.3.137:6000": 0.3277722251024646, "192.168.3.138:6000": 0.3407520463806724}
        devices_alpha_meta = {"192.168.3.131:6000": 2.0866255518457413, "192.168.3.132:6000": 3.283366666881215,
                              "192.168.3.137:6000": 3.10944669945261, "192.168.3.138:6000": 3.170886112751283}
        devices_alpha_data = {"192.168.3.131:6000": 3.180533441552923, "192.168.3.132:6000": 1.140878656027706,
                              "192.168.3.137:6000": 1.0986415206930444, "192.168.3.138:6000": 1.112302242565262}
        devices_alpha_total = {"192.168.3.131:6000": 0.7125107299836276, "192.168.3.132:6000": 0.6602799845021714,
                               "192.168.3.137:6000": 0.6420578845640275, "192.168.3.138:6000": 0.6652170970590439}
        devices_b2_factors = {"192.168.3.131:6000": (0.009015, 0.009664, 0.006430),
                              "192.168.3.132:6000": (0.009241, 0.009754, 0.005647),
                              "192.168.3.137:6000": (0.009018, 0.009679, 0.005534),
                              "192.168.3.138:6000": (0.008453, 0.009291, 0.005369)}
        devices_alpha_open = devices_alpha_open_base
        devices_alpha_meta = devices_alpha_meta_base
        devices_alpha_data = devices_alpha_data_base
        devices_b2_factors = devices_b2_factors_base

        epsilon = lambd_open - lambd_data

        def laplace_disk_latency_distribution(s, alpha_disk, b2_disk):
            beta_disk = alpha_disk / b2_disk
            laplace_disk_lat = (beta_disk)**(alpha_disk)*(s+beta_disk)**(-alpha_disk)
            return laplace_disk_lat

        def laplace_part_latency_distribution(s, alpha_disk, b2_disk, theta):
            laplace_part_lat = 1 - theta + theta * laplace_disk_latency_distribution(s, alpha_disk, b2_disk)
            return laplace_part_lat

        def laplace_b1_exp(s):
            laplace_b1 = 1 / (1+b1*s)
            # laplace_b1 = np.exp(-b1 * s)
            return laplace_b1

        def laplace_b1_determin(s):
            # laplace_b1 = 1 / (1+b1*s)
            laplace_b1 = np.exp(-b1 * s)
            return laplace_b1

        def laplace_b1_logistic(s):
            location = b1
            scale = 0.06240118068052774
            laplace_b1 = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_b1

        def laplace_b1_logistic_ondemand(s):
            scales = {"192.168.3.131:6000": 0.10420402224961232, "192.168.3.132:6000": 0.11127659736849023,
                      "192.168.3.137:6000": 0.11264790129260442, "192.168.3.138:6000": 0.09575652668076151}
            scales = {"192.168.3.131:6000": 0.07147761825157728, "192.168.3.132:6000": 0.07714479399550411,
                      "192.168.3.137:6000": 0.06968345903140147, "192.168.3.138:6000": 0.07508798663994147}
            location = b1
            scale = scales[device_name]
            laplace_b1 = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_b1

        def laplace_b1_logistic_performance(s):
            scales = {"192.168.3.131:6000": 0.056066101875884365, "192.168.3.132:6000": 0.05733568679664464,
                      "192.168.3.137:6000": 0.06385827310619747, "192.168.3.138:6000": 0.06730755712896615}
            location = b1
            scale = scales[device_name]
            laplace_b1 = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_b1

        def laplace_b1(s):
            if obj_mode == 'exp':
                return laplace_b1_exp(s)
            elif obj_mode == 'determin':
                return laplace_b1_determin(s)
            elif obj_mode == 'logistic':
                return laplace_b1_logistic(s)
            elif obj_mode == 'logistic_ondemand':
                return laplace_b1_logistic_ondemand(s)
            elif obj_mode == 'logistic_performance':
                return laplace_b1_logistic_performance(s)
            else:
                return eval('laplace_b1_%s' % (obj_mode))(s)

        def calculate_mean_srvtime(lambd):
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open
            lambd_disk_meta = lambd_meta * theta_meta
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            beta_open = alpha_open / b2_o
            beta_meta = alpha_meta / b2_m
            beta_data = alpha_data / b2_d

            epsilon = (lambd_data - lambd) if lambd_data > lambd else 0

            mean_srvtime = b1 + b2_o * theta_open + b2_m * theta_meta + b2_d * theta_data

            return mean_srvtime

        def laplace_srvtime_pdf(s):
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open
            lambd_disk_meta = lambd_meta * theta_meta
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            lambd = lambd_open
            epsilon = (lambd_data - lambd_open) if lambd_data > lambd_open else 0

            laplace_srvtime = laplace_part_latency_distribution(s, alpha_data, b2_d, theta_data) * laplace_part_latency_distribution(s, alpha_meta, b2_m, theta_meta) * laplace_part_latency_distribution(s, alpha_open, b2_o, theta_open) * laplace_b1(s)
            return laplace_srvtime

        def laplace_waittime_pdf(s):
            lambd = lambd_open
            mean_srvtime = calculate_mean_srvtime(lambd)
            rho = lambd * mean_srvtime

            laplace_waittime = (1 - rho) * s / (lambd * laplace_srvtime_pdf(s) + s - lambd)
            return laplace_waittime

        def laplace_sojourntime_pdf(s):
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open
            lambd_disk_meta = lambd_meta * theta_meta
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            laplace_sojourntime = laplace_waittime_pdf(s) * laplace_part_latency_distribution(s, alpha_open, b2_o, theta_open) * laplace_part_latency_distribution(s, alpha_meta, b2_m, theta_meta) * laplace_b1(s)
            if responsedata is True:
                laplace_sojourntime *= laplace_part_latency_distribution(s, alpha_data, b2_d, theta_data)
            return laplace_sojourntime

        def laplace_bp_pdf1(s):
            # laplace_bp = np.exp(-bp * s)
            laplace_bp = (1 / (1+0.5*s))
            return laplace_bp

        def laplace_bp_pdf2(s):
            # laplace_bp = np.exp(-bp * s)
            laplace_bp = (1 / (1+1.0*s))
            return laplace_bp

        def laplace_bp_pdf3(s):
            alpha_bp = 0.40321444078574364
            beta_bp = alpha_bp / 2.0
            laplace_bp = (beta_bp)**(alpha_bp) * (s+beta_bp)**(-alpha_bp)
            return laplace_bp

        def laplace_bp_pdf_exp(s):
            # laplace_bp = np.exp(-bp * s)
            laplace_bp = (1 / (1 + bp * s))
            return laplace_bp

        def laplace_bp_pdf_determin(s):
            laplace_bp = np.exp(-bp * s)
            return laplace_bp

        def laplace_bp_pdf_logistic(s):
            location = bp
            scale = 0.2980952087304305
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf_logistic_ondemand(s):
            scales = {"192.168.3.131:6000": 0.7535452873795191, "192.168.3.132:6000":  0.7534572439147595,
                      "192.168.3.137:6000": 0.767393652700738, "192.168.3.138:6000": 0.7492503830322381}
            scales = {"192.168.3.131:6000": 0.6306146695492518, "192.168.3.132:6000":  0.6737770941272766,
                      "192.168.3.137:6000": 0.6350972540152318, "192.168.3.138:6000": 0.6634937192911066}
            location = bp
            scale = scales[device_name]
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf_logistic_performance(s):
            scales = {"192.168.3.131:6000": 0.4193826181085095, "192.168.3.132:6000":  0.40504827887155465,
                      "192.168.3.137:6000": 0.5385209183156736, "192.168.3.138:6000": 0.5422830955719604}
            location = bp
            scale = scales[device_name]
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf_logistic_performance_union(s):
            scale = 0.47738487306528626
            location = bp
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf(s):
            if proxy_mode == 'exp':
                return laplace_bp_pdf_exp(s)
            elif proxy_mode == 'determin':
                return laplace_bp_pdf_determin(s)
            elif proxy_mode == 'logistic':
                return laplace_bp_pdf_logistic(s)
            elif proxy_mode == 'logistic_ondemand':
                return laplace_bp_pdf_logistic_ondemand(s)
            elif proxy_mode == 'logistic_performance':
                return laplace_bp_pdf_logistic_performance(s)
            elif proxy_mode == 'logistic_performance_union':
                return laplace_bp_pdf_logistic_performance_union(s)
            else:
                return eval('laplace_bp_pdf_%s(s)' % (proxy_mode))

        def laplace_proxy_queue_pdf(s):
            lambd = lambd_proxy / (npn * npp)
            rho = lambd * bp
            laplace_queue = ((1 - rho) * laplace_bp_pdf(s) * s) / (lambd * laplace_bp_pdf(s) + s - lambd)
            return laplace_queue

        def laplace_proxy_sojourntime_pdf(s):
            laplace_proxy_sojourntime = laplace_proxy_queue_pdf(s) * laplace_sojourntime_pdf(s) * laplace_waittime_pdf(s)
            return laplace_proxy_sojourntime

        def laplace_proxy_sojourntime_cdf(s):
            return laplace_proxy_sojourntime_pdf(s) / s

        def laplace_proxy_sojourntime_noaccept_pdf(s):
            laplace_proxy_sojourntime_noaccept = laplace_proxy_queue_pdf(s) * laplace_sojourntime_pdf(s)
            return laplace_proxy_sojourntime_noaccept

        def laplace_proxy_sojourntime_noaccept_cdf(s):
            return laplace_proxy_sojourntime_noaccept_pdf(s) / s

        if noaccept is True:
            p = numerical_inverse_laplace_transform(laplace_proxy_sojourntime_noaccept_cdf, t, n=6)
        else:
            p = numerical_inverse_laplace_transform(laplace_proxy_sojourntime_cdf, t, n=6)
        if diskio is not None:
            if diskio * b2 / 1000.0 > 1.0:
                p = 0

        # if p >= 1:
        #     print lambd_proxy, lambd_open, lambd_meta, lambd_data, theta_total, theta_open, theta_meta, theta_data, bp, b1, b2, npn, npp, no, device_name, obj_mode, proxy_mode

        return p if p <=1 and p >= 0 else (1 if p >= 1 else 0)

    def odopr_model_nobj(slo_latency):
        t = slo_latency
        devices_alpha_open = {"192.168.3.131:6000": 0.328339060497165, "192.168.3.132:6000": 0.33414346518274846,
                              "192.168.3.137:6000": 0.3277722251024646, "192.168.3.138:6000": 0.3407520463806724}
        devices_alpha_meta = {"192.168.3.131:6000": 2.0866255518457413, "192.168.3.132:6000": 3.283366666881215,
                              "192.168.3.137:6000": 3.10944669945261, "192.168.3.138:6000": 3.170886112751283}
        devices_alpha_data = {"192.168.3.131:6000": 3.180533441552923, "192.168.3.132:6000": 1.140878656027706,
                              "192.168.3.137:6000": 1.0986415206930444, "192.168.3.138:6000": 1.112302242565262}
        devices_alpha_total = {"192.168.3.131:6000": 0.7125107299836276, "192.168.3.132:6000": 0.6602799845021714,
                               "192.168.3.137:6000": 0.6420578845640275, "192.168.3.138:6000": 0.6652170970590439}
        devices_b2_factors = {"192.168.3.131:6000": (0.009015, 0.009664, 0.006430),
                              "192.168.3.132:6000": (0.009241, 0.009754, 0.005647),
                              "192.168.3.137:6000": (0.009018, 0.009679, 0.005534),
                              "192.168.3.138:6000": (0.008453, 0.009291, 0.005369)}
        devices_alpha_open = devices_alpha_open_base
        devices_alpha_meta = devices_alpha_meta_base
        devices_alpha_data = devices_alpha_data_base
        devices_b2_factors = devices_b2_factors_base

        epsilon = lambd_data - lambd_open

        def laplace_disk_srvtime_distribution_mg1(s, alpha_disk, b2_disk):
            beta_disk = alpha_disk / b2_disk
            laplace_disk_srvt = (beta_disk)**(alpha_disk)*(s+beta_disk)**(-alpha_disk)
            return laplace_disk_srvt

        def laplace_disk_unionsrvtime_distribution_mg1(s):
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open
            lambd_disk_meta = lambd_meta * theta_meta
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data

            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            laplace_disk_usrvt = (lambd_disk_open * laplace_disk_srvtime_distribution_mg1(s, alpha_open, b2_o) + lambd_disk_meta * laplace_disk_srvtime_distribution_mg1(s, alpha_meta, b2_m) + lambd_disk_data * laplace_disk_srvtime_distribution_mg1(s, alpha_data, b2_d)) / lambd_disk

            return laplace_disk_usrvt

        def laplace_disk_waittime_distribution_mg1(s, lambd_disk):
            # use m/g/1 for mimic m/g/1/k waiting time distribution
            rho = lambd_disk * b2
            lambd = lambd_disk
            laplace_disk_waitt = (1 - rho) * s / (lambd * laplace_disk_unionsrvtime_distribution_mg1(s) + s - lambd)
            return laplace_disk_waitt

        def laplace_disk_latency_distribution_mg1(s, lambd_disk, alpha_disk, b2_disk):
            # use m/g/1 for mimic m/g/1/k response time distribution
            laplace_disk_sojournt = laplace_disk_waittime_distribution_mg1(s, lambd_disk) * laplace_disk_srvtime_distribution_mg1(s, alpha_disk, b2_disk)
            return laplace_disk_sojournt

        def calculate_mean_disk_latency_mg1():
            lambd_disk_open = lambd_open * theta_open
            lambd_disk_meta = lambd_meta * theta_meta
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data

            theta_disk_open = lambd_disk_open / lambd_disk
            theta_disk_meta = lambd_disk_meta / lambd_disk
            theta_disk_data = lambd_disk_data / lambd_disk

            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            beta_open = alpha_open / b2_o
            beta_meta = alpha_meta / b2_m
            beta_data = alpha_data / b2_d

            E_rb = b2 # the disk latency expect
            rho_disk = E_rb * lambd_disk
            V_rbo = alpha_open / (beta_open**2)
            V_rbm = alpha_meta / (beta_meta**2)
            V_rbd = alpha_data / (beta_data**2)
            E_sb = theta_disk_open**2 * (V_rbo + b2_o**2) # the squared disk latency expect
            E_sb += theta_disk_meta**2 * (V_rbm + b2_m**2)
            E_sb += theta_disk_data**2 * (V_rbd + b2_d**2)
            E_sb += 2 * theta_disk_open * theta_disk_meta * b2_o * b2_m
            E_sb += 2 * theta_disk_open * theta_disk_data * b2_o * b2_d
            E_sb += 2 * theta_disk_meta * theta_disk_data * b2_m * b2_d

            mean_disk_latency_mg1 = (rho_disk * E_sb) / (2 * (1 - rho_disk) * E_rb)

            return (mean_disk_latency_mg1+b2_o, mean_disk_latency_mg1+b2_m, mean_disk_latency_mg1+b2_d)

        def laplace_part_latency_distribution_mg1(s, lambd_disk, theta, alpha_disk, b2_disk):
            laplace_part_lat = 1 - theta + theta * laplace_disk_latency_distribution_mg1(s, lambd_disk, alpha_disk, b2_disk)
            return laplace_part_lat

        def laplace_disk_latency_distribution(s, lambd_disk):
            # use m/m/1/k for mimic m/g/1/k response latency distribution
            rho = lambd_disk * b2
            mu = 1 / b2
            P0 = 1 / (sum([rho**k for k in range(0, no+1)]))
            # Pn = ((1-rho) * rho**n) / (1 - rho**(n+1))
            Pn = rho**no / (sum([rho**k for k in range(0, no+1)]))
            laplace_disk_lat = ((mu*P0)/(1-Pn)) * ((1 - (lambd_disk/(mu+s))**no) / (mu - lambd_disk + s))
            return laplace_disk_lat

        def calculate_mean_disk_latency(lambd_disk):
            # use m/m/1/k for mimic m/g/1/k mean disk latency
            rho = lambd_disk * b2
            mu = 1 / b2
            Nbar = (rho * (1 - (no+1)*rho**no + no*rho**(no+1))) / ((1 - rho) * (1 - rho**(no+1)))
            Pn = rho**no / (sum([rho**k for k in range(0, no+1)]))
            Bbar = Nbar / (lambd_disk*(1 - Pn))
            return Bbar

        def laplace_part_latency_distribution(s, lambd_disk, theta):
            laplace_part_lat = 1 - theta + theta * laplace_disk_latency_distribution(s, lambd_disk)
            return laplace_part_lat

        def laplace_b1_exp(s):
            laplace_b1 = 1 / (1+b1*s)
            # laplace_b1 = np.exp(-b1 * s)
            return laplace_b1

        def laplace_b1_determin(s):
            # laplace_b1 = 1 / (1+b1*s)
            laplace_b1 = np.exp(-b1 * s)
            return laplace_b1

        def laplace_b1_logistic(s):
            location = b1
            scale = 0.06240118068052774
            laplace_b1 = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_b1

        def laplace_b1_logistic_ondemand(s):
            scales = {"192.168.3.131:6000": 0.10420402224961232, "192.168.3.132:6000": 0.11127659736849023,
                      "192.168.3.137:6000": 0.11264790129260442, "192.168.3.138:6000": 0.09575652668076151}
            scales = {"192.168.3.131:6000": 0.07147761825157728, "192.168.3.132:6000": 0.07714479399550411,
                      "192.168.3.137:6000": 0.06968345903140147, "192.168.3.138:6000": 0.07508798663994147}
            location = b1
            scale = scales[device_name]
            laplace_b1 = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_b1

        def laplace_b1_logistic_performance(s):
            scales = {"192.168.3.131:6000": 0.056066101875884365, "192.168.3.132:6000": 0.05733568679664464,
                      "192.168.3.137:6000": 0.06385827310619747, "192.168.3.138:6000": 0.06730755712896615}
            location = b1
            scale = scales[device_name]
            laplace_b1 = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_b1

        def laplace_b1(s):
            if obj_mode == 'exp':
                return laplace_b1_exp(s)
            elif obj_mode == 'determin':
                return laplace_b1_determin(s)
            elif obj_mode == 'logistic':
                return laplace_b1_logistic(s)
            elif obj_mode == 'logistic_ondemand':
                return laplace_b1_logistic_ondemand(s)
            elif obj_mode == 'logistic_performance':
                return laplace_b1_logistic_performance(s)
            else:
                return eval('laplace_b1_%s' % (obj_mode))(s)

        def calculate_mean_srvtime():
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open_origin
            lambd_disk_meta = lambd_meta * theta_meta_origin
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            if usediskmodel_mg1:
                b2_o, b2_m, b2_d = calculate_mean_disk_latency_mg1()
            else:
                Bbar = calculate_mean_disk_latency(lambd_disk)
                b2_o, b2_m, b2_d = Bbar, Bbar, Bbar

            # Not use (1/n epsilon) and (1/n lambd), because only the ratio matters here
            # for calculating the request service time
            lambd = lambd_open
            epsilon = (lambd_data - lambd) if lambd_data > lambd else 0

            mean_srvtime = b2_d * theta_data + b2_m * theta_meta + b2_o * theta_open + b1
            return mean_srvtime

        def laplace_srvtime_pdf(s):
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open_origin
            lambd_disk_meta = lambd_meta * theta_meta_origin
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            # Not use (1/n epsilon) and (1/n lambd), because only the ratio matters here
            # for calculating the request service time
            lambd = lambd_open
            epsilon = (lambd_data - lambd_open) if lambd_data > lambd_open else 0

            if usediskmodel_mg1:
                b2_o, b2_m, b2_d = calculate_mean_disk_latency_mg1()
                laplace_srvtime = laplace_part_latency_distribution_mg1(s, lambd_disk, theta_data, alpha_data, b2_d) * laplace_part_latency_distribution_mg1(s, lambd_disk, theta_meta, alpha_meta, b2_m) * laplace_part_latency_distribution_mg1(s, lambd_disk, theta_open, alpha_open, b2_o) * laplace_b1(s)
            else:
                Bbar = calculate_mean_disk_latency(lambd_disk)
                b2_o, b2_m, b2_d = Bbar, Bbar, Bbar
                laplace_srvtime = laplace_part_latency_distribution(s, lambd_disk, theta_data) * laplace_part_latency_distribution(s, lambd_disk, theta_meta) * laplace_part_latency_distribution(s, lambd_disk, theta_open) * laplace_b1(s)
            return laplace_srvtime

        def laplace_waittime_pdf(s):
            # this is important: for each object server process
            # the requests arriving rate is only (1/n) of the total requests arriving rate
            lambd = lambd_open / no
            mean_srvtime = calculate_mean_srvtime()
            rho = lambd * mean_srvtime

            laplace_waittime = (1 - rho) * s / (lambd * laplace_srvtime_pdf(s) + s - lambd)
            return laplace_waittime

        def laplace_sojourntime_pdf(s):
            alpha_open = devices_alpha_open[device_name]
            alpha_meta = devices_alpha_meta[device_name]
            alpha_data = devices_alpha_data[device_name]

            lambd_disk_open = lambd_open * theta_open
            lambd_disk_meta = lambd_meta * theta_meta
            lambd_disk_data = lambd_data * theta_data
            lambd_disk = lambd_disk_open + lambd_disk_meta + lambd_disk_data
            b2f_o, b2f_m, b2f_d = devices_b2_factors[device_name]
            atom_b2 = (b2 * lambd_disk * b2f_d) / (b2f_o * lambd_disk_open + b2f_m * lambd_disk_meta + b2f_d * lambd_disk_data)

            b2_o = b2f_o * atom_b2 / b2f_d
            b2_m = b2f_m * atom_b2 / b2f_d
            b2_d = atom_b2

            if usediskmodel_mg1:
                b2_o, b2_m, b2_d = calculate_mean_disk_latency_mg1()
                laplace_sojourntime = laplace_waittime_pdf(s) * laplace_part_latency_distribution_mg1(s, lambd_disk, theta_open, alpha_open, b2_o) * laplace_part_latency_distribution_mg1(s, lambd_disk, theta_meta, alpha_meta, b2_m) * laplace_b1(s)
                if responsedata is True:
                    laplace_sojourntime *= laplace_part_latency_distribution_mg1(s, lambd_disk, theta_data, alpha_data, b2_d)
            else:
                Bbar = calculate_mean_disk_latency(lambd_disk)
                b2_o, b2_m, b2_d = Bbar, Bbar, Bbar
                laplace_sojourntime = laplace_waittime_pdf(s) * laplace_part_latency_distribution(s, lambd_disk, theta_open) * laplace_part_latency_distribution(s, lambd_disk, theta_meta) * laplace_b1(s)
                if responsedata is True:
                    laplace_sojourntime *= laplace_part_latency_distribution(s, lambd_disk, theta_data)
            return laplace_sojourntime

        def laplace_bp_pdf1(s):
            # laplace_bp = np.exp(-bp * s)
            laplace_bp = (1 / (1+0.5*s))
            return laplace_bp

        def laplace_bp_pdf2(s):
            # laplace_bp = np.exp(-bp * s)
            laplace_bp = (1 / (1+1.0*s))
            return laplace_bp

        def laplace_bp_pdf3(s):
            alpha_bp = 0.40321444078574364
            beta_bp = alpha_bp / 2.0
            laplace_bp = (beta_bp)**(alpha_bp) * (s+beta_bp)**(-alpha_bp)
            return laplace_bp

        def laplace_bp_pdf_exp(s):
            # laplace_bp = np.exp(-bp * s)
            laplace_bp = (1 / (1 + bp * s))
            return laplace_bp

        def laplace_bp_pdf_determin(s):
            laplace_bp = np.exp(-bp * s)
            return laplace_bp

        def laplace_bp_pdf_logistic(s):
            location = bp
            scale = 0.2980952087304305
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf_logistic_ondemand(s):
            scales = {"192.168.3.131:6000": 0.7535452873795191, "192.168.3.132:6000":  0.7534572439147595,
                      "192.168.3.137:6000": 0.767393652700738, "192.168.3.138:6000": 0.7492503830322381}
            scales = {"192.168.3.131:6000": 0.6306146695492518, "192.168.3.132:6000":  0.6737770941272766,
                      "192.168.3.137:6000": 0.6350972540152318, "192.168.3.138:6000": 0.6634937192911066}
            location = bp
            scale = scales[device_name]
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf_logistic_performance(s):
            scales = {"192.168.3.131:6000": 0.4193826181085095, "192.168.3.132:6000":  0.40504827887155465,
                      "192.168.3.137:6000": 0.5385209183156736, "192.168.3.138:6000": 0.5422830955719604}
            location = bp
            scale = scales[device_name]
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf_logistic_performance_union(s):
            scale = 0.47738487306528626
            location = bp
            laplace_bp = np.exp(location/scale) * (1 / (1+np.exp(location/scale)) - s*scale*gamma(1+s*scale)*hyp2f1(1, 1+s*scale, 2+s*scale, -np.exp(location/scale))/gamma(2+s*scale))
            return laplace_bp

        def laplace_bp_pdf(s):
            if proxy_mode == 'exp':
                return laplace_bp_pdf_exp(s)
            elif proxy_mode == 'determin':
                return laplace_bp_pdf_determin(s)
            elif proxy_mode == 'logistic':
                return laplace_bp_pdf_logistic(s)
            elif proxy_mode == 'logistic_ondemand':
                return laplace_bp_pdf_logistic_ondemand(s)
            elif proxy_mode == 'logistic_performance':
                return laplace_bp_pdf_logistic_performance(s)
            elif proxy_mode == 'logistic_performance_union':
                return laplace_bp_pdf_logistic_performance_union(s)
            else:
                return eval('laplace_bp_pdf_%s(s)' % (proxy_mode))

        def laplace_proxy_queue_pdf(s):
            # lambd = lambd_proxy / (npn * npp)
            # lambd = lambd_proxy * 2 / (npn)
            # rho1 = lambd * 0.5
            # laplace_queue1 = ((1 - rho1) * laplace_bp_pdf1(s) * s) / (lambd * laplace_bp_pdf1(s) + s - lambd)
            # rho2 = lambd * 1.0
            # laplace_queue2 = ((1 - rho2) * laplace_bp_pdf2(s) * s) / (lambd * laplace_bp_pdf2(s) + s - lambd)
            # rho3 = lambd * 2.0
            # laplace_queue3 = ((1 - rho3) * laplace_bp_pdf3(s) * s) / (lambd * laplace_bp_pdf3(s) + s - lambd)
            lambd = lambd_proxy / (npn * npp)
            rho = lambd * bp
            laplace_queue = ((1 - rho) * laplace_bp_pdf(s) * s) / (lambd * laplace_bp_pdf(s) + s - lambd)
            return laplace_queue

        def laplace_proxy_sojourntime_pdf(s):
            laplace_proxy_sojourntime = laplace_proxy_queue_pdf(s) * laplace_sojourntime_pdf(s) * laplace_waittime_pdf(s)
            return laplace_proxy_sojourntime

        def laplace_proxy_sojourntime_noaccept_pdf(s):
            laplace_proxy_sojourntime_noaccept = laplace_proxy_queue_pdf(s) * laplace_sojourntime_pdf(s)
            return laplace_proxy_sojourntime_noaccept

        def laplace_proxy_sojourntime_cdf(s):
            return laplace_proxy_sojourntime_pdf(s) / s

        def laplace_proxy_sojourntime_noaccept_cdf(s):
            return laplace_proxy_sojourntime_noaccept_pdf(s) / s

        if noaccept is True:
            p = numerical_inverse_laplace_transform(laplace_proxy_sojourntime_noaccept_cdf, t, n=6)
        else:
            p = numerical_inverse_laplace_transform(laplace_proxy_sojourntime_cdf, t, n=6)
        if diskio is not None:
            if diskio * b2 / 1000.0 > 1.0:
                p = 0

        return p if p <=1 and p >= 0 else (1 if p >= 1 else 0)

    m = None
    if no == 1:
        m = odopr_model_1obj
    else:
        m = odopr_model_nobj

    return m