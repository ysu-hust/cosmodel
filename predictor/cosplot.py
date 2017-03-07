def plot_estimation_res(df, titles, key_set, label_set, fig_file, fig_size=6.5, max_x=-1, trace_workload=None):
    import numpy as np
    import pandas as pd
    import matplotlib.pyplot as plt
    import seaborn as sns

    comparison_min_diffs = {} # {label_name: min_diff}
    comparison_max_diffs = {}
    comparison_mean_diffs = {}

    if trace_workload is not None:
        font_size = 7
        axes_labelsize = 7
        axes_titlesize = 7
        xtick_labelsize = 6
        ytick_labelsize = 6
        legend_fontsize = 7
        lines_markersize = 6
        fig_hight = 1.7
        hight_ratio1 = 4.5
        hight_ratio2 = 1
    else:
        font_size = 8
        axes_labelsize = 8
        axes_titlesize = 8
        xtick_labelsize = 6
        ytick_labelsize = 6
        legend_fontsize = 8
        lines_markersize = 6
        fig_hight = 1.8
        hight_ratio1 = 4
        hight_ratio2 = 1

    sns.set_context("paper")
    sns.set(style="ticks",
            rc={"xtick.major.size": 1,
                "xtick.minor.size": 0,
                "ytick.major.size": 1,
                "xtick.direction":"in",
                "ytick.direction":"in",
                "font.size": font_size,
                "axes.labelsize": axes_labelsize,
                "axes.titlesize": axes_titlesize,
                "xtick.labelsize": xtick_labelsize,
                "ytick.labelsize": ytick_labelsize,
                "legend.fontsize": legend_fontsize,
                "lines.markersize": lines_markersize,
                # "lines.markeredgewidth": 25,
                # "lines.fillstyle": 'none',
                "lines.linewidth": 1,
                # "figure.autolayout": True,
                "figure.figsize": np.array([fig_size, fig_hight])})

    colors = sns.color_palette("hls", 8)
    measured_color = colors[0]
    estimated_color = colors[2]
    workload_color = colors[4]
    estimated_without_WTA_color = colors[2]
    estimated_without_IM_color = colors[5]
    estimated_without_DC_color = colors[3]
    markers = ['v', '*', 's', 'o', '>']

    fig, axes = plt.subplots(nrows=2, ncols=len(titles), sharex='col', sharey='row', gridspec_kw = {'height_ratios':[hight_ratio1, hight_ratio2]})

    i = 0
    min_diffes = []
    max_diffes = []
    mean_diffes = []
    for title, keys, labels in zip(titles, key_set, label_set):
        if len(titles) > 1:
            ax1 = axes[0, i]
            ax2 = axes[1, i]
        else:
            ax1 = axes[0]
            ax2 = axes[1]
        x, y, z = keys[0], keys[1], keys[2]
        xlabel, ylabel = labels[0], labels[1]
        comparison_min_diffs[ylabel] = df[z].apply(lambda x: abs(x)).min()
        comparison_max_diffs[ylabel] = df[z].apply(lambda x: abs(x)).max()
        comparison_mean_diffs[ylabel] = df[z].apply(lambda x: abs(x)).mean()
        # "lines" is used for union legend of ax1 and ax1t
        lines = ax1.plot(df[x].dropna(), marker='v', label=xlabel, color=measured_color, markevery=5)
        if len(labels) > 2:
            start_idx = 3
            m_idx = 2
            c_idx = 1
            for ylabelext in labels[2:]:
                yext, zext = keys[start_idx], keys[start_idx+1]
                comparison_min_diffs[ylabelext] = df[zext].apply(lambda x: abs(x)).min()
                comparison_max_diffs[ylabelext] = df[zext].apply(lambda x: abs(x)).max()
                comparison_mean_diffs[ylabelext] = df[zext].apply(lambda x: abs(x)).mean()
                colorext, markerext = colors[c_idx], markers[m_idx]
                line = ax1.plot(df[yext].dropna(), marker=markerext, label=ylabelext, color=colorext, markevery=5)
                lines = lines + line
                start_idx += 2
                m_idx += 1
                c_idx += 4
        line = ax1.plot(df[y].dropna(), marker='*', label=ylabel, color=estimated_color, markevery=5)
        lines = lines + line
        if trace_workload is not None:
            ax1_loc = "center left"
            # ax1_loc = "best"
            ax1_borderpad = 0.1
            ax1_labelspacing = 0.1
            ax1_handlelength = None
            ax1_handletextpad = 0.25
            ax1_borderaxespad = 0.1
            ax1_columnspacing = 0.1
        else:
            ax1_loc = "best"
            ax1_borderpad = 0.1
            ax1_labelspacing = 0.3
            ax1_handlelength = None
            ax1_handletextpad = 0.25
            ax1_borderaxespad = 0.1
            ax1_columnspacing = 0.1
        if trace_workload is None:
            ax1.legend(loc=ax1_loc, shadow=True, fancybox=True, borderpad=ax1_borderpad, labelspacing=ax1_labelspacing, handlelength=ax1_handlelength, handletextpad=ax1_handletextpad, borderaxespad=ax1_borderaxespad, columnspacing=ax1_columnspacing)
        ax1.set_ylabel('Percentile')
        if max_x > 0:
            ax1.set_xlim([0, max_x])
        # ax1.set_title(title)
        # ax1.set_ylim([0.5, 1.0])
        # ax1.set_ylim(ymax=1.01)
        # ax1.set_ylim([0.0, 1.0])
        if trace_workload is not None:
            ax1t = ax1.twinx()
            line = ax1t.plot(trace_workload, label='Workload-R16', linestyle='-', color=workload_color)
            lines = line + lines
            ax1t.set_ylabel('Request Arriving Rate (reqs/s)')
            ax1t_loc = "center right"
            # ax1t_loc = "best"
            ax1t_borderpad = 0.1
            ax1t_labelspacing = 0.1
            ax1t_handlelength = None
            ax1t_handletextpad = 0.25
            ax1t_borderaxespad = 0.1
            ax1t_columnspacing = 0.1
            # ax1t.legend(loc=ax1t_loc, shadow=True, fancybox=True, borderpad=ax1t_borderpad, labelspacing=ax1t_labelspacing, handlelength=ax1t_handlelength, handletextpad=ax1t_handletextpad, borderaxespad=ax1t_borderaxespad, columnspacing=ax1t_columnspacing)
            ax1_labels = [l.get_label() for l in lines]
            ax1.legend(lines, ax1_labels, loc="center left", shadow=False, fancybox=False, borderpad=ax1_borderpad, labelspacing=ax1_labelspacing, handlelength=1, handletextpad=ax1_handletextpad, borderaxespad=ax1_borderaxespad, columnspacing=ax1_columnspacing)
        # draw error of the prediction of the model without WTA
        enable_draw_error_of_nowta = False
        if len(labels) > 2 and enable_draw_error_of_nowta:
            df[zna].dropna().plot.bar(ax=ax2, rot=0, color=estimated_without_WTA_color,  hatch="////////////////", label="error_without_WTA")
        df[z].dropna().plot.bar(ax=ax2, rot=0, color=estimated_color,  hatch="\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\\", label="Error of Our Model")
        ax2.set_xlabel('Time(5 mins)')
        ax2.set_ylabel('Percentile')
        xticks = range(0, df[z].count(), 15)
        ax2.set_xticks(xticks)
        ax2.set_xticklabels(xticks, rotation=0)
        ax2.set_yticks([-0.2, 0, 0.2])
        ax2.set_ylim([-0.20, 0.20])
        if max_x > 0:
            ax2.set_xlim([0, max_x])
        if trace_workload is not None:
            ax2_loc = "upper center"
        else:
            ax2_loc = "upper center"
        ax2_borderpad = 0.02
        ax2_labelspacing = 0.1
        ax2_handlelength = None
        ax2_handletextpad = 0.25
        ax2_borderaxespad = 0.1
        ax2_columnspacing = 0.1
        ax2.legend(loc=ax2_loc, shadow=True, fancybox=True,borderpad=ax2_borderpad, labelspacing=ax2_labelspacing, handlelength=ax2_handlelength, handletextpad=ax2_handletextpad, borderaxespad=ax2_borderaxespad, columnspacing=ax2_columnspacing)
        i += 1
        min_diffes.append(df[z].apply(lambda x: abs(x)).min())
        max_diffes.append(df[z].apply(lambda x: abs(x)).max())
        mean_diffes.append(df[z].apply(lambda x: abs(x)).mean())
    # plt.suptitle('Storage Server SLO estimation')
    fig.tight_layout(pad=0.17)
    fig.subplots_adjust(hspace=0.1, wspace=0.05)
    fig.savefig(fig_file+'.pdf', format='pdf')
    plt.close('all')
    if 'sys' in fig_file:
        with open('./comparison_prediction_error.csv', 'a+') as f:
            for lab in label_set.pop():
                if lab == xlabel:
                    continue
                line = '%s,%s,%f,%f,%f\n' % (fig_file, lab, comparison_min_diffs[lab], comparison_max_diffs[lab], comparison_mean_diffs[lab])
                f.write(line)
    return (min(min_diffes), max(max_diffes), sum(mean_diffes) / float(len(mean_diffes)))
