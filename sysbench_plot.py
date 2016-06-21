#!/usr/bin/env python3
"""
A helper program to parse the sysbench logs and plot graphs.
Usage:
    ./sysbench_plot.py -p prefix log_file(s)
"""
import sys
from collections import defaultdict
import re
import os
import argparse
import numpy as np
import matplotlib
from matplotlib import pyplot as plt


def parse_log(log_file, log_type):
    """
    The unction to parse sysbench log and return tuples(tps, response time) as a generator.
    The log sample:
    **sysbench:
    [   1s] threads: 16, tps: 3410.72, reads: 0.00, writes: 13661.87, response time: 9.69ms (95%), errors: 0.00, reconnects:  0.00

    **iostat:
    Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util
    nvme0n1           0.00     0.00    0.00    0.00     0.00     0.00     0.00     0.00    0.00    0.00    0.00   0.00   0.00

    :param log_file:
    :param log_type:
    :return:
    """
    ptn_list = {'sb': r'\[\s*(?P<sec>.*?)s\].*tps: (?P<tps>.*?),.*response time: (?P<rt>.*?)ms',
                'iostat': r'^(?P<device>[^:]+?)\s+'
                          r'(?P<rrqm>[\d\.]+)\s+'
                          r'(?P<wrqm>[\d\.]+)\s+'
                          r'(?P<rs>[\d\.]+)\s+'
                          r'(?P<ws>[\d\.]+)\s+'
                          r'(?P<rmbs>[\d\.]+)\s+'
                          r'(?P<wmbs>[\d\.]+)\s+'
                          r'(?P<avgrqsz>[\d\.]+)\s+'
                          r'(?P<avgquz>[\d\.]+)\s+'
                          r'(?P<await>[\d\.]+)\s+'
                          r'(?P<rawait>[\d\.]+)\s+'
                          r'(?P<wawait>[\d\.]+)\s+'
                          r'(?P<svctm>[\d\.]+)\s+'
                          r'(?P<util>[\d\.]+)',
                'mpstat': r'^(?:.*all)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)\s+(\S+)'
                          r'\s+(\S+)\s+(\S+)\s+(\S+)',
                'vmstat': r'^(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+'
                          r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)',
                'tdctl': r'^[\d.]+\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)[\s-]+(\w*)'
                }

    ptn = re.compile(ptn_list.get(log_type))

    try:
        with open(log_file) as log:
            for line in log:
                match = ptn.findall(line)
                if len(match) > 0:
                    yield match[0]
    except FileNotFoundError as e:
        print(e)
        raise


def plot(type, data, plotfile, prefix=''):
    if type == 'sb':
        plot_sb(data, plotfile, prefix)
    elif type == 'iostat':
        plot_iostat(data, plotfile, prefix)
    elif type == 'mpstat':
        plot_mpstat(data, plotfile, prefix)
    elif type == 'vmstat':
        plot_vmstat(data, plotfile, prefix)
    elif type == 'tdctl':
        plot_tdctl(data, plotfile, prefix)
    else:
        print('Invalid plot type: {}'.format(type))
        pass


def plot_tdctl(data, plotfile, prefix):
    """
    Sample tdctl data:
    Time(s)               IOPS   Rd MB/s   Wr MB/s        Lat(us)     Warn    Error
    1466515297.647           0      0.00      0.00          0.000        0        0  - tda
    1466515297.647           0      0.00      0.00          0.000        0        0  - tdb
    1466515297.647           0      0.00      0.00          0.000        0        0  - tdc
    1466515297.647           0      0.00      0.00          0.000        0        0  - tdd
    1466515297.647           0      0.00      0.00          0.000        0        0  - tde
    1466515297.647           0      0.00      0.00          0.000        0        0  - tdf
    1466515297.647           0      0.00      0.00          0.000        0        0  - tdg
    1466515297.647           0      0.00      0.00          0.000        0        0  - tdh
    1466515297.647           0      0.00      0.00          0.000        0        0
    """
    assert data is not None
    matplotlib.rcParams.update({'font.size': 60})
    plt.figure(figsize=(100, 60))

    tdctl_data = defaultdict(list)
    for *value, device in data:
        if not device:
            device = 'total'
        for i in range(len(value)):
            value[i] = float(value[i])
        tdctl_data[device].append(value)

    for key in tdctl_data.keys():
        tdctl_data[key] = list(zip(*tdctl_data[key]))

    sec = range(0, len(tdctl_data['total'][0]) * 10, 10)
    title = '{}_tdctl'.format(prefix)

    # Plot IOPS
    plt.subplot(2, 2, 1)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][0], label=key)

    plt.xlabel('seconds')
    plt.ylabel('IOPS')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('IOPS', fontsize=60, fontweight='bold')

    # Plot Read MBPS
    plt.subplot(2, 2, 2)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][1], label=key)

    plt.xlabel('seconds')
    plt.ylabel('MB/s')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('Read MB/s', fontsize=60, fontweight='bold')

    # Plot Write MBPS
    plt.subplot(2, 2, 3)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][2], label=key)

    plt.xlabel('seconds')
    plt.ylabel('MB/s')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('Write MB/s', fontsize=60, fontweight='bold')

    # Plot Latency
    plt.subplot(2, 2, 4)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][3], label=key)

    plt.xlabel('seconds')
    plt.ylabel('latency(us)')
    plt.xlim([0, sec[-1]])
    flat_lat = [i for i in tdctl_data[key][3] for key in tdctl_data.keys()]
    max_lat = max(flat_lat)
    lat_ylim = np.percentile(np.array(flat_lat), 99)
    plt.ylim([0, lat_ylim])
    plt.legend(fontsize=60)
    plt.title('Latency, max={}us'.format(max_lat), fontsize=60, fontweight='bold')

    fig = plt.gcf()
    fig.suptitle(title, fontsize=120, fontweight='bold')
    plt.grid(True)

    plt.savefig(plotfile)
    plt.close()

    # Plot the following graph iff there are warnings and/or errors.
    flat_warn = sum(i for i in tdctl_data[key][4] for key in tdctl_data.keys())
    flat_err = sum(i for i in tdctl_data[key][5] for key in tdctl_data.keys())

    if flat_warn or flat_err:
        # Plot Write MBPS
        plt.subplot(1, 1, 1)
        for key in tdctl_data.keys():
            plt.plot(sec, tdctl_data[key][4], label=key+'_Warn')
            plt.plot(sec, tdctl_data[key][5], label=key + '_Err')

        plt.xlabel('seconds')
        plt.ylabel('count')
        plt.xlim([0, sec[-1]])
        plt.legend(fontsize=60)
        plt.title('Warnings and Errors', fontsize=60, fontweight='bold')
        fig = plt.gcf()
        fig.suptitle(title, fontsize=120, fontweight='bold')
        plt.grid(True)

        plt.savefig(plotfile)
        plt.close()


def plot_vmstat(data, plotfile, prefix):
    assert data is not None
    matplotlib.rcParams.update({'font.size': 60})
    plt.figure(figsize=(100, 60))

    vmstat_data = list(zip(*data))
    metrics = ['r', 'b',
               'swpd', 'free', 'buff', 'cache',
               'si', 'so',
               'bi', 'bo',
               'in_', 'cs',
               'us', 'sy', 'id', 'wa', 'st']
    sec = range(0, len(vmstat_data[0]) * 10, 10)
    title = '{}_vmstat'.format(prefix)

    for i in range(len(metrics)):
        vmstat_data[i] = [float(x) for x in vmstat_data[i]]

    plt.subplot(3, 2, 1)
    plt.plot(sec, vmstat_data[0], label='r')
    plt.plot(sec, vmstat_data[1], label='b')
    plt.xlabel('seconds')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('procs', fontsize=60, fontweight='bold')

    plt.subplot(3, 2, 2)
    plt.plot(sec, vmstat_data[2], label='swpd')
    plt.plot(sec, vmstat_data[3], label='free')
    plt.plot(sec, vmstat_data[4], label='buff')
    plt.plot(sec, vmstat_data[5], label='cache')
    plt.xlabel('seconds')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('memory', fontsize=60, fontweight='bold')

    plt.subplot(3, 2, 3)
    plt.plot(sec, vmstat_data[6], label='si')
    plt.plot(sec, vmstat_data[7], label='so')
    plt.xlabel('seconds')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('swap', fontsize=60, fontweight='bold')

    plt.subplot(3, 2, 4)
    plt.plot(sec, vmstat_data[8], label='bi')
    plt.plot(sec, vmstat_data[9], label='bo')
    plt.xlabel('seconds')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('io', fontsize=60, fontweight='bold')

    plt.subplot(3, 2, 5)
    plt.plot(sec, vmstat_data[10], label='in')
    plt.plot(sec, vmstat_data[11], label='cs')
    plt.xlabel('seconds')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('system', fontsize=60, fontweight='bold')

    plt.subplot(3, 2, 6)
    plt.plot(sec, vmstat_data[12], label='us')
    plt.plot(sec, vmstat_data[13], label='sy')
    plt.plot(sec, vmstat_data[14], label='id')
    plt.plot(sec, vmstat_data[15], label='wa')
    plt.plot(sec, vmstat_data[16], label='st')
    plt.xlabel('seconds')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=60)
    plt.title('cpu', fontsize=60, fontweight='bold')

    fig = plt.gcf()
    fig.suptitle(title, fontsize=120, fontweight='bold')
    plt.grid(True)

    plt.savefig(plotfile)
    plt.close()


def plot_mpstat(data, plotfile, prefix=''):
    assert data is not None
    matplotlib.rcParams.update({'font.size': 9})

    user, nice, sys, iowait, irq, soft, steal, guest, gnice, idle = zip(*data)
    mpstat_data = [user, nice, sys, iowait, irq, soft, steal, guest, gnice, idle]
    metrics = ['%user', '%nice', '%sys', '%iowait', '%irq', '%soft', '%steal', '%guest', '%gnice', '%idle']

    for i in range(len(metrics)):
        mpstat_data[i] = [float(x) for x in mpstat_data[i]]

    mpstat = np.row_stack(mpstat_data)
    sec = range(0, len(mpstat_data[0]) * 10, 10)
    colors = ['b', 'g', 'r', 'c', 'm', 'y', 'k', 'Yellow', 'Cornsilk', 'DarkSlateGray']

    fig, ax = plt.subplots()
    plt.xlim([0, sec[-1]])
    plt.ylim([0, 105])
    plt.xlabel('seconds')
    plt.ylabel('%')
    plt.title('{}_mpstat'.format(prefix), y=1.05, fontsize=10, fontweight='bold')

    polys = ax.stackplot(sec, mpstat, edgecolor='white', colors=colors)
    legendProxies = []

    for poly in polys:
        legendProxies.append(plt.Rectangle((0, 0), 1, 1, fc=poly.get_facecolor()[0]))

    plt.legend(legendProxies, metrics, loc='upper center', bbox_to_anchor=(0.5, 1.05),
          ncol=5, fancybox=True, shadow=True, prop={'size': 9})

    plt.savefig(plotfile)
    plt.close()


def plot_iostat(data, plotfile, prefix=''):
    """
    Device:         rrqm/s   wrqm/s     r/s     w/s    rMB/s    wMB/s avgrq-sz avgqu-sz   await r_await w_await  svctm  %util

    :param data:
    :param plotfile:
    :param prefix:
    :return:
    """
    assert data is not None
    matplotlib.rcParams.update({'font.size': 60})
    # matplotlib.rcParams['figure.figsize'] = 40, 60
    plt.figure(figsize=(100, 60))
    try:
        device, _, _, rs, ws, rmbs, wmbs, avgrqsz, avgqusz, _, rawait, wawait, svctm, util = zip(*data)
        iostat_data = [rs, ws, rmbs, wmbs, avgrqsz, avgqusz, rawait, wawait, svctm, util]
        metrics = ['r/s', 'w/s', 'rMB/s', 'wMBs', 'avgrq-sz', 'avgqu-sz', 'r_await', 'w_await', 'svctm', 'util']
        sec = range(0, len(iostat_data[0]) * 10, 10)
        # title = 'iostat of {}'.format(device[0])

    except ValueError as e:
        print(e)
        raise

    for i in range(len(metrics)):
        iostat_data[i] = [float(x) for x in iostat_data[i]]
        plt.subplot(5, 2, i+1)
        plt.plot(sec, iostat_data[i])
        plt.title(metrics[i])
        plt.ylabel(metrics[i])
        plt.xlim([0, sec[-1]])
        plt.grid(True)

    fig = plt.gcf()
    fig.suptitle("{}_iostat".format(prefix), fontsize=120, fontweight='bold')

    plt.savefig(plotfile)
    plt.close()


def plot_sb(data, plotfile, prefix=''):
    """
    The function to plot the data extracted by parse_log()
    :param data:
    :param plotfile:
    :return:
    """
    try:
        sec, tps, rt = zip(*data)
    except ValueError as e:
        print(e)
        raise

    sec = [int(x) for x in sec]
    tps = [float(x) for x in tps]
    rt = [float(x) for x in rt]

    sec_max = sec[-1]
    rt_max = max(rt)
    tps_max = max(tps)
    rt_avg = sum(rt) / float(len(rt))
    tps_avg = sum(tps) / float(len(tps))

    # Sometimes a spike makes the major part invisible ...
    rt_ylim = np.percentile(np.array([float(x) for x in rt]), 99.9)

    matplotlib.rcParams.update({'font.size': 10})

    plt.subplot(211)
    plt.plot(sec, tps)
    plt.title('TPS({}) (max={}, avg={:.2f})'.format(prefix + plotfile.split('.')[0], tps_max, tps_avg),
              fontsize=10, fontweight='bold')
    plt.ylabel('tps')
    plt.xlim([0, sec_max])
    plt.grid(True)

    plt.subplot(212)
    plt.plot(sec, rt)
    plt.title('Response Time(ms) (max={}, avg = {:.2f})'.format(rt_max, rt_avg),
              fontsize=10, fontweight='bold')
    plt.xlim([0, sec_max])
    plt.ylabel('ms')
    plt.ylim([0, rt_ylim])
    plt.grid(True)

    plt.xlabel('seconds')
    plt.savefig(plotfile)
    plt.close()


if __name__ == '__main__':
    # Get the file name from the first parameter, it's better to use argparse here.
    # Deprecated: logfile_names = sys.argv[1:]

    parser = argparse.ArgumentParser(description="The Utility to plot your sysbench logs.")
    parser.add_argument("-p", help="the prefix of the title of the graphs", default='')
    parser.add_argument("files", nargs='*', help="the files to plot")


    args = parser.parse_args()

    logfile_names = args.files
    prefix = args.p

    for file_name in logfile_names:
        plot_type = os.path.basename(file_name).split('_')[0]
        log_data = parse_log(file_name, plot_type)
        plot_file = file_name.split('.')[0] + '.png'

        plot(plot_type, log_data, plot_file, prefix)

