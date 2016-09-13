#!/usr/bin/env python3
"""
A helper program to parse the sysbench logs and plot graphs.
Written ugly by @Eric Yang
Usage:
    ./sysbench_plot.py -p prefix log_file(s)
"""
from collections import defaultdict
import re
import os
import argparse
import numpy as np
import matplotlib
# Force matplotlib to not use any Xwindow backend.
matplotlib.use('Agg')
from matplotlib import pyplot as plt


def parse_log(log_file, log_type):
    """
    The unction to parse sysbench log and return tuples(tps, response time) as a generator.

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
                'vmstat': r'^\s*(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+'
                          r'(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)',
                'tdctl': r'^[\d.]+\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)\s+([\d.]+)[\s-]+(\w*)',
                'barffr': r'^\s*TOTAL:\s+(\d+)%\s+(\d+)\s+(\d+)\s*$',
                'network': r'^\S+\s+PM\s+(\S+)\s+\S+\s+\S+\s+(\S+)\s+(\S+)\s+'
                }
    ptn = ptn_list.get(log_type)
    if ptn:  # Parsed with re
        ptn = re.compile(ptn)
        try:
            with open(log_file) as log:
                for line in log:
                    match = ptn.findall(line)
                    if len(match) > 0:
                        # print(match[0])
                        yield match[0]
        except FileNotFoundError as e:
            print(e)
            return
    else:  # Parsed by customized functions
        if log_type == 'innodb':
            yield from parse_innodb_status(log_file)
        else:
            print('Unsupported log type: {}'.format(log_file))
            return


def parse_innodb_status(log_file):
    """
    This function parses log files of 'show engine innodb status' to extract information like
    checkpoint lag, dirty buffer ratio, etc.
    :param log_file:
    :return:
    """
    current_lsn = 0
    log_flushed_lsn = 0
    page_flushed_lsn = 0
    checkpoint_lsn = 0
    dirty_pages = 0

    with open(log_file) as file:
        for line in file:
            if line.startswith('Log sequence number'):
                current_lsn = float(line.split()[3])
            elif line.startswith('Log flushed up to'):
                log_flushed_lsn = float(line.split()[4])
            elif line.startswith('Pages flushed up to'):
                page_flushed_lsn = float(line.split()[4])
            elif line.startswith('Last checkpoint at'):
                checkpoint_lsn = float(line.split()[3])
            elif line.startswith('Modified db pages'):
                dirty_pages = float(line.split()[3])

            elif line.startswith('END OF INNODB MONITOR OUTPUT'):
                log_flush_lag = current_lsn - log_flushed_lsn
                page_flush_lag = current_lsn - page_flushed_lsn
                checkpoint_lag = current_lsn - checkpoint_lsn
                yield (log_flush_lag, page_flush_lag, checkpoint_lag, dirty_pages)
            else:
                pass


def plot(p_type, data, plotfile, pre=''):
    """
    Call different plot functions for different types of logs
    :param p_type:
    :param data:
    :param plotfile:
    :param pre:
    :return:
    """
    if data is None:
        return

    if p_type == 'sb':
        plot_sb(data, plotfile, pre)
    elif p_type == 'iostat':
        plot_iostat(data, plotfile, pre)
    elif p_type == 'mpstat':
        plot_mpstat(data, plotfile, pre)
    elif p_type == 'vmstat':
        plot_vmstat(data, plotfile, pre)
    elif p_type == 'tdctl':
        plot_tdctl(data, plotfile, pre)
    elif p_type == 'innodb':
        plot_innodb(data, plotfile, pre)
    elif p_type == 'barffr':
        plot_barf_fr(data, plotfile, pre)
    elif p_type == 'network':
        plot_sar(data, plotfile, pre)
    else:
        print('Skipping: {}'.format(p_type))
        pass


def plot_sar(data, plotfile, prefix):
    """
    Plots the logs of sar to show the network traffic
    :param data:
    :param plotfile:
    :param prefix:
    :return:
    """
    try:
        inet_name, rxkb_s, txkb_s = list(zip(*data))
    except ValueError as e:
        print(e)
        raise
    rxmb_s = [float(x)/1024 for x in rxkb_s]
    txmb_s = [float(x)/1024 for x in txkb_s]
    sec = [10*x for x in range(len(rxmb_s))]
    sec_max = sec[-1]

    y_max = int(max(max(rxmb_s), max(txmb_s)))

    matplotlib.rcParams.update({'font.size': 10})

    plt.subplot(111)
    plt.plot(sec, rxmb_s, label='Received MB/s')
    plt.plot(sec, txmb_s, label='Transmitted MB/s')
    _, name_with_ext = os.path.split(plotfile)
    title_desc, _ = os.path.splitext(name_with_ext)

    plt.title('Network Traffic: {} ({}/{})'.format(inet_name[0], prefix, title_desc),
              fontsize=10, fontweight='bold')
    plt.ylabel('MB/s')
    plt.xlim([0, sec_max])
    plt.ylim([0, y_max*1.5])
    plt.legend(fontsize=8, loc='best')
    plt.grid(True)

    plt.xlabel('seconds')
    plt.savefig(plotfile)
    plt.close()


def plot_barf_fr(data, plotfile, prefix):
    """
    This function plots the log of command: barf --fr
    :param data:
    :param plotfile:
    :param pre:
    :return:
    """
    try:
        pct, free, used = list(zip(*data))
    except ValueError as e:
        print(e)
        raise

    sec = [10*x for x in range(len(free))]
    sec_max = sec[-1]

    mb_max = int(max(free)) + int(min(used))

    matplotlib.rcParams.update({'font.size': 10})

    plt.subplot(211)
    plt.plot(sec, free, label='Free MB')
    plt.plot(sec, used, label='Used MB')
    _, name_with_ext = os.path.split(plotfile)
    title_desc, _ = os.path.splitext(name_with_ext)

    plt.title('barf --fr ({}/{})'.format(prefix, title_desc),
              fontsize=10, fontweight='bold')
    plt.ylabel('MegaBytes')
    plt.xlim([0, sec_max])
    plt.ylim([0, mb_max])
    plt.legend(fontsize=8, loc='best')
    plt.grid(True)

    plt.subplot(212)
    plt.plot(sec, pct, label='Used%')
    plt.title('Used Percentage', fontsize=10, fontweight='bold')
    plt.xlim([0, sec_max])
    plt.ylabel('%')
    plt.ylim([0, 100])
    plt.legend(fontsize=8, loc='best')
    plt.grid(True)

    plt.xlabel('seconds')
    plt.savefig(plotfile)
    plt.close()


def plot_innodb(data, plotfile, prefix):
    """
    This function plots the data extracted from innodb_status_dbx.log
    :param data:
    :param plotfile:
    :param prefix:
    :return:
    """
    # The data is (log_flush_lag, page_flush_lag, checkpoint_lag, dirty_pages)
    try:
        log_flush_lag, page_flush_lag, checkpoint_lag, dirty_pages = list(zip(*data))
    except ValueError as e:
        print(e)
        raise

    sec = [60*x for x in range(len(log_flush_lag))]
    sec_max = sec[-1]

    lag_max = max(log_flush_lag, page_flush_lag, checkpoint_lag)
    dirty_pages_max = max(dirty_pages)

    matplotlib.rcParams.update({'font.size': 10})

    plt.subplot(211)
    plt.plot(sec, log_flush_lag, label='Log flush lag')
    plt.plot(sec, page_flush_lag, label='Dirty page flush lag')
    plt.plot(sec, checkpoint_lag, label='Checkpoint lag')
    _, name_with_ext = os.path.split(plotfile)
    title_desc, _ = os.path.splitext(name_with_ext)

    plt.title('LSN lag({}/{})'.format(prefix, title_desc),
              fontsize=10, fontweight='bold')
    plt.ylabel('LSN lag')
    plt.xlim([0, sec_max])
    plt.legend(fontsize=8, loc='best')
    plt.grid(True)

    plt.subplot(212)
    plt.plot(sec, dirty_pages, label='Dirty pages')
    plt.title('Buffer pool dirty pages', fontsize=10, fontweight='bold')
    plt.xlim([0, sec_max])
    plt.ylabel('pages')
    plt.ylim([0, dirty_pages_max])
    plt.legend(fontsize=8, loc='best')
    plt.grid(True)

    plt.xlabel('seconds')
    plt.savefig(plotfile)
    plt.close()


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
    plt.legend(fontsize=30)
    plt.title('IOPS', fontweight='bold')

    # Plot Read MBPS
    plt.subplot(2, 2, 2)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][1], label=key)

    plt.xlabel('seconds')
    plt.ylabel('MB/s')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=30)
    plt.title('Read MB/s', fontweight='bold')

    # Plot Write MBPS
    plt.subplot(2, 2, 3)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][2], label=key)

    plt.xlabel('seconds')
    plt.ylabel('MB/s')
    plt.xlim([0, sec[-1]])
    plt.legend(fontsize=30)
    plt.title('Write MB/s', fontweight='bold')

    # Plot Latency
    plt.subplot(2, 2, 4)
    for key in tdctl_data.keys():
        plt.plot(sec, tdctl_data[key][3], label=key)

    plt.xlabel('seconds')
    plt.ylabel('latency(us)')
    plt.xlim([0, sec[-1]])
    flat_lat = [i for key in tdctl_data.keys() for i in tdctl_data[key][3]]
    max_lat = max(flat_lat)
    lat_ylim = np.percentile(np.array(flat_lat), 99)
    plt.ylim([0, lat_ylim])
    plt.legend(fontsize=30)
    plt.title('Latency, max={}us'.format(max_lat), fontsize=60, fontweight='bold')

    fig = plt.gcf()
    fig.suptitle(title, fontsize=120, fontweight='bold')
    plt.grid(True)

    plt.savefig(plotfile)
    plt.close()

    # Plot the following graph iff there are warnings and/or errors.
    flat_warn = sum(i for k in tdctl_data.keys() for i in tdctl_data[k][4])
    flat_err = sum(i for err_k in tdctl_data.keys() for i in tdctl_data[err_k][5])

    if flat_warn or flat_err:
        # Plot Write MBPS
        matplotlib.rcParams.update({'font.size': 9})
        plt.figure(figsize=(10, 6))
        plt.subplot(1, 1, 1)
        for key in tdctl_data.keys():
            plt.plot(sec, tdctl_data[key][4], label=key+'_Warn')
            plt.plot(sec, tdctl_data[key][5], label=key+'_Err')

        plt.xlabel('seconds')
        plt.ylabel('count')
        plt.xlim([0, sec[-1]])
        plt.legend(fontsize=6)
        plt.title('Warnings and Errors', fontweight='bold')
        fig = plt.gcf()
        fig.suptitle(title, fontweight='bold')
        plt.grid(True)

        head, tail = os.path.split(plotfile)
        tail = 'errs_' + tail
        err_plotfile = os.path.join(head, tail)
        plt.savefig(err_plotfile)
        plt.close()


def plot_vmstat(data, plotfile, prefix):
    matplotlib.rcParams.update({'font.size': 60})
    plt.figure(figsize=(100, 60))
    vmstat_data = list(zip(*data))

    if not vmstat_data:
        return

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
    legends = []

    for poly in polys:
        legends.append(plt.Rectangle((0, 0), 1, 1, fc=poly.get_facecolor()[0]))

    plt.legend(legends, metrics, loc='upper center', bbox_to_anchor=(0.5, 1.05),
               ncol=5, fancybox=True, shadow=True, prop={'size': 9})

    plt.savefig(plotfile)
    plt.close()


def plot_iostat(data, plotfile, prefix=''):
    """
    Plot the iostat log.
    :param data:
    :param plotfile:
    :param prefix:
    :return:
    """
    assert data is not None
    matplotlib.rcParams.update({'font.size': 60})
    # matplotlib.rcParams['figure.figsize'] = 40, 60
    plt.figure(figsize=(100, 60))
    title = '{}_iostat'.format(prefix)

    try:
        device, _, _, rs, ws, rmbs, wmbs, avgrqsz, avgqusz, _, rawait, wawait, svctm, util = zip(*data)
        iostat_data = [rs, ws, rmbs, wmbs, avgrqsz, avgqusz, rawait, wawait, svctm, util]
        metrics = ['r/s', 'w/s', 'rMB/s', 'wMB/s', 'avgrq-sz', 'avgqu-sz', 'r_await', 'w_await', 'svctm', 'util']
        sec = range(0, len(iostat_data[0]) * 10, 10)
        # title = 'iostat of {}'.format(device[0])

    except ValueError as e:
        print(e)
        raise

    for i in range(len(metrics)):
        iostat_data[i] = [float(x) for x in iostat_data[i]]
        plt.subplot(5, 2, i + 1)
        plt.plot(sec, iostat_data[i])
        plt.title(metrics[i])
        plt.ylabel(metrics[i])
        plt.xlim([0, sec[-1]])
        plt.grid(True)

    fig = plt.gcf()
    fig.suptitle(title, fontsize=120, fontweight='bold')

    plt.savefig(plotfile)
    plt.close()


def plot_sb(data, plotfile, prefix):
    """
    The function to plot the data extracted by parse_log()
    :param data:
    :param plotfile:
    :param prefix:
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
    rt_ylim = np.percentile(np.array([float(x) for x in rt]), 99)

    matplotlib.rcParams.update({'font.size': 10})

    plt.subplot(211)
    plt.plot(sec, tps)
    _, name_with_ext = os.path.split(plotfile)
    title_desc, _ = os.path.splitext(name_with_ext)

    plt.title('TPS({}/{}) (max={}, avg={:.2f})'.format(prefix, title_desc, tps_max, tps_avg),
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
    title_prefix = args.p

    for file_name in logfile_names:
        # [Hard-coded]the first part of the log file name is the type
        plot_type = os.path.basename(file_name).split('_')[0]
        log_data = parse_log(file_name, plot_type)
        plot_file = file_name.split('.')[0] + '.png'

        plot(plot_type, log_data, plot_file, title_prefix)
