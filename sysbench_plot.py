#!/usr/bin/env python3
"""
A helper program to parse the sysbench logs and plot graphs.
Usage:
    ./sysbench_plot.py -p prefix log_file(s)
"""
import sys
import re
import argparse
import numpy as np
import matplotlib
from matplotlib import pyplot as plt


def parse_log(log_file):
    """
    The unction to parse sysbench log and return tuples(tps, response time) as a generator.
    The log sample:
    "[   1s] threads: 16, tps: 3410.72, reads: 0.00, writes: 13661.87, response time: 9.69ms\
    (95%), errors: 0.00, reconnects:  0.00'
    :param log_file:
    :return: A generator producing a tuple (sec, tps, rt) for each call.
    """
    ptn_str = r'\[\s*(?P<sec>.*?)s\].*tps: (?P<tps>.*?),.*response time: (?P<rt>.*?)ms'
    ptn = re.compile(ptn_str)

    try:
        with open(log_file) as log:
            for line in log:
                match = ptn.findall(line)
                if len(match) > 0:
                    yield match[0]
    except FileNotFoundError as e:
        print(e)
        raise


def plot(tps_data, plot_filename, prefix=''):
    """
    The function to plot the data extracted by parse_log()
    :param tps_data:
    :param plot_filename:
    :return:
    """
    try:
        sec, tps, rt = zip(*tps_data)
    except ValueError as e:
        print(e)
        raise

    sec = [int(x) for x in sec]
    tps = [float(x) for x in tps]
    rt = [float(x) for x in rt]

    sec_max = sec[-1]
    rt_max = max(rt)
    tps_max = max(tps)
    rt_avg = sum(rt)/float(len(rt))
    tps_avg = sum(tps)/float(len(tps))

    # Sometimes a spike makes the major part invisible ...
    rt_ylim = np.percentile(np.array([float(x) for x in rt]), 99.9)

    matplotlib.rcParams.update({'font.size': 10})

    plt.subplot(211)
    plt.plot(sec, tps)
    plt.title('TPS({}) (max={}, avg={:.2f})'.format(prefix + plot_filename.split('.')[0], tps_max, tps_avg),
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
    plt.savefig(plot_filename)
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

    try:
        for file_name in logfile_names:
            log_data = parse_log(file_name)
            plot_file = file_name.split('.')[0] + '.png'
            plot(log_data, plot_file, prefix)
    except Exception:
        print('Error occured, see error stack above.')
        sys.exit(1)
