#!/usr/bin/env python3
"""
This program gathers the data of tps (transactions per seconds) and rt (response time)
and draw Excel charts.
"""
import os
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.chart import BarChart, Reference
import numpy


def parse_sb_log(log_file):
    """
    This function parse a sysbench log file and return the tps and rt to the caller.
    :param log_file:  must be an absolute path otherwise it may complain
    :return:
    """
    tps, rt, avg_rt = 0, 0, 0
    tps_arr, rt_arr = [], []

    with open(log_file) as file:
        for line in file:
            if line.startswith('['):
                tps_item = float(line.split('tps: ')[1].split(', reads:')[0])
                rt_item = float(line.split('response time: ')[1].split('ms ')[0])
                tps_arr.append(tps_item)
                rt_arr.append(rt_item)
            elif 'transactions' in line:
                tps = float(line.split('(')[1].split()[0])
            elif '95 percentile' in line:
                rt = float(line.split()[-1].strip('ms'))
            elif 'avg:' in line:
                avg_rt = float(line.split()[1].strip('ms'))

    tps_std = float(numpy.std(numpy.array(tps_arr), axis=0))
    rt_std = float(numpy.std(numpy.array(rt_arr), axis=0))

    if 'DMX_RWD_3_64' in log_file:
        print(rt_arr)
        print(rt_std)
    return tps, rt, avg_rt, tps_std, rt_std


def get_tps_rt(dir_name):
    """
    This function accepts a directory name, parses the logs in it, and return a tuple
    with the format of ('benchmark name', tps, rt). Both the tps and the rt are numbers.
    :param dir_name:
    :return:
    """
    tps_list, rt_list, avg_rt_list, tps_std_list, rt_std_list = [], [], [], [], []

    for file in os.listdir(dir_name):
        abspath = os.path.join(dir_name, file)
        if file.startswith('sb_') and file.endswith('.log') and os.path.isfile(abspath):
            sub_tps, sub_rt, sub_avg_rt, sub_tps_std, sub_rt_std = parse_sb_log(abspath)
            tps_list.append(sub_tps)
            rt_list.append(sub_rt)
            avg_rt_list.append(sub_avg_rt)
            tps_std_list.append(sub_tps_std)
            rt_std_list.append(sub_rt_std)

    tps = sum(tps_list) / len(tps_list) if len(tps_list) else 0
    rt = sum(rt_list) / len(rt_list) if len(rt_list) else 0
    avg_rt = sum(avg_rt_list) / len(avg_rt_list) if len(avg_rt_list) else 0
    tps_std = sum(tps_std_list) / len(tps_std_list) if len(tps_std_list) else 0
    rt_std = sum(rt_std_list) / len(rt_std_list) if len(rt_std_list) else 0

    title = os.path.basename(os.path.normpath(dir_name))
    return [title, tps, rt, avg_rt, tps_std, rt_std]


def parse_all_sblogs(base_dir):
    """
    This function will parse all the directories under base_dir, no matter whether
    it contains benchmark logs - so don't put other directories under base_dir.
    A dict like {thread_num: ['Thread', thread_num, thread_num], ['Benchmark name',
    tps, rt], ...} is returned to the caller
    :param base_dir:
    :return:
    """
    sblog_dict = defaultdict(lambda: [['Thread', 0, 0, 0, 0, 0]])

    for name in sorted(os.listdir(base_dir), reverse=True):
        abspath = os.path.join(base_dir, name)
        if os.path.isdir(abspath):
            # print(name, end=' ')
            thread_num = name.split('_')[-1]
            sblog_dict[thread_num].append(get_tps_rt(abspath))

    for key in sblog_dict.keys():
        sblog_dict[key][0] = ['Thread', key, key, key, key, key]

    return sblog_dict


def data_cleansing(sblog_dict):
    """
    For a benchmark matrix, fill the missing data as all zeros.
    Warning: this function modifies the input parameter!
    :param sblog_dict:
    :return:
    """
    # print(sblog_dict)
    title_set = set()
    for vals in sblog_dict.values():
        for unit in vals:
            title_set.add(unit[0])

    for key in sblog_dict.keys():
        titles = set(x[0] for x in sblog_dict[key])
        for title in title_set - titles:
            sblog_dict[key].append([title, 0, 0, 0, 0, 0])

    # print(sblog_dict)
    return sblog_dict


def get_sheetname_by_workload(workload_type):
    """
    Get sheet name by workload type
    :param workload_type:
    :return:
    """
    wl_type_lower = workload_type.lower()

    if wl_type_lower == 'ro':
        sheet_name = wl_type_lower + ' (rw_ratio ' + '1v0)'
    elif wl_type_lower == 'wo':
        sheet_name = wl_type_lower + ' (rw_ratio ' + '0v1)'
    elif wl_type_lower == 'rw':
        sheet_name = wl_type_lower + ' (rw_ratio ' + '6v4)'
    elif wl_type_lower == 'rwd':
        sheet_name = wl_type_lower + ' (rw_ratio ' + '4v6)'
    else:
        sheet_name = wl_type_lower
    return sheet_name


def get_wl_from_sheetname(name):
    return name.split()[0].upper()


def draw_excel_charts(sblog_dict, excel_filename):
    """
    This function accepts a defaultdict which contains all the data of tps and rt
    an will create an Microsoft Excel spreadsheet with charts.
    :param sblog_dict:
    :param excel_filename:
    :return:
    """
    clean_dict = data_cleansing(sblog_dict)

    tps_data, rt_data, avg_rt_data, tps_std_data, rt_std_data = [], [], [], [], []
    workload_cols_rows = {}
    workload_types = set()

    col_name_parsed = False
    for key in clean_dict.keys():
        data_list = clean_dict[key]
        col_name, tps, rt, avg_rt, tps_std, rt_std = zip(*data_list)
        if not col_name_parsed:
            rt_data.append(col_name)
            tps_data.append(col_name)
            avg_rt_data.append(col_name)
            tps_std_data.append(col_name)
            rt_std_data.append(col_name)

            workload_types = set(x.split('_')[1] for x in col_name[1:])

            workload_cols_rows.update({wl_type: {'cols': 0, 'rows': 0} for wl_type in workload_types})
            col_name_parsed = True

        tps_data.append(tps)
        rt_data.append(rt)
        avg_rt_data.append(avg_rt)
        tps_std_data.append(tps_std)
        rt_std_data.append(rt_std)

    # print('tps_data: {}'.format(tps_data))
    # print('rt_data: {}'.format(rt_data))
    # print('avg_rt_data: {}'.format(avg_rt_data))
    wb = Workbook(write_only=True)

    for wl_type in workload_types:
        wb.create_sheet(title=get_sheetname_by_workload(wl_type))

    merged_rows = []
    for tps, rt, avg_rt, tps_std, rt_std in zip(tps_data, rt_data, avg_rt_data, tps_std_data, rt_std_data):
        merged_rows.append(tps + rt + avg_rt + tps_std + rt_std)

    # print(merged_rows)
    # The tps chart:
    # print('merged_rows: {}\n'.format(merged_rows))
    for row in merged_rows:
        for wl_type in workload_types:
            wl_row = [row[i] for i in range(len(row)) if
                      wl_type in merged_rows[0][i].split('_') or i == 0 or merged_rows[0][i] == 'Thread']
            # print('wl_row: {}'.format(wl_row))
            wb.get_sheet_by_name(get_sheetname_by_workload(wl_type)).append(wl_row)

            workload_cols_rows[wl_type]['cols'] = len(wl_row)
            workload_cols_rows[wl_type]['rows'] += 1

    for ws in wb:
        global_max_row = workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows']
        # Chart of TPS
        chart_tps = BarChart(gapWidth=500)
        chart_tps.type = "col"
        chart_tps.style = 10
        chart_tps.title = "TPS chart of {}".format(ws.title)
        chart_tps.y_axis.title = 'tps'
        chart_tps.y_axis.scaling.min = 0
        chart_tps.x_axis.title = 'threads'

        data_tps = Reference(ws, min_col=2, min_row=1,
                             max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'],
                             max_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] / 5)
        cats_tps = Reference(ws, min_col=1, min_row=2,
                             max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'])

        chart_tps.add_data(data_tps, titles_from_data=True)
        chart_tps.set_categories(cats_tps)
        chart_tps.shape = 4
        ws.add_chart(chart_tps, "A{}".format(global_max_row + 5))

        # Chart of Response Time
        chart_rt = BarChart(gapWidth=500)
        chart_rt.type = "col"
        chart_rt.style = 10
        chart_rt.title = "Response Time(95%) chart of {}".format(ws.title)
        chart_rt.y_axis.title = 'rt'
        chart_rt.y_axis.scaling.min = 0
        chart_rt.x_axis.title = 'threads'

        data_rt = Reference(ws, min_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] / 5 + 2,
                            min_row=1,
                            max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'],
                            max_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 2 / 5)
        cats_rt = Reference(ws, min_col=1, min_row=2,
                            max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'])

        chart_rt.add_data(data_rt, titles_from_data=True)
        chart_rt.set_categories(cats_rt)
        chart_rt.shape = 4
        ws.add_chart(chart_rt, "I{}".format(global_max_row + 5))

        # Chart of avg response time
        chart_avg_rt = BarChart(gapWidth=500)
        chart_avg_rt.type = "col"
        chart_avg_rt.style = 10
        chart_avg_rt.title = "Average Response Time chart of {}".format(ws.title)
        chart_avg_rt.y_axis.title = 'avg rt'
        chart_avg_rt.y_axis.scaling.min = 0
        chart_avg_rt.x_axis.title = 'threads'

        data_avg_rt = Reference(ws, min_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 2 / 5 + 2,
                                min_row=1,
                                max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'],
                                max_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 3 / 5)
        cats_avg_rt = Reference(ws, min_col=1, min_row=2,
                                max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'])

        chart_avg_rt.add_data(data_avg_rt, titles_from_data=True)
        chart_avg_rt.set_categories(cats_avg_rt)
        chart_avg_rt.shape = 4
        ws.add_chart(chart_avg_rt, "Q{}".format(global_max_row + 5))

        # Chart of tps standard deviation
        chart_tps_std = BarChart(gapWidth=500)
        chart_tps_std.type = "col"
        chart_tps_std.style = 10
        chart_tps_std.title = "tps standard deviation chart of {}".format(ws.title)
        chart_tps_std.y_axis.title = 'std'
        chart_tps_std.y_axis.scaling.min = 0
        chart_tps_std.x_axis.title = 'threads'

        data_tps_std = Reference(ws, min_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 3 / 5 + 2,
                                min_row=1,
                                max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'],
                                max_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 4 / 5)
        cats_tps_std = Reference(ws, min_col=1, min_row=2,
                                max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'])

        chart_tps_std.add_data(data_tps_std, titles_from_data=True)
        chart_tps_std.set_categories(cats_tps_std)
        chart_tps_std.shape = 4
        ws.add_chart(chart_tps_std, "A{}".format(global_max_row + 20))

        # Chart of response time standard deviation
        chart_rt_std = BarChart(gapWidth=500)
        chart_rt_std.type = "col"
        chart_rt_std.style = 10
        chart_rt_std.title = "response time standard deviation chart of {}".format(ws.title)
        chart_rt_std.y_axis.title = 'std'
        chart_rt_std.y_axis.scaling.min = 0
        chart_rt_std.x_axis.title = 'threads'

        data_rt_std = Reference(ws, min_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 4 / 5 + 2,
                                min_row=1,
                                max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'],
                                max_col=workload_cols_rows[get_wl_from_sheetname(ws.title)]['cols'] * 5 / 5)
        cats_rt_std = Reference(ws, min_col=1, min_row=2,
                                max_row=workload_cols_rows[get_wl_from_sheetname(ws.title)]['rows'])

        chart_rt_std.add_data(data_rt_std, titles_from_data=True)
        chart_rt_std.set_categories(cats_rt_std)
        chart_rt_std.shape = 4
        ws.add_chart(chart_rt_std, "I{}".format(global_max_row + 20))

    wb.save(excel_filename)


if __name__ == '__main__':
    cwd = os.path.basename(os.getcwd())
    sb_dict = parse_all_sblogs('.')
    draw_excel_charts(sb_dict, '{}_summary.xlsx'.format(cwd))
