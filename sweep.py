#!/usr/bin/env python3
"""This sweep program is used to run MySQL benchmarks with various configurations in client/server.
    Run this command on the client server.
    External dependencies: sysbench_plot.py, cleandb.py
"""
import os
import time
import logging
import psutil
import argparse
import paramiko
from subprocess import Popen, STDOUT, PIPE
from configparser import ConfigParser, NoSectionError, NoOptionError

log = logging.getLogger('')


class Sweep:
    def __init__(self, config_file):
        cnf = ConfigParser()

        try:
            cnf.read(config_file)

            # section: server
            self._db_ip = cnf.get('server', 'db_ip')
            self._db_port = cnf.get('server', 'db_port')
            self._client_ip = cnf.get('server', 'client_ip')
            self._cleandb_path = cnf.get('server', 'cleandb_script_path')

            # section: benchmark
            self._sweep_name = cnf.get('benchmark', 'sweep_name')
            self._sysbench_threads = [int(x.strip())
                                      for x in cnf.get('benchmark', 'sysbench_threads').split(sep=',')]
            self._db_inst_num = cnf.getint('benchmark', 'db_inst_num')
            self._benchmark_target = cnf.get('benchmark', 'benchmark_target')
            self._benchmark_duration = cnf.getint('benchmark', 'benchmark_duration')
            self._db_size = cnf.get('benchmark', 'db_size').rstrip('G')
            self._table_size = self._get_table_size()
            self._table_num = self._get_table_num()

            # section: workload
            self._workload_type = cnf.get('workload', 'workload_type')
            self._read_only = True if self._workload_type == 'RO' else False
            self._oltp_point_selects = cnf.get('workload', 'oltp_point_selects')
            self._oltp_simple_ranges = cnf.get('workload', 'oltp_simple_ranges')
            self._oltp_sum_ranges = cnf.get('workload', 'oltp_sum_ranges')
            self._oltp_order_ranges = cnf.get('workload', 'oltp_order_ranges')
            self._oltp_distinct_ranges = cnf.get('workload', 'oltp_distinct_ranges')
            self._oltp_index_updates = cnf.get('workload', 'oltp_index_updates')
            self._oltp_non_index_updates = cnf.get('workload', 'oltp_non_index_updates')

            # section: database
            self._db_parms = ' '.join(['{}={}'.format(k, v) for k, v in cnf.items('database')])

            self._log_dir = ''
            self._log_files = []
            log.debug('Sweep config file: {} loaded.'.format(config_file))
        except (NoSectionError, NoOptionError, KeyError):
            log.error('Invalid config file or unsupported option:{}'.format(config_file))
            raise

    def _get_table_size(self):
        matrix = {'84': 1000000,
                  '75': 1000000}
        try:
            table_size = matrix[self._db_size]
            return table_size
        except KeyError:
            raise

    def _get_table_num(self):
        matrix = {'84': 350,
                  '75': 310}
        try:
            table_num = matrix[self._db_size]
            return table_num
        except KeyError:
            raise

    @property
    def sysbench_threads(self):
        return self._sysbench_threads

    def _run_remote(self, target, cmd):
        assert target and cmd

        log.debug('[{} command] {}'.format(target, cmd))
        conn = paramiko.SSHClient()
        conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())

        if target == 'server':
            conn.connect(self._db_ip, 22, 'root', '/root/.ssh/id_rsa')
        elif target == 'client':
            conn.connect(self._client_ip, 22, 'root', '/root/.ssh/id_rsa')
        else:
            raise RuntimeError('Unsupported target type: {}'.format(target))

        stdin, stdout, stderr = conn.exec_command(cmd)
        err, out = stderr.readlines(), stdout.readlines()
        conn.close()
        log.debug('(ret={}) {}'.format(str(err), ''.join(out)))

        return err, out

    def db_cmd(self, cmd):
        return self._run_remote('server', cmd)

    def client_cmd(self, cmds):
        """
        Accept a bunch of commands and run them concurrently in background.
        :param cmds:
        :return:
        """
        # return self._ssh_run('client', cmd)
        assert cmds
        if not isinstance(cmds, list):
            if isinstance(cmds, str):
                cmds = [cmds]
            else:
                raise RuntimeError("Invalid client cmd: {}".format(cmds))

        log.debug('[client cmd]:\n{}'.format('\n'.join(cmds)))

        running_procs = [Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE) for cmd in cmds]

        while running_procs:
            for proc in running_procs:
                ret = proc.poll()
                if ret is not None:  # Process finished.
                    results, errors = proc.communicate()

                    if ret != 0:
                        log.error('client cmd failed: (ret= {}, results={})'.format(ret, results.decode('utf-8')))
                        raise RuntimeError(errors)
                    else:
                        log.error('client cmd finished: (ret= {}, results={})'.format(ret, results.decode('utf-8')))

                    running_procs.remove(proc)
                    break
                else:  # No process is done, wait a bit and check again.
                    time.sleep(10)
                    continue

    def clean_db(self):
        log.debug('Running database cleanup program.')
        cleanup_script = os.path.join(self._cleandb_path, 'cleandb.py')
        cmd_template = '{cleanup_script} {db_size} {db_num} -vv ' \
                       '-p "{parameters}" 2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_size=self._db_size,
                                           db_num=self._db_inst_num,
                                           parameters=self._db_parms,
                                           log_path=self._cleandb_path)
        self.db_cmd(clean_db_cmd)

    @staticmethod
    def kill_proc(proc_names, skip_pid=0):
        """
        Kill a process by name.
        :param proc_names:
        :return:
        """
        assert proc_names is not None

        if not isinstance(proc_names, list):
            if isinstance(proc_names, str):
                proc_names = [proc_names]
            else:
                raise RuntimeError("Invalid process name: {}".format(str(proc_names)))

        for proc in psutil.process_iter():
            if proc.name() in proc_names and proc.pid != skip_pid:
                log.debug('Kill process: {} pid={}'.format(proc.name(), proc.pid))
                proc.kill()

    def clean_client(self):
        log.debug('Running client housekeeping scripts.')
        proc_names = ["iostat",
                     "mpstat",
                     "vmstat",
                     "tdctl",
                     "sysbench",
                     "run-readonly",
                     "run-sysbench",
                     "mysqladmin"]

        self.kill_proc(proc_names)

        time.sleep(5)
        log.debug('Client is ready.')

    def run_one_test(self, thread_num):
        assert thread_num
        cmd_template = 'sysbench ' \
                       '--test=/usr/local/src/sysbench/sysbench/tests/db/oltp.lua ' \
                       '--oltp-table-size={oltp_table_size} ' \
                       '--oltp-tables-count={oltp_tables_count} ' \
                       '--mysql-host={mysql_host} ' \
                       '--mysql-port={mysql_port} ' \
                       '--mysql-db=sbtest ' \
                       '--mysql-user=sbtest ' \
                       '--mysql-password=sbtest ' \
                       '--num-threads={thread_num} ' \
                       '--max-requests=0  ' \
                       '--max-time={max_time} ' \
                       '--report-interval=1 ' \
                       '--oltp-read-only={oltp_read_only} ' \
                       '--oltp-point-selects={oltp_point_selects}  ' \
                       '--oltp-simple-ranges={oltp_simple_ranges} ' \
                       '--oltp-sum-ranges={oltp_sum_ranges} ' \
                       '--oltp-order-ranges={oltp_order_ranges} ' \
                       '--oltp-distinct-ranges={oltp_distinct_ranges} ' \
                       '--oltp-index-updates={oltp_index_updates} ' \
                       '--oltp_non_index_updates={oltp_non_index_updates} ' \
                       'run > {file_name}'

        cmds = [cmd_template.format(oltp_table_size=self._table_size,
                                    oltp_tables_count=self._table_num,
                                    mysql_host=self._db_ip,
                                    mysql_port=port,
                                    thread_num=thread_num,
                                    max_time=self._benchmark_duration,
                                    oltp_read_only='on' if self._read_only else 'off',
                                    oltp_point_selects=self._oltp_point_selects,
                                    oltp_simple_ranges=self._oltp_simple_ranges,
                                    oltp_sum_ranges=self._oltp_sum_ranges,
                                    oltp_order_ranges=self._oltp_order_ranges,
                                    oltp_distinct_ranges=self._oltp_distinct_ranges,
                                    oltp_index_updates=self._oltp_index_updates,
                                    oltp_non_index_updates=self._oltp_non_index_updates,
                                    file_name='{}/{}_{}_{}_db{}.log'.format(self._sweep_name,
                                                                            self._sweep_name,
                                                                            self._benchmark_target,
                                                                            thread_num,
                                                                            port-int(self._db_port)+1))
                for port in range(int(self._db_port),
                                  int(self._db_port) + int(self._db_inst_num))]

        self.client_cmd(cmds)

        for port in range(int(self._db_port), int(self._db_port) + int(self._db_inst_num)):
            log_file_name = '{}/{}_{}_{}_db{}.log'.format(self._sweep_name,
                                                          self._sweep_name,
                                                          self._benchmark_target,
                                                          thread_num,
                                                          port-int(self._db_port)+1)
            self._log_files.append(log_file_name)

    def plot(self):
        plot_files = ' '.join(self._log_files)
        plot_cmd = 'sysbench_plot.py {}'.format(plot_files)

        self.client_cmd(plot_cmd)

    def start(self):
        try:
            os.mkdir(self._sweep_name)
            self._log_dir = self._sweep_name
        except FileExistsError:
            pass

        log.debug('Sweep <{}> started.'.format(self._sweep_name))
        for threads in self._sysbench_threads:
            log.debug('Benchmark for {} threads started.'.format(str(threads)))
            sweep.clean_client()
            sweep.clean_db()
            sweep.run_one_test(threads)

        self.plot()


if __name__ == "__main__":
    """The main function to run the sweep.
    """
    parser = argparse.ArgumentParser(description="This program run the benchmarks defined by a config file.")
    parser.add_argument("config", help="config file name/path")
    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)",
                        action='count', default=0)

    args = parser.parse_args()

    if args.v == 0:
        log_level = logging.ERROR
    elif args.v == 1:
        log_level = logging.INFO
    else:
        log_level = logging.DEBUG

    logging.basicConfig(level=log_level)
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    sweep = Sweep('sweep.cnf')

    # TODO: parse config file to get parms:
    sweep.start()
