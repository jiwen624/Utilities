#!/usr/bin/env python3
"""This sweep program is used to run MySQL benchmarks with various configurations in client/server.
    Run this command on the client server.
    External dependencies: sysbench_plot.py, cleandb.py
    
    History:
    1. Removed 'sweep_name' from config file. -- @EricYang v0.6 Aug 11, 2016
    2. Changed threading.Timer handler        -- @EricYang v0.61 Aug 25, 2016
"""
import os
import re
import time
import sys
import logging
import psutil
import threading
import select
import argparse
import paramiko
import zipfile
import shutil
from subprocess import Popen, check_output, PIPE
from configparser import ConfigParser, NoSectionError, NoOptionError

log = logging.getLogger('')


class Sweep:
    """
    This class launch a sysbench benchmark sweep with various threads.
    Usage:
        sweep = Sweep('test.cnf')
        sweep.start()
    """

    def __init__(self, config_file):
        assert config_file
        cnf = ConfigParser()

        try:
            self._cnf_file = config_file
            cnf.read(config_file)

            # section: server
            self._db_ip = cnf.get('server', 'db_ip')
            self._db_port = cnf.get('server', 'db_port')
            self._client_ip = cnf.get('server', 'client_ip')
            self._dbscript_path = cnf.get('server', 'dbscript_path')

            # section: benchmark
            # self._sweep_name = cnf.get('benchmark', 'sweep_name')
            self._sweep_name, _ = os.path.splitext(self._cnf_file)
            self._log_dir = self._sweep_name
            self._threads = cnf.get('benchmark', 'sysbench_threads').split(sep=',')
            self._db_num = cnf.get('benchmark', 'db_num')
            self._target = cnf.get('benchmark', 'target')
            self._duration = cnf.getint('benchmark', 'duration')
            self._db_size = cnf.get('benchmark', 'db_size').rstrip('G')
            self._lua_script = cnf.get('benchmark', 'lua_script')
            self._tblsize = self._get_table_size()
            self._tblnum = self._get_table_num()

            # section: workload
            self._workload = cnf.get('workload', 'workload_type')
            self._read_only = True if self._workload == 'RO' else False
            self._point_selects = cnf.get('workload', 'oltp_point_selects')
            self._simple_ranges = cnf.get('workload', 'oltp_simple_ranges')
            self._sum_ranges = cnf.get('workload', 'oltp_sum_ranges')
            self._order_ranges = cnf.get('workload', 'oltp_order_ranges')
            self._distinct_ranges = cnf.get('workload', 'oltp_distinct_ranges')
            self._index_updates = cnf.get('workload', 'oltp_index_updates')
            self._non_index_updates = cnf.get('workload', 'oltp_non_index_updates')

            # section: database
            self._db_parms = ' '.join(['{}={}'.format(k, v) for k, v in cnf.items('database')])

            track_active = cnf.get('database', 'track_active')
            if self._target == 'RAM' and track_active != '0':
                log.debug('Benchmark target = RAM but track_active = {}'.format(track_active))
                self._db_parms = re.sub(r'(track_active=\d{1,2}\s)', 'track_active=0 ', self._db_parms)
                log.debug('New database parms: {}'.format(self._db_parms))

            # section: misc
            self._plot = True if cnf.get('misc', 'plot') == 'true' else False
            self._send_mail = True if cnf.get('misc', 'send_mail') == 'true' else False
            if self._send_mail:
                self._mail_sender = cnf.get('misc', 'mail_sender')
                self._mail_recipients = cnf.get('misc', 'mail_recipients')
                self._smtp_server = cnf.get('misc', 'smtp_server')
                self._smtp_port = cnf.get('misc', 'smtp_port')

                if not ('@' in self._mail_recipients and '@' in self._mail_sender):
                    raise ValueError('Invalid email address in *mail_recipients*')

            self._sweep_logs = []
            self._user = cnf.get('misc', 'user')
            self._ssd_device = cnf.get('misc', 'ssd_device')
            self._skip_db_recreation = True if cnf.get('misc', 'skip_db_recreation') == 'true' else False
            log.debug('Sweep config file: {} loaded.'.format(config_file))
            self._running_procs = []
            self._original_sigint = None

        except (NoSectionError, NoOptionError, KeyError, ValueError):
            log.error('Invalid config file or unsupported option:{}'.format(config_file))
            raise

    @property
    def original_sigint(self):
        return self._original_sigint

    @original_sigint.setter
    def original_sigint(self, handler):
        self._original_sigint = handler

    @property
    def running_procs(self):
        return self._running_procs

    def _get_table_size(self):
        """
        The matrix to map database size to table size (how many rows per table)
        :return:
        """
        matrix = {'84': 1000000,
                  '75': 1000000}
        try:
            table_size = matrix[self._db_size]
            return table_size
        except KeyError:
            raise

    def _get_table_num(self):
        """
        The matrix to map database size to table count (how many tables)
        :return:
        """
        matrix = {'84': 350,
                  '75': 310}
        try:
            table_num = matrix[self._db_size]
            return table_num
        except KeyError:
            raise

    @property
    def sysbench_threads(self):
        return self._threads

    def _run_remote2(self, cmd):
        """
        The new interface to run a command remotely,
        with the enhancement that show remote output in real time
        :param cmd:
        :return:
        """
        assert cmd
        log.info('[db] {}'.format(cmd))

        trans = paramiko.Transport(self._db_ip, 22)
        key_path = os.path.expanduser('~/.ssh/id_rsa')
        key = paramiko.RSAKey.from_private_key_file(key_path)
        trans.connect(username=self._user, pkey=key)
        session = trans.open_channel("session")
        session.exec_command(cmd)

        result = ''
        while not session.exit_status_ready():
            if session.recv_ready():
                buff = session.recv(4096).decode('utf-8').strip()
                log.info('[db] {}'.format(buff))
                result += buff
        return result

    def _run_remote(self, cmd):
        """
        Run a remote command in synchronise mode
        :param cmd: a command
        :return:
        """
        assert cmd

        log.info('[db] {}'.format(cmd))
        conn = paramiko.SSHClient()
        conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn.connect(self._db_ip, 22, self._user, key_filename='~/.ssh/id_rsa')

        stdin, stdout, stderr = conn.exec_command(cmd)

        err, out = stderr.readlines(), stdout.readlines()
        conn.close()
        log.info('(ret={}) {}'.format(str(err), ''.join(out)))

        return err, out

    def db_cmd(self, cmd):
        """
        Run a command remotely on the database server
        :param cmd:
        :return:
        """
        # return self._run_remote(cmd)
        return self._run_remote2(cmd)

    def _run_local(self, commands, timeout):
        """
        Accept a bunch of commands and run them concurrently in background.
        :param commands:
        :return:
        """
        assert commands
        if not isinstance(commands, list):
            if isinstance(commands, str):
                commands = [commands]
            else:
                raise RuntimeError("[client] Invalid cmd: {}".format(commands))

        self._running_procs = [Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE,
                                     universal_newlines=True, close_fds=True) for cmd in commands]
        watcher = threading.Timer(timeout, self.kill_running_procs)
        watcher.start()
        p = select.epoll()
        pipe_dict = {}

        for proc in self._running_procs:
            p.register(proc.stdout, select.POLLIN | select.POLLERR | select.POLLHUP)
            stdout_fileno = proc.stdout.fileno()
            pipe_dict[stdout_fileno] = proc.stdout
            log.info('[client] (id:{}) cmd=({})'.format(stdout_fileno, proc.args))

        while self._running_procs:
            result = p.poll(timeout=5000)
            if len(result):
                for m in result:
                    if m[1] & select.POLLIN:
                        log.debug('[client] (id:{}) {}'.format(m[0], pipe_dict[m[0]].readline().strip()))

            for proc in self._running_procs:
                ret = proc.poll()
                if ret is not None:  # Process finished.
                    _, errors_raw = proc.communicate()
                    errors = errors_raw.rstrip()

                    if ret != 0:  # Process failed.
                        log.error('[client] command failed: {}'.format(proc.args))
                        log.error('[client] (errs={})'.format(errors))
                        # watcher.cancel()

                        if 'sysbench' in proc.args:  # sysbench failure is a critical error.
                            watcher.cancel()
                            raise RuntimeError
                        else:  # Just ignore failures from the other commands
                            self._running_procs.remove(proc)
                    else:
                        log.info('[client] finished: (cmd={})'.format(proc.args))
                        self._running_procs.remove(proc)
                    break
                else:  # No process is done, just do nothing and check the next command in the running_procs list.
                    continue
        # We need to cancel the watcher here as we don't need it any more.
        watcher.cancel()
        watcher.join(timeout=10)

    def kill_running_procs(self):
        log.info('[client] Timeout! Commands still running will be killed.')
        procs = self._running_procs
        self._running_procs = []

        for proc in procs:
            log.debug('[client] Preparing to kill: {} but proc.kill may get stuck randomly.'.format(proc.args))
            proc.kill()
            log.debug('[client] Process killed: {}.'.format(proc.args))

    def client_cmd(self, cmds, timeout):
        """
        Run a bunch of commands on the client server.
        :param cmds:
        :param timeout:
        :return:
        """
        self._run_local(cmds, timeout)

    def clean_db(self):
        """
        Clean up the database environment
        :return:
        """
        log.info('Running database cleanup program.')
        cleanup_script = os.path.join(self._dbscript_path,
                                      'cleandb.py')

        skip_db_recreation = '-o skip_db_recreation' if self._skip_db_recreation else ''
        cmd_template = '{cleanup_script} {db_size} {db_num} {skip_db_recreation} -v -p "{parameters}" 2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_size=self._db_size,
                                           db_num=self._db_num,
                                           skip_db_recreation=skip_db_recreation,
                                           parameters=self._db_parms,
                                           log_path=self._dbscript_path)
        self.db_cmd(clean_db_cmd)

    @staticmethod
    def kill_proc(proc_names, skip_pid=0):
        """
        Kill a process by name.
        :param proc_names:
        :param skip_pid:
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
        """
        Clean up the client server.
        :return:
        """
        log.info('Running client housekeeping scripts.')
        proc_names = ["iostat",
                      "mpstat",
                      "vmstat",
                      "tdctl",
                      "sysbench",
                      "run-readonly",
                      "run-sysbench",
                      "mysqladmin"]

        self.kill_proc(proc_names)

        # Kill previous sweep which may still be running
        _, exec_file = os.path.split(__file__)
        self_pid = os.getpid()
        self.kill_proc(exec_file, self_pid)

        time.sleep(5)
        log.info('Client is ready.')

    def run_one_test(self, thread_num):
        """
        Run a benchmark with sysbench thread number=thread_num
        - and may plot and compress them.
        - and send an email.
        :param thread_num:
        :return:
        """
        assert thread_num is not None

        log.info('Running test of {} sysbench threads'.format(thread_num))
        current_log_files = []
        all_cmds = []

        cmd_template = 'sysbench ' \
                       '--test={lua_script} ' \
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
                       '--rand-init=on --rand-type=uniform ' \
                       'run > {file_name}'

        for port in range(int(self._db_port), int(self._db_port) + int(self._db_num)):
            sb_logfile_tail = 'sb_{}_{}_db{}.log'.format(self._target,
                                                         thread_num,
                                                         port - int(self._db_port) + 1)
            sb_log_file = os.path.join(self._log_dir, sb_logfile_tail)

            sb_cmd = cmd_template.format(lua_script=self._lua_script,
                                         oltp_table_size=self._tblsize,
                                         oltp_tables_count=self._tblnum,
                                         mysql_host=self._db_ip,
                                         mysql_port=port,
                                         thread_num=thread_num,
                                         max_time=self._duration,
                                         oltp_read_only='on' if self._read_only else 'off',
                                         oltp_point_selects=self._point_selects,
                                         oltp_simple_ranges=self._simple_ranges,
                                         oltp_sum_ranges=self._sum_ranges,
                                         oltp_order_ranges=self._order_ranges,
                                         oltp_distinct_ranges=self._distinct_ranges,
                                         oltp_index_updates=self._index_updates,
                                         oltp_non_index_updates=self._non_index_updates,
                                         file_name=sb_log_file)
            all_cmds.append(sb_cmd)
            current_log_files.append(sb_log_file)

        os_cmds = ('iostat -dmx {} -y'.format(self._ssd_device),
                   'mpstat',
                   'vmstat -S M -w',
                   'tdctl -v --dp +')
        for cmd in os_cmds:
            sys_log_file = os.path.join(self._log_dir, '{}_{}_{}.log'.format(cmd.split()[0], self._target, thread_num))
            count = '' if 'tdctl' in cmd else int(self._duration / 10)
            sys_mon_cmd = 'ssh root@{server_ip} {cmd} 10 {count} > {log_name}'.format(server_ip=self._db_ip,
                                                                                      cmd=cmd,
                                                                                      count=count,
                                                                                      log_name=sys_log_file)
            all_cmds.append(sys_mon_cmd)
            current_log_files.append(sys_log_file)

        self.client_cmd(all_cmds, self._duration + 60)
        self._sweep_logs.extend(current_log_files)
        return current_log_files

    def plot(self):
        """
        Plot the logs: sysbench, system monitor logs (iostat, mpstat, vmstat, etc)
        :return:
        """
        log.info('Plotting the sweep')
        plot_files = ' '.join(self._sweep_logs)
        plot_cmd = './sysbench_plot.py -p {} {}'.format(self._sweep_name, plot_files)

        # The timeout of plot is 600 seconds, it will be killed if not return before timeout
        self.client_cmd(plot_cmd, 600)

    def _compress(self):
        """
        Compress the raw logs and graphs.
        :return:
        """
        zip_file = '{}.zip'.format(self._log_dir)
        with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zipped:
            for fname in os.listdir(self._log_dir):
                absname = os.path.join(self._log_dir, fname)
                zipped.write(absname)
        return zip_file

    def send_mail(self):
        """
        Send the compressed file to a recipient.
        :return:
        """
        log.info('Send an email to the recipients.')
        attachment = self._compress()
        sendmail_template = "./mailto.py " \
                            "{sender} " \
                            "{recipients} " \
                            "-S {smtp_server} " \
                            "-P {smtp_port} " \
                            "-s \"{subject}\" " \
                            "-a {attachment} " \
                            "-B \"{msg_body}\""
        subject_str = "Logs and graphs for sweep {}".format(self._sweep_name)
        msg_body = 'Please see attached.'
        sendmail_cmd = [sendmail_template.format(sender=self._mail_sender,
                                                 recipients=self._mail_recipients,
                                                 smtp_server=self._smtp_server,
                                                 smtp_port=self._smtp_port,
                                                 subject=subject_str,
                                                 attachment=attachment,
                                                 msg_body=msg_body)]
        self.client_cmd(sendmail_cmd, timeout=600)

    @staticmethod
    def result_is_good(current_log_files):
        """
        Check if the benchmark has been done successfully and raise an RuntimeError if some error happens.
        It's considered good if the sb_*.log (sysbench logs) contains 'execution time' in the tail.
        :param current_log_files:
        :return:
        """
        log.info('Checking if the sweep is in good state.')
        assert current_log_files
        for file in current_log_files:
            _, tail = os.path.split(file)
            if tail.startswith('sb'):
                check_cmd = "tail -2 {} | awk '{{print $1, $2}}'".format(file)
                started = check_output(check_cmd, shell=True, universal_newlines=True).replace("\n", " ")
                if 'execution time' not in started:
                    return False
        return True

    def start(self):
        """
        Start the sweep. The entry point of the benchmark(s).
        :return:
        """
        try:
            os.mkdir(self._sweep_name)
        except FileExistsError:
            pass

        log.info('Sweep <{}> started, check logs under the directory with that name.'.format(self._sweep_name))
        for threads in self._threads:
            log.info('Benchmark for {} threads has started.'.format(threads))
            sweep.clean_client()
            sweep.clean_db()
            if not self.result_is_good(sweep.run_one_test(threads)):
                log.error('Benchmark for {} threads has failed. Exiting...'.format(threads))
                raise RuntimeError('At least one of the benchmark is finished but some error happened.')

            log.info('Benchmark for {} threads has finished.'.format(threads))

        if self._plot:
            self.plot()

        # Copy the sweep config file to the sweep directory.
        try:
            shutil.copy2(self._cnf_file, os.path.join(self._log_dir, self._cnf_file))
            shutil.copy2('/etc/my.cnf', os.path.join(self._log_dir, 'my.cnf'))
            if self._target == 'DMX':
                shutil.copy2('/dmx/etc/bfapp.d/mysqld', os.path.join(self._log_dir, 'bfapp.d.mysqld'))
                shutil.copy2('/dmx/etc/bfcs.d/mysqld', os.path.join(self._log_dir, 'bfcs.d.mysqld'))
            else:
                pass
        except FileNotFoundError as e:
            log.warning('Failed to copy dmx/mysql config files: {}'.format(e))

        # Get the barf command print and write to a log file
        barf_file = os.path.join(self._log_dir, 'barf.out')
        barf_cmd = 'ssh {user}@{db_ip} barf -v -l >{barf_file};sync'.format(user=self._user,
                                                                            db_ip=self._db_ip,
                                                                            barf_file=barf_file)
        self.client_cmd(barf_cmd, timeout=120)

        if self._send_mail:
            self.send_mail()

        # Change the .cnf file to .done
        try:
            pure_filename, _ = os.path.splitext(self._cnf_file)
            os.rename(self._cnf_file, pure_filename+'.done')
        except OSError as e:
            log.warning('Failed to rename the config file: {}'.format(e))


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

    logging.basicConfig(level=log_level, stream=sys.stdout, format='%(asctime)s %(levelname)s: %(message)s')
    # I don't want to see paramiko debug logs, unless they are WARNING or worse.
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    log.debug('\n\n****New sweep config file found, preparing to start.****')
    sweep = Sweep(args.config)
    try:
        sweep.start()
        log.info('The sweep has finished. Bye.')
    except KeyboardInterrupt:
        log.warning('Received SIGINT. I will kill the running processes')
        for process in sweep.running_procs:
            process.kill()
    sys.exit(0)
