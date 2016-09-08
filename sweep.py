#!/usr/bin/env python3
"""This sweep program is used to run MySQL benchmarks with various configurations in client/server.
    Run this command on the client server.
    External dependencies: sysbench_plot.py, cleandb.py
    
    History:
    0. Initial version created.               -- @EricYang v0.1 March xx, 2016
    ...

    1. Removed 'sweep_name' from config file. -- @EricYang v0.6 Aug 11, 2016
    2. Changed threading.Timer handler        -- @EricYang v0.61 Aug 20, 2016
    3. Changes:
           Added 'tarball_path' as a new config option
           Log directory of a failed benchmark will be renamed to 'failed_blabla'
           Added logs for commands: lscpu, free, etc
                                              --@EricYang v0.62 Aug 24, 2016
    4. Added 'mysql_base_dir' to the .cnf file --@EricYang v0.63 Aug 27, 2016
    5. Added logs of show engine innodb status --@EricYang v0.64 Sep 01, 2016
    6. Fixed a few bugs.
       Added 'tar_strips_components' to the .cnf file --@EricYang v0.65 Sep 03, 2016
"""
import os
import re
import time
import sys
import logging
import psutil
import signal
import threading
import select
import argparse
import paramiko
import zipfile
import shutil
from subprocess import Popen, check_output, PIPE
from configparser import ConfigParser, NoSectionError, NoOptionError

log = logging.getLogger('')
SweepFatalError = RuntimeError


class Sweep:
    """
    This class launches a sysbench benchmark sweep with a certain number of threads.
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
            self._login_user = cnf.get('server', 'dbserver_user')

            # section: benchmark
            tmp_log_dir, _ = os.path.splitext(self._cnf_file)
            self._log_dir = '{}_{}'.format(tmp_log_dir, time.strftime('%Y%m%d%H%M%S'))
            self._threads = cnf.get('benchmark', 'sysbench_threads').split(sep=',')
            self._db_num = cnf.get('benchmark', 'db_num')
            self._target = cnf.get('benchmark', 'target')
            self._duration = cnf.getint('benchmark', 'duration')
            # self._db_size = cnf.get('benchmark', 'db_size').rstrip('G')
            self._lua_script = cnf.get('benchmark', 'lua_script')
            self._tarball_path = cnf.get('benchmark', 'tarball_path')
            self._tar_strips = cnf.get('benchmark', 'tar_strip_components')
            # self._tblsize = self._get_table_size()
            # self._tblnum = self._get_table_num()
            self._db_name = cnf.get('benchmark', 'db_name')
            self._tblsize = cnf.get('benchmark', 'table_rows')
            self._tblnum = cnf.get('benchmark', 'table_num')
            self._base_dir = cnf.get('benchmark', 'mysql_base_dir')
            self._socket_prefix = cnf.get('benchmark', 'mysql_socket_file_prefix')

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
            self._ssd_device = cnf.get('misc', 'ssd_device')
            self._skip_db_recreation = True if cnf.get('misc', 'skip_db_recreation') == 'true' else False
            log.debug('Sweep config file: {} loaded.'.format(config_file))
            self._running_procs = []
            self._original_sigint = None
            # paramiko Transport object
            self._trans = None
            self._sweep_successful = True

        except (NoSectionError, NoOptionError, KeyError, ValueError):
            log.error('Invalid config file or unsupported option:{}'.format(config_file))
            self._sweep_successful = False
            return

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()

    def close(self):
        """
        close:
            - kill all processes which are still running
            - close transport to database server
        :return:
        """
        self.kill_running_procs()
        self.close_db_conn()

    @property
    def original_sigint(self):
        return self._original_sigint

    @property
    def successful(self):
        return self._sweep_successful

    @original_sigint.setter
    def original_sigint(self, handler):
        self._original_sigint = handler

    @property
    def running_procs(self):
        return self._running_procs

    # def _get_table_size(self):
    #     """
    #     The matrix to map database size to table size (how many rows per table)
    #     ** This function is no longer used **
    #     :return:
    #     """
    #     matrix = {'84': 1000000,
    #               '75': 1000000}
    #     try:
    #         table_size = matrix[self._db_size]
    #         return table_size
    #     except KeyError:
    #         raise
    #
    # def _get_table_num(self):
    #     """
    #     The matrix to map database size to table count (how many tables)
    #     ** This function is no longer used **
    #     :return:
    #     """
    #     matrix = {'84': 350,
    #               '75': 310}
    #     try:
    #         table_num = matrix[self._db_size]
    #         return table_num
    #     except KeyError:
    #         raise

    @property
    def sysbench_threads(self):
        return self._threads

    def _run_remote2(self, cmd):
        """
        The new interface to run a command remotely, with the enhancement that
        shows remote output in real time.
        This function needs to be enhanced to run multiple commands simultaneously.
        :param cmd:
        :return:
        """
        assert cmd
        log.info('[db] {}'.format(cmd))
        # Reuse the Transport object if there is already there.
        if self._trans is None:
            self._trans = paramiko.Transport(self._db_ip, 22)

        # Reconnect to remote server if the connection is inactive.
        if not self._trans.is_active():
            key_path = os.path.expanduser('~/.ssh/id_rsa')
            key = paramiko.RSAKey.from_private_key_file(key_path)
            self._trans.connect(username=self._login_user, pkey=key)

        # Each command needs a separate session
        session = self._trans.open_channel("session")
        # session.get_pty() -- Do I need this?
        session.exec_command(cmd)

        result = ''
        # Check remote command status and get the real-time output.
        # while not session.exit_status_ready():
        #     if session.recv_ready():
        #         buff = session.recv(4096).decode('utf-8').rstrip()
        #         log.info('[db] {}'.format(buff))
        #         result += buff

        while True:
            if session.recv_ready():
                buff = session.recv(4096).decode('utf-8').strip().replace('\r', '')
                log.info('[db] {}'.format(buff))
                result += buff
            # We can break out if there is no buffered data and the process has exited.
            elif session.exit_status_ready():
                break
            time.sleep(0.01)

        exit_status = session.recv_exit_status()
        result += '\n'  # The '\n' was striped.
        session.close()  # Should I close it explicitly here?
        return exit_status, result

    def close_db_conn(self):
        """
        Close the Transport object to the database server
        :return:
        """
        if self._trans:
            self._trans.close()
            self._trans = None

    # def _run_remote(self, cmd):
    #     """
    #     Run a remote command in synchronise mode
    #     :param cmd: a command
    #     :return:
    #     """
    #     assert cmd
    #
    #     log.info('[db] {}'.format(cmd))
    #     conn = paramiko.SSHClient()
    #     conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    #     conn.connect(self._db_ip, 22, self._user, key_filename='~/.ssh/id_rsa')
    #
    #     stdin, stdout, stderr = conn.exec_command(cmd)
    #
    #     err, out = stderr.readlines(), stdout.readlines()
    #     conn.close()
    #     log.info('(ret={}) {}'.format(str(err), ''.join(out)))
    #
    #     return err, out

    def db_cmd(self, cmd, out=None):
        """
        Run a command remotely on the database server
        :param out: output file
        :param cmd:
        :return: exit_status
        """
        # return self._run_remote(cmd)
        exit_status, buff = self._run_remote2(cmd)
        if out:
            try:
                with open(out, 'a') as out_file:
                    out_file.write(buff)
            except IOError:
                log.warning('Cannot open {} for db command output'.format(out))

        return exit_status

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
                log.error("[client] Invalid cmd: {}".format(commands))

        # shell=True is not the best practice but let's keep it for now.
        self._running_procs = [Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE,
                                     universal_newlines=True, close_fds=True,
                                     preexec_fn=os.setsid) for cmd in commands]
        watcher = threading.Timer(timeout, self.sweep_timeout_handler)
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

                        # Check if sysbench is failed and do fast-fail if so:
                        if 'sysbench' in proc.args:  # sysbench failure is a critical error.
                            watcher.cancel()
                            self.close()
                            self._sweep_successful = False
                            log.error('[client] Fatal error found in sysbench, exiting...')
                            # raise RuntimeError
                        else:  # Just ignore failures from the other commands
                            self._running_procs.remove(proc)
                    else:
                        log.info('[client] Done: (cmd={})'.format(proc.args))
                        self._running_procs.remove(proc)
                    break
                else:
                    # No process is done.
                    # just do nothing and check the next command in the running_procs list.
                    continue
        # We need to cancel the watcher here as we don't need it any more.
        watcher.cancel()
        watcher.join(timeout=10)

    def sweep_timeout_handler(self):
        """
        The timeout handler for the sweep. It will be invoked when the designated time has passed.
        :return:
        """
        log.info('Shutdown the sweep as it has reached its time limit: {} seconds'.format(self._duration))
        self.kill_running_procs()

    def kill_running_procs(self):
        log.info('[client] Cleaning up the sweep. ALL commands still running will be killed:')
        procs = self._running_procs
        self._running_procs = []

        for proc in procs:
            log.debug('[client] Killing: ({}) {}.'.format(proc.pid, proc.args))
            # proc.kill()  # This would not work for 'shell=True'
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                log.debug('[client] Killed: ({}) {}.'.format(proc.pid, proc.args))
            except ProcessLookupError as e:
                log.warning('Failed to kill process ({}): {}'.format(proc.pid, e))

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
        cmd_template = '{cleanup_script} {db_num} {skip_db_recreation} ' \
                       '-d {base_dir} -z {tarball} -s {strips} -v -p "{parameters}" 2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_num=self._db_num,
                                           skip_db_recreation=skip_db_recreation,
                                           base_dir=self._base_dir,
                                           tarball=self._tarball_path,
                                           strips=self._tar_strips,
                                           parameters=self._db_parms,
                                           log_path=self._dbscript_path)
        exit_status = self.db_cmd(clean_db_cmd)
        if exit_status == 1:  # The cleanup.py from the database server is failed.
            log.error('Database cleanup failed, err_code: {}'.format(exit_status))
            self._sweep_successful = False
            raise SweepFatalError

    @staticmethod
    def kill_proc_by_name(proc_names, skip_pid=0):
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
                raise SweepFatalError("Invalid process name: {}".format(str(proc_names)))

        for proc in psutil.process_iter():
            for name in proc_names:
                try:
                    if name in ' '.join(proc.cmdline()) and proc.pid != skip_pid:
                        log.debug('Killing process:({}) {}'.format(proc.pid, ' '.join(proc.cmdline())))
                        proc.kill()
                        continue
                except psutil.NoSuchProcess:
                    pass

    def clean_client(self):
        """
        Clean up the client server.
        :return:
        """
        log.info('Running client housekeeping scripts.')
        # Should these processes be cleaned up on the server side?
        proc_names = ["iostat",
                      "mpstat",
                      "vmstat",
                      "tdctl",
                      "sysbench",
                      "mysql"]

        self.kill_proc_by_name(proc_names)

        # Kill previous sweep which may still be running
        _, exec_file = os.path.split(__file__)
        self_pid = os.getpid()
        self.kill_proc_by_name(exec_file, self_pid)

        time.sleep(5)
        log.info('[client] ***Client is ready.***')

    def run_one_test(self, thread_cnt):
        """
        Run a benchmark with sysbench thread number=thread_num
        - and may plot and compress them.
        - and send an email.
        :param thread_cnt:
        :return:
        """
        assert thread_cnt is not None

        log.info('Running test of {} sysbench threads'.format(thread_cnt))

        # 0. list to store all commands and logs----------------------------
        curr_logs = []  # Record the file names of all current logs.
        all_cmds = []  # All the commands need to be executed

        # 1. sysbench commands ---------------------------------------------
        cmd_template = 'sysbench ' \
                       '--test={lua_script} ' \
                       '--oltp-table-size={oltp_table_size} ' \
                       '--oltp-tables-count={oltp_tables_count} ' \
                       '--mysql-host={mysql_host} ' \
                       '--mysql-port={mysql_port} ' \
                       '--mysql-db={db_name} ' \
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
                       'run > {log_name}'

        for port in range(int(self._db_port), int(self._db_port) + int(self._db_num)):
            db_idx = port - int(self._db_port) + 1
            # For each instance, record sysbench logs and innodb status logs, etc.
            # 1. the sysbench logs:
            sb_log_name = 'sb_{}_{}_db{}.log'.format(self._target,
                                                     thread_cnt,
                                                     db_idx)
            sb_log_path = os.path.join(self._log_dir, sb_log_name)

            sb_cmd = cmd_template.format(lua_script=self._lua_script,
                                         oltp_table_size=self._tblsize,
                                         oltp_tables_count=self._tblnum,
                                         mysql_host=self._db_ip,
                                         mysql_port=port,
                                         db_name=self._db_name,
                                         thread_num=thread_cnt,
                                         max_time=self._duration,
                                         oltp_read_only='on' if self._read_only else 'off',
                                         oltp_point_selects=self._point_selects,
                                         oltp_simple_ranges=self._simple_ranges,
                                         oltp_sum_ranges=self._sum_ranges,
                                         oltp_order_ranges=self._order_ranges,
                                         oltp_distinct_ranges=self._distinct_ranges,
                                         oltp_index_updates=self._index_updates,
                                         oltp_non_index_updates=self._non_index_updates,
                                         log_name=sb_log_path)
            all_cmds.append(sb_cmd)
            curr_logs.append(sb_log_path)

            # 2. The innodb status logs - every 60 seconds------------------------------
            innodb_log_name = 'innodb_status_db{}.log'.format(db_idx)
            innodb_log_path = os.path.join(self._log_dir, innodb_log_name)
            innodb_cmd_tmp = "while true; " \
                             "do " \
                             "  (mysql -S {socket_prefix}{db_idx} -e " \
                             "'     show engine innodb status\G' | " \
                             "  grep -A 28 -E 'LOG|END OF INNODB MONITOR OUTPUT'&); " \
                             "  sleep 60; " \
                             "done".format(socket_prefix=self._socket_prefix,
                                           db_idx=db_idx)
            innodb_cmd = 'ssh {user}@{server_ip} ' \
                         '"{cmd}" &> {log_name}'.format(user=self._login_user,
                                                        server_ip=self._db_ip,
                                                        cmd=innodb_cmd_tmp,
                                                        log_name=innodb_log_path)
            all_cmds.append(innodb_cmd)
            curr_logs.append(innodb_log_path)

        # 3. Commands for system monitoring---------------------------------------------
        os_cmds = ('iostat -dmx {} -y'.format(self._ssd_device),
                   'mpstat',
                   'vmstat -S M -w',
                   'tdctl -v --dp +')
        for cmd in os_cmds:
            sys_log_name = '{}_{}_{}.log'.format(cmd.split()[0], self._target, thread_cnt)
            sys_log_path = os.path.join(self._log_dir, sys_log_name)
            count = '' if 'tdctl' in cmd else int(self._duration / 10)
            full_sysmon_cmd = 'ssh {user}@{server_ip} ' \
                              '"{cmd} 10 {count}" > {log_name}'.format(user=self._login_user,
                                                                       server_ip=self._db_ip,
                                                                       cmd=cmd,
                                                                       count=count,
                                                                       log_name=sys_log_path)
            all_cmds.append(full_sysmon_cmd)
            curr_logs.append(sys_log_path)

        # 4. Commands for client monitoring---------------------------------------------
        client_cmds = ('vmstat -S M -w',)
        for cmd in client_cmds:
            client_log_name = '{}_{}_{}_client.log'.format(cmd.split()[0], self._target, thread_cnt)
            client_log_path = os.path.join(self._log_dir, client_log_name)
            count = int(self._duration / 10)
            full_client_cmd = '{cmd} 10 {count} > {log_name}'.format(cmd=cmd,
                                                                     count=count,
                                                                     log_name=client_log_path)
            all_cmds.append(full_client_cmd)
            curr_logs.append(client_log_path)

        # 5. Shoot the commands out------------------------------------------------------
        self.client_cmd(all_cmds, self._duration + 60)
        self._sweep_logs.extend(curr_logs)
        return curr_logs

    def plot(self):
        """
        Plot the logs: sysbench, system monitor logs (iostat, mpstat, vmstat, etc)
        :return:
        """
        log.info('Plotting the sweep')
        plot_files = ' '.join(self._sweep_logs)
        plot_cmd = './sysbench_plot.py -p {} {}'.format(self._log_dir, plot_files)

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
        subject_str = "Logs and graphs for sweep {}".format(self._log_dir)
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
    def result_is_good(curr_logs):
        """
        Check if the benchmark has been done successfully and raise an RuntimeError if some error happens.
        It's considered good if the sb_*.log (sysbench logs) contains 'execution time' in the tail.
        :param curr_logs:
        :return:
        """
        log.info('Checking if the sweep is in good state.')
        assert curr_logs
        for file in curr_logs:
            _, tail = os.path.split(file)
            if tail.startswith('sb'):
                check_cmd = "tail -2 {} | awk '{{print $1, $2}}'".format(file)
                started = check_output(check_cmd, shell=True, universal_newlines=True).replace("\n", " ")
                if 'execution time' not in started:
                    log.warning('Found error in {}.'.format(file))
                    return False
        return True

    def copy_db_file(self, remote_abs_path, local_file):
        """
        Inner function to cat db files from remote server
        :param remote_abs_path: absolute path
        :param local_file: relative path (just file name)
        :return:
        """
        local_path = os.path.join(self._log_dir, local_file)

        cmd = 'scp {user}@{db_ip}:{remote_path} {local_path} '.format(user=self._login_user,
                                                                      db_ip=self._db_ip,
                                                                      remote_path=remote_abs_path,
                                                                      local_path=local_path)
        self.client_cmd(cmd, timeout=10)

    def get_db_cnf_by_cmd(self, cmd, local_file):
        """Inner function to get database config from a specific command
        """
        local_file = os.path.join(self._log_dir, local_file)
        self.db_cmd(cmd, local_file)

    def start(self):
        """
        Start the sweep. The entry point of the benchmark(s).
        :return:
        """
        if not self._sweep_successful:
            return

        try:
            os.mkdir(self._log_dir)
            # Copy the sweep config file to the log directory.
            shutil.copy2(self._cnf_file, os.path.join(self._log_dir, self._cnf_file))
        except FileExistsError:
            pass
        except FileNotFoundError as e:
            log.warning('Sweep config file is gone now! {}'.format(e))

        log.info('Sweep <{}> started, check logs under that directory.'.format(self._log_dir))

        # Support only one sb_thread count config but leave the code here.
        for threads in self._threads:
            log.info('Benchmark for {} threads has started.'.format(threads))
            sweep.clean_client()
            sweep.clean_db()
            # Run the benchmark and check results
            if self.result_is_good(sweep.run_one_test(threads)):
                log.info('Benchmark for {} threads has finished.'.format(threads))
            else:
                self._sweep_successful = False
                log.error('Benchmark for {} threads has failed. Exiting...'.format(threads))
                # Rename the log directory with a prefix 'failed_'
                try:
                    os.rename(self._log_dir, self._log_dir + '_FAILED')
                    self._log_dir += '_FAILED'
                except (OSError, FileExistsError) as e:
                    log.warning('Failed to rename the log directory: {}'.format(e))

                break

        if self._plot and self._sweep_successful:
            self.plot()

        # Copy server config files
        self.copy_db_file('/etc/my.cnf', 'my.cnf')
        if self._target == 'DMX':
            self.copy_db_file('/dmx/etc/bfapp.d/mysqld', 'bfappd.mysqld')
            self.copy_db_file('/dmx/etc/bfcs.d/mysqld', 'bfcsd.mysqld')

        # Get the database server configurations and write to a log file
        self.get_db_cnf_by_cmd('barf --dv', 'barf.out')
        self.get_db_cnf_by_cmd('barf -v -l', 'barf.out')
        self.get_db_cnf_by_cmd('lscpu', 'server_os_info.out')
        self.get_db_cnf_by_cmd('free', 'server_os_info.out')

        if self._send_mail:
            self.send_mail()

        # Change the .cnf file to .done if it's successful.
        if self._sweep_successful:
            try:
                pure_fname, _ = os.path.splitext(self._cnf_file)
                os.rename(self._cnf_file, pure_fname + '.done')
            except (OSError, FileExistsError) as e:
                log.warning('Failed to rename the config file: {}'.format(e))


if __name__ == "__main__":
    """The main function to run the sweep.
    """
    parser = argparse.ArgumentParser(description="This program runs the benchmarks defined by a config file.")
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
    # I don't want to see paramiko debug logs, unless they are WARNING or worse than that.
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    log.info('******New sweep config file found, preparing to start.******')
    with Sweep(args.config) as sweep:
        try:
            sweep.start()
            if sweep.successful:
                log.info('The sweep has finished. Bye.')
            else:
                log.error('The sweep has failed.')
        except KeyboardInterrupt:
            log.warning('Received SIGINT. I will kill the running processes')
            # run clean-up of the sweep object
            sweep.close()
        except SweepFatalError:
            log.error('Fatal error. See above error messages.')
            sweep.close()

    sys.exit(0)
