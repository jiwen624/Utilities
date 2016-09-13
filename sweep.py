#!/usr/bin/env python3
""" This sweep program is used to run MySQL benchmarks with various configurations
    in client/server. Run this command on the client.
    External dependencies: plotter.py, cleandb.py
    
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
    7. A few enhancements and bug fixes.       --@EricYang v0.65 Sep 09, 2016
    8. Fixed a major bug: the _run_loal() should be reentrant.  --@EricYang v0.66 Sep10, 2016
    9. Used time comparison instead of the timer in _run_local()
       Fxied a bug of the code using epoll                      --@EricYang v0.67 Sep12, 2016
"""
import os
import re
import time
import sys
import logging
import psutil
import signal
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
            self._db_port = cnf.getint('server', 'db_port')
            self._client_ip = cnf.get('server', 'client_ip')
            self._dbscript_path = cnf.get('server', 'dbscript_path')
            self._login_user = cnf.get('server', 'dbserver_user')

            # section: benchmark
            tmp_log_dir, _ = os.path.splitext(self._cnf_file)
            self._log_dir = '{}_{}'.format(tmp_log_dir, time.strftime('%Y%m%d%H%M%S'))
            self._threads = cnf.getint('benchmark', 'sysbench_threads')
            self._db_num = cnf.getint('benchmark', 'db_num')
            self._target = cnf.get('benchmark', 'target')
            self._duration = cnf.getint('benchmark', 'duration')
            # self._db_size = cnf.get('benchmark', 'db_size').rstrip('G')
            self._lua_script = cnf.get('benchmark', 'lua_script')
            self._tarball_path = cnf.get('benchmark', 'tarball_path')
            self._tar_strips = cnf.getint('benchmark', 'tar_strip_components')
            self._db_name = cnf.get('benchmark', 'db_name')
            self._tblsize = cnf.get('benchmark', 'table_rows')
            self._tblnum = cnf.get('benchmark', 'table_num')
            self._base_dir = cnf.get('benchmark', 'mysql_base_dir')
            self._socket_prefix = cnf.get('benchmark', 'mysql_socket_file_prefix')
            self._sb_poll_interval = cnf.getint('benchmark', 'sysbench_poll_interval', fallback=1)

            # section: workload
            self._workload = cnf.get('workload', 'workload_type')
            self._read_only = True if self._workload == 'RO' else False
            self._point_selects = cnf.getint('workload', 'oltp_point_selects')
            self._simple_ranges = cnf.getint('workload', 'oltp_simple_ranges')
            self._sum_ranges = cnf.getint('workload', 'oltp_sum_ranges')
            self._order_ranges = cnf.getint('workload', 'oltp_order_ranges')
            self._distinct_ranges = cnf.getint('workload', 'oltp_distinct_ranges')
            self._index_updates = cnf.getint('workload', 'oltp_index_updates')
            self._non_index_updates = cnf.getint('workload', 'oltp_non_index_updates')

            # section: database
            self._db_parms = ' '.join(['{}={}'.format(k, v) for k, v in cnf.items('database')])

            track_active = cnf.getint('database', 'track_active')
            if self._target == 'RAM' and track_active != 0:
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
                self._smtp_port = cnf.getint('misc', 'smtp_port')

                if not ('@' in self._mail_recipients and '@' in self._mail_sender):
                    raise ValueError('Invalid email address in *mail_recipients*')

            self._sweep_logs = []
            self._ssd_device = cnf.get('misc', 'ssd_device')
            self._skip_db_recreation = True if cnf.get('misc', 'skip_db_recreation') == 'true' else False
            log.debug('Sweep config file: {} loaded.'.format(config_file))
            self.all_running_procs = []
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
        return self.all_running_procs

    @property
    def sysbench_threads(self):
        return self._threads

    @property
    def log_dir(self):
        return self._log_dir

    def _run_remote2(self, cmd, suppress=False):
        """
        The new interface to run a command remotely, with the enhancement that
        shows remote output in real time.
        This function needs to be enhanced to run multiple commands simultaneously.
        :param cmd:
        :return:
        """
        assert cmd
        if not suppress:
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

        while True:
            if session.recv_ready():
                buff = session.recv(4096).decode('utf-8').strip().replace('\r', '')
                if not suppress:
                    for line in buff.split('\n'):
                        log.info('[db] {}'.format(line))
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

    def run_db_cmd(self, cmd, suppress=False):
        """
        Run a command remotely on the database server
        :param suppress:
        :param cmd:
        :return: exit_status
        """
        # return self._run_remote(cmd)
        return self._run_remote2(cmd, suppress)

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
                log.error("Invalid cmd: {}".format(commands))

        # shell=True is not the best practice but let's keep it for now.
        running_procs = [Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE,
                               universal_newlines=True, close_fds=True,
                               preexec_fn=os.setsid) for cmd in commands]
        self.all_running_procs.extend(running_procs)
        _timeout = timeout
        start = time.time()

        # Use context management to close epoll object in the end.
        with select.epoll() as p:
            pipe_dict = {}

            for proc in running_procs:
                p.register(proc.stdout, select.POLLIN | select.POLLERR | select.POLLHUP)
                stdout_fileno = proc.stdout.fileno()
                pipe_dict[stdout_fileno] = proc.stdout
                log.info('(pid:{}) cmd=({})'.format(proc.pid, proc.args))

            # Check the output of commands in real-time and poll the status of command:
            #       1. Finished (Done|Failed)
            #       2. Still running.
            # Remove the process if it has finished so the loop will exit
            # when no process is running.
            while running_procs and (time.time() - start) < _timeout:
                # Get the processes list which have printed something.
                result = p.poll(timeout=1)
                if len(result):
                    # result --> a list of processes structs
                    # m[0]   --> file_no of the stdout of that process.
                    #            the stderr is also redirected to PIPE
                    # m[1]   --> signal
                    for fd, event in result:
                        if event & select.POLLIN:
                            out_str = pipe_dict[fd].readline().strip()
                            log.debug('(id:{}) {}'.format(fd, out_str))

                # Check the running status of the processes.
                for proc in running_procs:
                    ret = proc.poll()
                    if ret is not None:  # Process finished.
                        # Remove finished process ASAP from local and global lists,
                        # as well as epoll list
                        try:
                            running_procs.remove(proc)
                            self.all_running_procs.remove(proc)
                            p.unregister(proc.stdout)
                        except ValueError:
                            pass

                        _, errors_raw = proc.communicate()
                        errors = errors_raw.rstrip()

                        if ret != 0:  # Process failed.
                            log.warning('Command failed: {}'.format(proc.args))
                            log.warning('(errs={})'.format(errors))
                            # watcher.cancel()

                            # Check if sysbench is failed and do fast-fail if so:
                            if 'sysbench' in proc.args:  # sysbench failure is a critical error.
                                log.error('Fatal error found in sysbench, exiting...')

                                # Clean the running process list to quit the loop,
                                # as all the processes have been killed in self.close()
                                self._sweep_successful = False
                                _timeout = 0
                            else:  # Just ignore failures from the other commands
                                #  self._running_procs.remove(proc)
                                pass
                        else:
                            log.info('Done: (cmd={})'.format(proc.args))
                            if 'sysbench' in proc.args:  # sysbench failure is a critical error.
                                log.info('Sysbench is done after {} sec'.format(int(time.time() - start)))
                                self._sweep_successful = True
                                # To break out of the outer while loop
                                _timeout = 0

                        # Break out from the inner loop after we found a finished process
                        # We'll not check the next process here, as we need to check the
                        # stdout first.
                        break
                    else:
                        # This process is still running.
                        # So we check the next command in the running_procs list.
                        continue
        # Kill all the local running processes when the sweep is successfully finished.
        # When a fatal error happens, the running_procs will be empty here, all the local
        # processes will be killed outside of this function, when the Sweep object is released.
        for proc in running_procs:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                log.info('Killed: ({}) {}.'.format(proc.pid, proc.args))
                self.all_running_procs.remove(proc)
            except (ProcessLookupError, BrokenPipeError, ValueError) as e:
                print('Error in killing ({}) ({})'.format(proc.pid, e))

    def copy_mysql_logs(self):
        """
        This function copies MySQL logs to sweep directory when sysbench is failed.
        :return:
        """
        exit_status, hostname = self.run_db_cmd('hostname', suppress=True)
        hostname = hostname.rstrip('\n')
        for idx in range(1, self._db_num + 1):
            mysql_path = os.path.join(self._base_dir, 'mysql{idx}'.format(idx=idx))
            mysql_log_name = '{hostname}.err'.format(hostname=hostname)
            mysql_log_path = os.path.join(mysql_path, mysql_log_name)
            local_log_name = 'mysql{idx}_{orig_name}'.format(idx=idx, orig_name=mysql_log_name)
            log.info('Copying {} from db to local: {}'.format(mysql_log_path, local_log_name))
            self.copy_db_file(mysql_log_path, local_log_name)

    def kill_running_procs(self):
        """
        Kill all the running processes. For now this function is only called
        when the Sweep object is closing.
        :return:
        """
        procs = self.all_running_procs
        self.all_running_procs = []

        for proc in procs:
            # log.debug('Killing: ({}) {}.'.format(proc.pid, proc.args))
            # proc.kill()  # This would not work for 'shell=True'
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                log.info('Killed: ({}) {}.'.format(proc.pid, proc.args))
            except (ProcessLookupError, BrokenPipeError):
                print('Failed to kill process ({})'.format(proc.pid))

        # Kill tdctl and monitor as these two commands won't exit by themselves.
        self.run_db_cmd('killall tdctl monitor')

    def run_client_cmd(self, cmds, timeout):
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
        staging_dir = '/tmp/{}'.format(self._log_dir)
        cmd_template = '{cleanup_script} {db_num} {skip_db_recreation} -n {staging_dir} ' \
                       '-d {base_dir} -z {tarball} -s {strips} -v -p "{parameters}" 2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_num=self._db_num,
                                           skip_db_recreation=skip_db_recreation,
                                           staging_dir=staging_dir,
                                           base_dir=self._base_dir,
                                           tarball=self._tarball_path,
                                           strips=self._tar_strips,
                                           parameters=self._db_parms,
                                           log_path=self._dbscript_path)
        exit_status, result = self.run_db_cmd(clean_db_cmd)
        # log.debug('clean_db exit status: {}'.format(exit_status))

        # The cleanup.py from the database server is failed.
        if not self.db_is_ready(exit_status, result):
            log.error('Database cleanup failed, err_code: {}'.format(exit_status))
            self._sweep_successful = False
            raise SweepFatalError

    @staticmethod
    def db_is_ready(cleandb_retcode, result):
        """
        Check if the database cleanup script has returned successfully.
        :param cleandb_retcode:
        :param result:
        :return:
        """
        # cleandb_retcode is useless now.
        log.debug('Database cleanup script returns: {}'.format(cleandb_retcode))

        if '***Database is ready.***' in result:
            return True
        else:
            return False

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
                raise SweepFatalError("Invalid proc name: {}".format(str(proc_names)))
        # TODO: This loop is time-consuming...
        for proc in psutil.process_iter():
            for name in proc_names:
                try:
                    if name in ' '.join(proc.cmdline()) and proc.pid != skip_pid:
                        log.debug('Killing:({}) {}'.format(proc.pid, ' '.join(proc.cmdline())))
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
        self_pid = os.getpid()
        # Should these processes be cleaned up on the server side?
        proc_names = ["iostat",
                      "mpstat",
                      "vmstat",
                      "tdctl",
                      "sysbench",
                      "mysql"]

        self.kill_proc_by_name(proc_names, self_pid)

        # Kill previous sweep which may still be running
        _, exec_file = os.path.split(__file__)
        self.kill_proc_by_name(exec_file, self_pid)

        time.sleep(5)
        log.info('***Client is ready.***')

    def run_one_test(self, thread_cnt):
        """
        Run a benchmark with sysbench thread number=thread_num
        - and may plot and compress them.
        - and send an email.
        :param thread_cnt:
        :return:
        """
        assert thread_cnt is not None

        log.info('******Running test of {} sysbench threads*****'.format(thread_cnt))
        log.info('Go to {ip}:/tmp/{dir} for db-side logs.'.format(ip=self._db_ip,
                                                                  dir=self._log_dir))

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
                       '--report-interval={sysbench_poll_interval} ' \
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

        for port in range(self._db_port, self._db_port + self._db_num):
            db_idx = port - self._db_port + 1
            # For each instance, record sysbench logs and innodb status logs, etc.
            # 1. the sysbench logs:
            sb_log_name = 'sb_{}_{}_db{}.log'.format(self._target,
                                                     thread_cnt,
                                                     db_idx)
            sb_log = os.path.join(self._log_dir, sb_log_name)

            sb_cmd = cmd_template.format(lua_script=self._lua_script,
                                         oltp_table_size=self._tblsize,
                                         oltp_tables_count=self._tblnum,
                                         mysql_host=self._db_ip,
                                         mysql_port=port,
                                         db_name=self._db_name,
                                         thread_num=thread_cnt,
                                         max_time=self._duration,
                                         sysbench_poll_interval=self._sb_poll_interval,
                                         oltp_read_only='on' if self._read_only else 'off',
                                         oltp_point_selects=self._point_selects,
                                         oltp_simple_ranges=self._simple_ranges,
                                         oltp_sum_ranges=self._sum_ranges,
                                         oltp_order_ranges=self._order_ranges,
                                         oltp_distinct_ranges=self._distinct_ranges,
                                         oltp_index_updates=self._index_updates,
                                         oltp_non_index_updates=self._non_index_updates,
                                         log_name=sb_log)
            all_cmds.append(sb_cmd)
            curr_logs.append(sb_log)

            # 2. The innodb status logs - every 60 seconds------------------------------
            innodb_log_name = 'innodb_status_db{}.log'.format(db_idx)
            innodb_log = os.path.join(self._log_dir, innodb_log_name)
            innodb_cmd = "while true; " \
                         "do " \
                         "ssh {user}@{ip} \"mysql -S {socket_prefix}{db_idx} -e " \
                         "'show engine innodb status\G' | grep -A 28 -E 'LOG|END' " \
                         "&>> /tmp/{log_name}\"; " \
                         "  sleep 60; " \
                         "done".format(user=self._login_user,
                                       ip=self._db_ip,
                                       socket_prefix=self._socket_prefix,
                                       db_idx=db_idx,
                                       log_dir=self._log_dir,
                                       log_name=innodb_log)

            all_cmds.append(innodb_cmd)
            curr_logs.append(innodb_log)

        # 3. Commands for system monitoring---------------------------------------------
        os_cmds = ('iostat -dmx {} -y'.format(self._ssd_device),
                   'mpstat',
                   'vmstat -S M -w',
                   'tdctl -v --dp +')
        for cmd in os_cmds:
            sys_log_name = '{}_{}_{}.log'.format(cmd.split()[0], self._target, thread_cnt)
            sys_log = os.path.join(self._log_dir, sys_log_name)
            count = '' if 'tdctl' in cmd else int(self._duration / 10)
            sys_cmd = 'ssh {user}@{ip} "{cmd} 10 {count} ' \
                      '&> /tmp/{log_name}"'.format(user=self._login_user,
                                                   ip=self._db_ip,
                                                   cmd=cmd,
                                                   count=count,
                                                   log_dir=self._log_dir,
                                                   log_name=sys_log)
            all_cmds.append(sys_cmd)
            curr_logs.append(sys_log)

        # 4. Commands for client monitoring---------------------------------------------
        client_cmds = ('vmstat -S M -w',)
        for cmd in client_cmds:
            client_log_name = '{}_{}_{}_client.log'.format(cmd.split()[0],
                                                           self._target, thread_cnt)
            client_log = os.path.join(self._log_dir, client_log_name)
            count = int(self._duration / 10)
            full_client_cmd = '{cmd} 10 {count} &> {log_name}'.format(cmd=cmd,
                                                                      count=count,
                                                                      log_name=client_log)
            all_cmds.append(full_client_cmd)
            curr_logs.append(client_log)

        # 5. The dmx monitoring logs: barf --fr - every 10 seconds------------------------------
        barf_fr_log = os.path.join(self._log_dir, 'barffr_.log')
        barf_fr_cmd = "while true; " \
                      "do " \
                      "ssh {user}@{ip} 'barf --fr &>> /tmp/{log_name}'; " \
                      "sleep 10; " \
                      "done".format(user=self._login_user,
                                    ip=self._db_ip,
                                    log_dir=self._log_dir,
                                    log_name=barf_fr_log)

        all_cmds.append(barf_fr_cmd)
        curr_logs.append(barf_fr_log)

        # 6. The dmx monitoring logs: barf -a --ct algo - every 10 seconds------------------------
        barf_act_algo_log = os.path.join(self._log_dir, 'barf_a_ct_algo.log')
        barf_act_algo_cmd = "while true; " \
                            "do " \
                            "ssh {user}@{ip} 'barf -a --ct algo &>> /tmp/{log_name}'; " \
                            "sleep 10; " \
                            "done".format(user=self._login_user,
                                          ip=self._db_ip,
                                          log_dir=self._log_dir,
                                          log_name=barf_act_algo_log)

        all_cmds.append(barf_act_algo_cmd)
        curr_logs.append(barf_act_algo_log)

        # 7. The dmx monitoring logs: barf -a --ct bf - every 10 seconds---------------------
        barf_act_bf_log = os.path.join(self._log_dir, 'barf_a_ct_bf.log')
        barf_act_bf_cmd = "while true; " \
                          "do " \
                          "ssh {user}@{ip} 'barf -a --ct bf &>> /tmp/{log_name}'; " \
                          "sleep 10; " \
                          "done".format(user=self._login_user,
                                        ip=self._db_ip,
                                        log_dir=self._log_dir,
                                        log_name=barf_act_bf_log)

        all_cmds.append(barf_act_bf_cmd)
        curr_logs.append(barf_act_bf_log)

        # 8. The dmx monitoring logs: monitor, every 10 seconds------------------------------
        pids = self.get_database_pid()
        for idx, pid in enumerate(pids, 1):
            if not pid:
                continue
            monitor_log = os.path.join(self._log_dir, 'monitor_p_db{}.log'.format(idx))
            monitor = 'monitor -p {pid} -D 10'.format(pid=pid)
            monitor_cmd = 'ssh {user}@{ip} ' \
                          '"{cmd} &> /tmp/{log_name}"'.format(user=self._login_user,
                                                              ip=self._db_ip,
                                                              cmd=monitor,
                                                              log_dir=self._log_dir,
                                                              log_name=monitor_log)
            all_cmds.append(monitor_cmd)
            curr_logs.append(monitor_log)

        # 9. The network traffic logs: sar, every 10 seconds------------------------------

        sar_log = os.path.join(self._log_dir, 'network_traffic.log')
        sar = "sar -n DEV 10 {cnt} " \
              "|grep -E `ip addr show | grep {ip} | awk '{{print $NF}}'` " \
              "&> {log_name}"
        sar_cmd = sar.format(cnt=int(self._duration / 10),
                             ip=self._client_ip,
                             log_name=sar_log)

        all_cmds.append(sar_cmd)
        curr_logs.append(sar_log)

        # 10. Shoot the commands out------------------------------------------------------
        # Allow 300 seconds more time to let the commands quit by themselves.
        self.run_client_cmd(all_cmds, self._duration + 180)
        self._sweep_logs.extend(curr_logs)
        return curr_logs

    def plot(self):
        """
        Plot the logs: sysbench, system monitor logs (iostat, mpstat, vmstat, etc)
        :return:
        """
        log.info('Plotting the sweep')
        plot_files = ' '.join(self._sweep_logs)
        plot_cmd = './plotter.py -p {} {}'.format(self._log_dir, plot_files)

        # The timeout of plot is 600 seconds, it will be killed if
        # not return before timeout
        self.run_client_cmd(plot_cmd, 600)

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
        self.run_client_cmd(sendmail_cmd, timeout=600)

    @staticmethod
    def result_is_good(curr_logs):
        """
        Check if the benchmark has been done successfully and raise an RuntimeError
        if some error happens. It's considered good if the sb_*.log (sysbench logs)
        contains 'execution time' in the tail.
        :param curr_logs:
        :return:
        """
        log.info('Checking if the sweep is in good state.')
        assert curr_logs
        for file in curr_logs:
            _, tail = os.path.split(file)
            if tail.startswith('sb'):
                check_cmd = "tail -2 {} | awk '{{print $1, $2}}'".format(file)
                # The return value of check_output will be a string since
                # universal_newlines is True
                started = check_output(check_cmd,
                                       shell=True,
                                       universal_newlines=True).replace("\n", " ")
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
        if os.path.isabs(local_file):
            local_path = local_file
        else:
            local_path = os.path.join(self._log_dir, local_file)

        cmd = 'scp -r {user}@{ip}:{frm} {to} '.format(user=self._login_user,
                                                      ip=self._db_ip,
                                                      frm=remote_abs_path,
                                                      to=local_path)
        self.run_client_cmd(cmd, timeout=10)

    def get_db_cnf_by_cmd(self, cmd, save_to):
        """Inner function to get database config from a specific command
        """
        save_to = os.path.join(self._log_dir, save_to)
        exit_status, result = self.run_db_cmd(cmd, suppress=True)

        if exit_status != 0:
            pass  # TODO: it should return here.

        try:
            with open(save_to, 'a') as out_file:
                out_file.write(result)
        except IOError:
            log.warning('Cannot open {} for db command output'.format(save_to))

    def get_database_pid(self):
        """
        This function returns a list which contains the pid of all the MySQL processes
        :return:
        """
        cmd = "grep pid-file /etc/my.cnf | awk -F= '{{print $NF}}' " \
              "| head -{} | xargs cat".format(self._db_num)
        exit_status, pids = self.run_db_cmd(cmd, suppress=True)
        return pids.split('\n')

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
            cnf_file = os.path.join(self._log_dir, self._cnf_file)
            shutil.copy2(self._cnf_file, cnf_file)
        except FileExistsError:
            pass
        except FileNotFoundError as e:
            log.warning('Sweep config file is gone now! {}'.format(e))

        log.info('Sweep <{}> started, check logs in that directory.'.format(self._log_dir))

        self.clean_client()
        self.clean_db()
        # Run the benchmark and check results
        if self.result_is_good(self.run_one_test(self._threads)):
            log.info('Benchmark for {} threads has finished.'.format(self._threads))
        else:
            self._sweep_successful = False
            log.error('Benchmark for {} threads has failed.'.format(self._threads))

        # Copy server config files
        self.copy_db_file('/etc/my.cnf', 'my.cnf')

        # Copy remote logs from staging area
        staging_logs = '/tmp/{}/*'.format(self._log_dir)
        self.copy_db_file(staging_logs, './')

        if self._target == 'DMX':
            log.info('Copying mysqld config file under bfapp.d and bfcs.d')
            self.copy_db_file('/dmx/etc/bfapp.d/mysqld', 'bfappd.mysqld')
            self.copy_db_file('/dmx/etc/bfcs.d/mysqld', 'bfcsd.mysqld')

        # Get the database server configurations and write to a log file
        log.info('Fetching database h/w and driver information. ')
        self.get_db_cnf_by_cmd('barf --dv', 'barf.out')
        self.get_db_cnf_by_cmd('barf -v -l', 'barf.out')
        self.get_db_cnf_by_cmd('free', 'server_os_info.out')

        lscpu = "lscpu | grep -Ev 'Architecture|Order|cache|[F|f]amily|Vendor" \
                "|Stepping|op-mode|Model:|node[0-9]|MIPS'"
        self.get_db_cnf_by_cmd(lscpu, 'server_os_info.out')

        # Plot the logs after sweep.
        if self._plot and self._sweep_successful:
            self.plot()

        if self._send_mail:
            self.send_mail()

        # Change the .cnf file to .done if it's successful.
        if self._sweep_successful:
            try:
                pure_fname, _ = os.path.splitext(self._cnf_file)
                os.rename(self._cnf_file, pure_fname + '.done')
            except (OSError, FileExistsError) as e:
                log.warning('Failed to rename the config file: {}'.format(e))
        else:
            # Copy MySQL logs from db server for further diagnosis.
            self.copy_mysql_logs()
            # Rename the log directory with a prefix 'failed_'
            try:
                log.info('Marked the log directory with suffix _FAILED.')
                os.rename(self._log_dir, self._log_dir + '_FAILED')
                self._log_dir += '_FAILED'
            except (OSError, FileExistsError) as e:
                log.warning('Failed to rename the log directory: {}'.format(e))


if __name__ == "__main__":
    """The main function to run the sweep.
    """
    cmd_desc = "This program runs the benchmarks defined by a config file."
    parser = argparse.ArgumentParser(description=cmd_desc)
    parser.add_argument("config", help="config file name/path")
    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug, -vvv: colored)",
                        action='count', default=0)

    args = parser.parse_args()

    log_level = logging.INFO
    if args.v == 0:
        log_level = logging.ERROR
    elif args.v == 1:
        log_level = logging.INFO
    elif args.v == 2:
        log_level = logging.DEBUG
    elif args.v >= 3:
        log_level = logging.DEBUG
        import coloredlogs

        coloredlogs.install(level='INFO', fmt='%(asctime)s: %(message)s')

    logging.basicConfig(level=log_level,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m-%d %H:%M:%S')
    # I don't want to see paramiko debug logs, unless they are WARNING or worse than that.
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    log.info('-----------------------------------------------------------')
    log.info('------New sweep config file found, preparing to start------')
    log.info('-----------------------------------------------------------')

    status = 'unknown'
    with Sweep(args.config) as sweep:
        try:
            sweep.start()
            status = 'finished' if sweep.successful else 'failed'

        except KeyboardInterrupt:
            # We cannot use logging here as the pipe is already broken
            status = 'canceled'
            print('Ctrl-C pressed by user. I will kill the running processes')
            try:
                os.rename(sweep.log_dir, sweep.log_dir + '_CANCELED')
            except (OSError, FileExistsError, FileNotFoundError):
                pass

        except SweepFatalError:
            status = 'failed'
            print('Fatal error. See above error messages.')

    log.info('The sweep is {}. Bye.'.format(status))
    sys.exit(0)
