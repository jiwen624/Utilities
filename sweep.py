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
    8. Fixed a major bug: the _run_local() should be reentrant.  --@EricYang v0.66 Sep10, 2016
    9. Used time comparison instead of the timer in _run_local()
       Fixed a bug of the code using epoll                      --@EricYang v0.67 Sep12, 2016
    10. Now sweep can send the redo/buffer size to plotter to get the percentages of lags.
                                                                --@EricYang v0.68 Sep13, 2016
"""
import os
import time
import sys
import logging
import psutil
import signal
import argparse
import paramiko
import zipfile
import shutil
from subprocess import Popen, check_output, PIPE
from configparser import ConfigParser, NoSectionError, NoOptionError
from select import epoll, POLLIN, POLLERR, POLLHUP

log = logging.getLogger('')
SweepFatalError = RuntimeError


class Sweep:
    """
    This class launches a sysbench benchmark sweep with a certain number of threads.
    Basically, the major steps of a benchmark are:
        1. read benchmark config files.
        2. clean up the database server (remove old databases, create new ones and
           start them up, kill old processes, etc.)
        3. clean up the client environment: kill old processes, etc.
        4. run the benchmarks and collect logs.
        5. plot the logs and (maybe) send emails.
    Usage:
        sweep = Sweep('test.cnf')
        sweep.start()
    """
    _id_rsa_path = '~/.ssh/id_rsa'
    _ssh_port = 22

    def __init__(self, config_file):
        self._sweep_logs = []
        self._running_procs = []
        self._trans = None  # Paramiko Transport object
        self._sweep_successful = True

        cnf = ConfigParser()

        try:
            self._cnf_file = config_file
            cnf.read(config_file)
            log.debug('Sweep config file: {} loaded.'.format(config_file))

            # Section: server
            self._db_ip = cnf.get('server', 'db_ip')
            self._db_port = cnf.getint('server', 'db_port')
            self._client_ip = cnf.get('server', 'client_ip')
            self._db_script_dir = cnf.get('server', 'dbscript_path')
            self._login_user = cnf.get('server', 'dbserver_user', fallback='root')

            # Section: benchmark
            cnf_name, _ = os.path.splitext(self._cnf_file)
            self._log_dir = '{}_{}'.format(cnf_name, time.strftime('%Y%m%d%H%M%S'))
            self._threads = cnf.getint('benchmark', 'sysbench_threads')
            self._db_num = cnf.getint('benchmark', 'db_num')
            self._target = cnf.get('benchmark', 'target')
            self._duration = cnf.getint('benchmark', 'duration')
            self._lua_script = cnf.get('benchmark', 'lua_script')
            self._tarball_path = cnf.get('benchmark', 'tarball_path')
            self._tar_strips = cnf.getint('benchmark', 'tar_strip_components', fallback=0)
            self._db_name = cnf.get('benchmark', 'db_name', fallback='sbtest')
            self._db_user = cnf.get('benchmark', 'db_user', fallback='sbtest')
            self._db_pwd = cnf.get('benchmark', 'db_pwd', fallback='sbtest')
            self._table_rows = cnf.get('benchmark', 'table_rows')
            self._table_num = cnf.get('benchmark', 'table_num')
            self._base_dir = cnf.get('benchmark', 'mysql_base_dir', fallback='/var/lib/mysql')
            self._socket_prefix = cnf.get('benchmark', 'mysql_socket_file_prefix')
            self._db_start_timeout = cnf.getint('benchmark', 'db_start_timeout', fallback=1800)

            # Section: poll_intervals
            self._sb_poll_interval = cnf.getint('poll_intervals', 'sysbench', fallback=1)
            self._innodb_poll_interval = cnf.getint('poll_intervals', 'innodb', fallback=60)

            # Section: workload
            self._workload = cnf.get('workload', 'workload_type')
            self._read_only = (self._workload == 'RO')
            self._rand_type = cnf.get('workload', 'rand_type', fallback='uniform')
            self._point_selects = cnf.getint('workload', 'oltp_point_selects')
            self._simple_ranges = cnf.getint('workload', 'oltp_simple_ranges')
            self._sum_ranges = cnf.getint('workload', 'oltp_sum_ranges')
            self._order_ranges = cnf.getint('workload', 'oltp_order_ranges')
            self._distinct_ranges = cnf.getint('workload', 'oltp_distinct_ranges')
            self._index_updates = cnf.getint('workload', 'oltp_index_updates')
            self._non_index_updates = cnf.getint('workload', 'oltp_non_index_updates')

            # Section: database
            self._db_params = dict(cnf.items('database'))
            self._redo_bytes = self.get_bytes(self.combined_db_config('innodb_log_file_size'))
            self._buffer_bytes = self.get_bytes(self.combined_db_config('innodb_buffer_pool_size'))

            self._track_active = int(self._db_params.get('track_active', 0))

            # Section: misc
            self._plot = (cnf.get('misc', 'plot', fallback=None) == 'true')
            self._send_mail = (cnf.get('misc', 'send_mail', fallback=None) == 'true')

            self._mail_sender = cnf.get('misc', 'mail_sender', fallback=None)
            self._mail_recipients = cnf.get('misc', 'mail_recipients', fallback=None)
            self._smtp_server = cnf.get('misc', 'smtp_server', fallback=None)
            self._smtp_port = cnf.getint('misc', 'smtp_port', fallback=None)

            self._ssd_device = cnf.get('misc', 'ssd_device')
            self._skip_db_recreation = (cnf.get('misc', 'skip_db_recreation') == 'true')
            self._check_config = cnf.get('misc', 'check_config', fallback='true')

            # Check if there are any logical errors in the configurations.
            if self._check_config == 'true':
                self._sanity_check()

        except (NoSectionError, NoOptionError, TypeError, KeyError, ValueError) as e:
            log.error('Invalid config file or unsupported option:{}'.format(config_file))
            _, _, exc_tb = sys.exc_info()
            log.error('Line {}:{}'.format(exc_tb.tb_lineno, e))
            self._sweep_successful = False
            return

    def combined_db_config(self, param):
        """
        This function get the value of a specific MySQL config, either from /etc/my.cnf.baseline
        or from the sweep config file. The latter will override the former one.
        :type param: string
        :return: a string represent the value of this parameter.
        """
        sweep_param = 'mysql_' + param
        value = self._db_params.get(sweep_param)
        if value is None:
            cmd = "grep {param} /etc/my.cnf.baseline | awk -F= '{{print $NF}}'".format(param=param)
            _, value = self.run_db_cmd(cmd, suppress=True)
            value = value.rstrip('\n')
        return value

    def _sanity_check(self):
        """
        This function does some basic sanity check to eliminate some configuration errors.
        If anything is error, it will raise ValueError (may not be an appropriate exception)
        :return:
        """
        self._local_misc_check()
        self._check_sb_threads()
        self._check_buffer_pool()

        # This print should be the last line of code
        log.debug('Config file sanity check passed.')

    def _local_misc_check(self):
        """
        Check some misc config locally
        :return:
        """
        if not (0 <= self._track_active <= 100):
            raise ValueError('track_active out of range (0-100): {}'.format(self._track_active))

        if self._target == 'RAM' and self._track_active != 0:
            raise ValueError('Invalid track_active for RAM: {}'.format(self._track_active))

        if self._send_mail:
            if not ('@' in self._mail_recipients and '@' in self._mail_sender):
                raise ValueError('Invalid email address in *mail_recipients*')

    def _check_sb_threads(self):
        """
        Sanity check: if the specified sysbench threads count is supported by current
        MySQL configuration
        :return:
        """
        db_supported_cnt = int(self.combined_db_config('max_connections'))

        if self._threads >= db_supported_cnt:
            raise ValueError('Too many sysbench threads: {}, '
                             'max_connections@my.cnf is only {}'.format(self._threads,
                                                                        db_supported_cnt))

    def _check_buffer_pool(self):
        """
        This functions checks if the buffer_pool size is larger than the RAM size.
        :return:
        """
        if self._target == 'DMX':
            return
        else:
            bp_size = self.combined_db_config('innodb_buffer_pool_size')
            cmd = "cat /proc/meminfo |grep MemTotal | awk '{print $2}'"
            _, ret = self.run_db_cmd(cmd, suppress=True)
            ram_size = ret.rstrip('\n') + 'K'
            if self.get_bytes(bp_size) >= self.get_bytes(ram_size):
                raise ValueError('Too large buffer pool:{}, RAM:{}.'.format(bp_size, ram_size))

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
    def successful(self):
        return self._sweep_successful

    @property
    def running_procs(self):
        return self._running_procs

    @property
    def sysbench_threads(self):
        return self._threads

    @property
    def log_dir(self):
        return self._log_dir

    @staticmethod
    def get_bytes(size_str):
        """
        This function converts a size with unit in string into a number
        :param size_str:   '32G', '16800M', etc.
        :return:
        """
        matrix = {'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3}
        unit = size_str[-1]
        if unit not in matrix.keys():
            raise ValueError
        size = int(size_str[0:-1])
        return size * matrix.get(unit)

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
            log.debug('[db] {}'.format(cmd))
        # Reuse the Transport object if there is already there.
        if self._trans is None:
            self._trans = paramiko.Transport(self._db_ip, self._ssh_port)

        # Reconnect to remote server if the connection is inactive.
        if not self._trans.is_active():
            key_path = os.path.expanduser(self._id_rsa_path)
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
                        log.debug('[db] {}'.format(line))
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
        try:
            if self._trans:
                self._trans.close()
                self._trans = None
        # AttributeError may happen if an error happens in __init__()
        except AttributeError:
            pass

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

        running_procs = []
        # shell=True is not the best practice but let's keep it for now.
        # Sleep for a while before running another command.
        for cmd in commands:
            running_procs.append(Popen(cmd, shell=True, stdout=PIPE, stderr=PIPE,
                                       universal_newlines=True, close_fds=True,
                                       preexec_fn=os.setsid))
            time.sleep(0.1)

        self._running_procs.extend(running_procs)
        _timeout = timeout
        start = time.time()

        # Use context management to close epoll object in the end.
        # with I/O multiplexing we can run and check multiple commands in parallel.
        with epoll() as p:
            pipe_dict = {}

            for proc in running_procs:
                p.register(proc.stdout, POLLIN | POLLERR | POLLHUP)
                stdout_fd = proc.stdout.fileno()
                pipe_dict[stdout_fd] = proc.stdout
                log.debug('(pid:{}) cmd=({})'.format(proc.pid, proc.args))

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
                        if event & POLLIN:
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
                            self._running_procs.remove(proc)
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
                                log.info('Sysbench done after {}s.'.format(int(time.time() - start)))
                                self._sweep_successful = True
                                # Wait another 20 seconds to allow system monitor commands to quit.
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
        # When a fatal error happens, the 'running_procs' will be empty here, all the local
        # processes will be killed outside of this function, when the Sweep object is released.
        for proc in running_procs:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                log.debug('Killed: ({}) {}.'.format(proc.pid, proc.args))
                self._running_procs.remove(proc)
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
            m_log_name = '{hostname}.err'.format(hostname=hostname)
            m_log_path = os.path.join(mysql_path, m_log_name)
            local_log_name = 'mysql{idx}_{orig_name}'.format(idx=idx, orig_name=m_log_name)
            log.debug('Copying {} from db to local: {}'.format(m_log_path, local_log_name))
            self.copy_db_file(m_log_path, local_log_name)

    def kill_running_procs(self):
        """
        Kill all the running processes. For now this function is only called
        when the Sweep object is closing.
        :return:
        """
        try:
            procs = self._running_procs
            self._running_procs = []
        # An error happens in __init__() before all_running_procs is defined.
        except AttributeError:
            return

        for proc in procs:
            # log.debug('Killing: ({}) {}.'.format(proc.pid, proc.args))
            # proc.kill()  # This would not work for 'shell=True'
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
                log.debug('Killed: ({}) {}.'.format(proc.pid, proc.args))
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
        cleanup_script = os.path.join(self._db_script_dir,
                                      'cleandb.py')

        skip_db_recreation = '-o skip_db_recreation' if self._skip_db_recreation else ''
        staging_dir = '/tmp/{}'.format(self._log_dir)
        params = ' '.join(['{}={}'.format(k, v) for k, v in self._db_params.items()])
        cmd_template = '{cleanup_script} {db_num} {skip_db_recreation} -n {staging_dir} ' \
                       '-d {base_dir} -z {tarball} -t {timeout} -s {strips} -v -p "{params}" 2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_num=self._db_num,
                                           skip_db_recreation=skip_db_recreation,
                                           staging_dir=staging_dir,
                                           base_dir=self._base_dir,
                                           tarball=self._tarball_path,
                                           timeout=self._db_start_timeout,
                                           strips=self._tar_strips,
                                           params=params,
                                           log_path=self._db_script_dir)
        exit_status, result = self.run_db_cmd(clean_db_cmd)
        # log.debug('clean_db exit status: {}'.format(exit_status))

        # The cleanup.py from the database server is failed.
        if not self.db_is_ready(exit_status, result):
            log.error('Database cleanup failed, err_code: {}'.format(exit_status))
            self._sweep_successful = False
            raise SweepFatalError

    @staticmethod
    def db_is_ready(ret_code, result):
        """
        Check if the database cleanup script has returned successfully.
        :param ret_code:
        :param result:
        :return:
        """
        # cleandb_retcode is useless now.
        log.debug('Database cleanup script returns: {}'.format(ret_code))

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
        Run a benchmark with sysbench thread number=thread_num. This function will
        launch a bunch of commands (sysbench, other system monitoring commands.) and
        record the logs to remote database server. After the benchmark is finished,
        the logs will be copied from database server to this server.
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
                       '--mysql-user={db_user} ' \
                       '--mysql-password={db_pwd} ' \
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
                       '--rand-init={rand_init} {rand_type} ' \
                       'run > {log_name}'

        rand_init = 'off' if self._rand_type == 'off' else 'on'
        rand_type = '' if self._rand_type == 'off' else '--rand-type={}'.format(self._rand_type)
        oltp_read_only = 'on' if self._read_only else 'off'

        for port in range(self._db_port, self._db_port + self._db_num):
            db_idx = port - self._db_port + 1
            # For each instance, record sysbench logs and innodb status logs, etc.
            # 1. the sysbench logs:
            sb_log_name = 'sb_{}_{}_db{}.log'.format(self._target,
                                                     thread_cnt,
                                                     db_idx)
            sb_log = os.path.join(self._log_dir, sb_log_name)

            sb_cmd = cmd_template.format(lua_script=self._lua_script,
                                         oltp_table_size=self._table_rows,
                                         oltp_tables_count=self._table_num,
                                         mysql_host=self._db_ip,
                                         mysql_port=port,
                                         db_name=self._db_name,
                                         db_user=self._db_user,
                                         db_pwd=self._db_pwd,
                                         thread_num=thread_cnt,
                                         max_time=self._duration,
                                         sysbench_poll_interval=self._sb_poll_interval,
                                         oltp_read_only=oltp_read_only,
                                         oltp_point_selects=self._point_selects,
                                         oltp_simple_ranges=self._simple_ranges,
                                         oltp_sum_ranges=self._sum_ranges,
                                         oltp_order_ranges=self._order_ranges,
                                         oltp_distinct_ranges=self._distinct_ranges,
                                         oltp_index_updates=self._index_updates,
                                         oltp_non_index_updates=self._non_index_updates,
                                         rand_init=rand_init,
                                         rand_type=rand_type,
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
                         "  sleep {innodb_poll_interval}; " \
                         "done".format(user=self._login_user,
                                       ip=self._db_ip,
                                       socket_prefix=self._socket_prefix,
                                       db_idx=db_idx,
                                       log_dir=self._log_dir,
                                       log_name=innodb_log,
                                       innodb_poll_interval=self._innodb_poll_interval)

            all_cmds.append(innodb_cmd)
            curr_logs.append(innodb_log)

        # 3. Commands for system monitoring-------------------------------------------------
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

        # 4. Commands for client monitoring--------------------------------------------------
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

        # 5. The dmx monitoring logs: barf --fr - every 10 seconds----------------------------
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

        # 6. The dmx monitoring logs: barf -a --ct algo - every 10 seconds--------------------
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

        # 9. The network traffic logs: sar, every 10 seconds-----------------------

        sar_log = os.path.join(self._log_dir, 'network_traffic.log')
        sar = "sar -n DEV 10 {cnt} " \
              "|grep -E `ip addr show | grep {ip} | awk '{{print $NF}}'` " \
              "&> {log_name}"
        sar_cmd = sar.format(cnt=int(self._duration / 10),
                             ip=self._client_ip,
                             log_name=sar_log)

        all_cmds.append(sar_cmd)
        curr_logs.append(sar_log)

        # 10. Shoot the commands out-----------------------------------------------
        # Allow 300 seconds more time to let the commands quit by themselves.
        self.run_client_cmd(all_cmds, self._duration + 180)
        self._sweep_logs.extend(curr_logs)
        return curr_logs

    def plot(self):
        """
        Plot the logs: sysbench, system monitor logs (iostat, mpstat, vmstat, etc)
        :return:
        """
        log.info('We are now plotting the sweep.')
        plot_files = ' '.join(self._sweep_logs)
        plot_cmd = './plotter.py ' \
                   '-p {prefix} ' \
                   '-b {buffer_size} ' \
                   '-r {redo_size} ' \
                   '{files}'.format(prefix=self._log_dir,
                                    buffer_size=self._buffer_bytes,
                                    redo_size=self._redo_bytes,
                                    files=plot_files)

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
        cmd_template = "./mailto.py " \
                       "{sender} " \
                       "{recipients} " \
                       "-S {smtp_server} " \
                       "-P {smtp_port} " \
                       "-s \"{subject}\" " \
                       "-a {attachment} " \
                       "-B \"{msg_body}\""
        subject_str = "Logs and graphs for sweep {}".format(self._log_dir)
        msg_body = 'Please see attached.'
        sendmail_cmd = [cmd_template.format(sender=self._mail_sender,
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
                                       universal_newlines=True)
                started = started.replace('\n', ' ')
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
            local_path = os.path.abspath(os.path.join(self._log_dir, local_file))

        cmd = 'scp -r {user}@{ip}:{frm} {to} '.format(user=self._login_user,
                                                      ip=self._db_ip,
                                                      frm=remote_abs_path,
                                                      to=local_path)
        self.run_client_cmd(cmd, timeout=120)

    def get_db_cnf_by_cmd(self, cmd, save_to):
        """Inner function to get database config from a specific command
        """
        save_to = os.path.join(self._log_dir, save_to)
        exit_status, result = self.run_db_cmd(cmd, suppress=True)

        if exit_status != 0:
            log.warning('Failed to get db config by cmd: {}'.format(cmd))
            return

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

        log.info('Sweep <{}> started, check logs in directory.'.format(self._log_dir))

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
            log.debug('Copying mysqld config file under bfapp.d and bfcs.d')
            self.copy_db_file('/dmx/etc/bfapp.d/mysqld', 'bfappd.mysqld')
            self.copy_db_file('/dmx/etc/bfcs.d/mysqld', 'bfcsd.mysqld')

        # Get the database server configurations and write to a log file
        log.debug('Fetching database h/w and driver information. ')
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
    parser.add_argument("-v", help="verbose ( -v: info, -vv: debug, -vvv: colored)",
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
    start_at = time.time()
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

    time_used = int(time.time() - start_at)
    log.info('The sweep is {} (time taken: {}s). Bye.'.format(status, time_used))
    sys.exit(0)
