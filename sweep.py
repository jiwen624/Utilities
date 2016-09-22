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
import socket
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
    # Global defaults
    ID_RSA_PATH = '~/.ssh/id_rsa'
    SSH_PORT = 22
    TARGETS = ('DMX', 'RAM')
    WORKLOADS = ('RO', 'RW', 'WO')
    DB_START_TIMEOUT = 1800
    LONG_POLL = 60
    MID_POLL = 10
    SHORT_POLL = 1
    DEFAULT_RND_TYPE = 'uniform'
    DEFAULT_USER = 'root'

    # default values of MySQL parameters
    MY_MAX_CONN = 151
    MY_LOGSIZE = 50331648
    MY_LOGS = 2
    MY_BP = 134217728

    def __init__(self, cnf_file):
        self._logs = []
        self._procs = []
        self._running_sb = 0
        # Paramiko Transport object
        self._trans = None
        # Record the number of failures of ssh connection
        self._trans_fails = 0
        # Fast-fail before sysbench is launched but try not to fail after it's running.
        self._sb_launched = False
        self._success = True

        cnf = ConfigParser()

        try:
            self._cnf_file = cnf_file
            if not cnf.read(cnf_file):
                raise NoSectionError('Failed to read cnf file {}'.format(cnf_file))

            log.debug('Sweep config file: {} loaded.'.format(cnf_file))

            # Section: server
            self._db_ip = cnf.get('server', 'db_ip')
            self._mysql_port = cnf.getint('server', 'db_port')
            self._client_ip = cnf.get('server', 'client_ip')
            self._script_dir = cnf.get('server', 'dbscript_path')
            self._sys_user = cnf.get('server', 'dbserver_user',
                                     fallback=self.DEFAULT_USER)

            # Section: benchmark
            cnf_name, _ = os.path.splitext(self._cnf_file)
            self._dir = '{}_{}'.format(cnf_name, time.strftime('%Y%m%d%H%M%S'))
            self._threads = cnf.getint('benchmark', 'sysbench_threads')
            self._db_num = cnf.getint('benchmark', 'db_num')
            self._target = cnf.get('benchmark', 'target')
            self._duration = cnf.getint('benchmark', 'duration')
            self._lua = cnf.get('benchmark', 'lua_script')
            self._tarball = cnf.get('benchmark', 'tarball_path')
            self._tar_strips = cnf.getint('benchmark', 'tar_strip_components',
                                          fallback=0)
            self._db_name = cnf.get('benchmark', 'db_name', fallback='sbtest')
            self._db_user = cnf.get('benchmark', 'db_user', fallback='sbtest')
            self._db_pwd = cnf.get('benchmark', 'db_pwd', fallback='sbtest')
            self._table_rows = cnf.get('benchmark', 'table_rows')
            self._table_num = cnf.get('benchmark', 'table_num')
            self._db_base = cnf.get('benchmark', 'mysql_base_dir',
                                    fallback='/var/lib/mysql')
            self._mysql_socket = cnf.get('benchmark', 'mysql_socket_file_prefix')
            self._dbstart_timeout = cnf.getint('benchmark', 'db_start_timeout',
                                               fallback=self.DB_START_TIMEOUT)

            # Section: poll_intervals
            self._sb_poll = cnf.getint('poll_intervals', 'sysbench',
                                       fallback=self.SHORT_POLL)
            self._innodb_poll = cnf.getint('poll_intervals', 'innodb',
                                           fallback=self.LONG_POLL)

            # Section: workload
            self._workload = cnf.get('workload', 'workload_type')
            self._read_only = (self._workload == 'RO')
            self._rand_t = cnf.get('workload', 'rand_type',
                                   fallback=self.DEFAULT_RND_TYPE)
            self._point_selects = cnf.getint('workload', 'oltp_point_selects')
            self._simple_ranges = cnf.getint('workload', 'oltp_simple_ranges')
            self._sum_ranges = cnf.getint('workload', 'oltp_sum_ranges')
            self._order_ranges = cnf.getint('workload', 'oltp_order_ranges')
            self._distinct_ranges = cnf.getint('workload', 'oltp_distinct_ranges')
            self._idx_updates = cnf.getint('workload', 'oltp_index_updates')
            self._nonidx_updates = cnf.getint('workload', 'oltp_non_index_updates')

            # Section: database
            self._db_params = dict(cnf.items('database'))
            size = self._tob(self.merge_dbcnf('innodb_log_file_size', self.MY_LOGSIZE))
            num = int(self.merge_dbcnf('innodb_log_files_in_group', self.MY_LOGS))
            self._log_size = size * num
            self._bp = self._tob(self.merge_dbcnf('innodb_buffer_pool_size', self.MY_BP))

            self._ta = cnf.getint('database', 'track_active')

            # Section: misc
            self._plot = cnf.getboolean('misc', 'plot', fallback=None)
            self._push = cnf.getboolean('misc', 'send_mail', fallback=None)

            self._mail_from = cnf.get('misc', 'mail_sender', fallback=None)
            self._mail_to = cnf.get('misc', 'mail_recipients', fallback=None)
            self._smtp_ip = cnf.get('misc', 'smtp_server', fallback=None)
            self._smtp_port = cnf.getint('misc', 'smtp_port', fallback=None)

            self._ssd = cnf.get('misc', 'ssd_device')
            self._skip_db_recreation = cnf.getboolean('misc', 'skip_db_recreation')
            self._verify_cnf = cnf.getboolean('misc', 'check_config', fallback=True)

            # Check if there are any logical errors in the configurations.
            if self._verify_cnf:
                self._pre_check()

        except (NoSectionError, NoOptionError, TypeError, KeyError, ValueError) as e:
            log.error('Invalid or missing file/option: ({})'.format(cnf_file))
            _, _, exc_tb = sys.exc_info()
            log.error('Line {}:{}'.format(exc_tb.tb_lineno, e))
            self._success = False
            return

    def merge_dbcnf(self, var, default):
        """
        This function get the value of a specific MySQL config, either from
        /etc/my.cnf.baseline or from the sweep config file. The latter will
        override the former one.
        Only global options are supported.
        :param default:
        :type var: string
        :return: a string represent the value of this parameter.
        """
        default = str(default)
        sweep_param = 'mysql_' + var
        # Try to get it from sweep config file.
        value = self._db_params.get(sweep_param)

        # Then get it from my.cnf if the variable is not specified in sweep config.
        # The values from sweep config file surpass those from my.cnf
        # sweep.cnf > my.cnf > default value
        if value is None:
            cmd = "grep {v} /etc/my.cnf.default|awk -F= '{{print $NF}}'".format(v=var)
            exit_status, value = self.db_cmd(cmd, suppress=True)
            if exit_status == 0:
                value = value.rstrip('\n')
            else:
                value = None
        # If it's still None, return the default value.
        return value if value else default

    def _pre_check(self):
        """
        This function does some basic sanity check to eliminate some configuration errors.
        If anything is error, it will raise ValueError (may not be an appropriate exception)

        We need to do pre-check to eliminate config errors as the following steps are
        time-consuming.
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
        # If target is one of DMX/RAM
        if self._target not in self.TARGETS:
            raise ValueError('Target: {}. Supported: {}.'.format(self._target,
                                                                 self.TARGETS))
        # These variables should be a number larger than 0
        if self._db_num <= 0 or self._duration <= 0:
            raise ValueError('db_num:{} duration:{}'.format(self._db_num,
                                                            self._duration))

        # Track active should be between 0 and 100
        if not (0 <= self._ta <= 100):
            raise ValueError('Track_active (0-100): {}'.format(self._ta))

        # Track active should be 0 if the target is RAMs
        if self._target == 'RAM' and self._ta != 0:
            raise ValueError('Track_active for RAM: {}'.format(self._ta))

        # The email items should be valid if you want to send an email.
        if self._push:
            if not (self._mail_to
                    and self._mail_from
                    and self._smtp_port
                    and self._smtp_ip
                    ):
                raise ValueError('send_mail is ON but missing mandatory option.')
            # Basic email address check.
            if not ('@' in self._mail_to
                    and '@' in self._mail_from
                    ):
                raise ValueError('Invalid email address in *mail_recipients*')

    def _check_sb_threads(self):
        """
        Sanity check: if the specified sysbench threads count is supported by current
        MySQL configuration
        :return:
        """
        db_max_cnt = int(self.merge_dbcnf('max_connections', self.MY_MAX_CONN))

        if self._threads <= 0:
            raise ValueError('Invalid thread count: {}'.format(self._threads))

        if self._threads >= db_max_cnt:
            raise ValueError('Too many sysbench threads: {}, '
                             'max_connections@my.cnf is only {}'.format(self._threads,
                                                                        db_max_cnt))

    def _check_buffer_pool(self):
        """
        This functions checks if the buffer_pool size is larger than the RAM size.
        :return:
        """
        if self._target == 'DMX':
            return
        else:
            bp = str(self._bp)
            cmd = "cat /proc/meminfo |grep MemTotal | awk '{print $2}'"
            ret, ram = self.db_cmd(cmd, suppress=True)
            if ret == 0:
                ram = ram.rstrip('\n') + 'K'
            else:
                ram = 0
            if self._tob(bp) >= self._tob(ram):
                raise ValueError('Bad buffer pool:{}, RAM:{}.'.format(bp, ram))

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
        self.teardown_procs()
        self.close_db_conn()

    @property
    def success(self):
        return self._success

    @property
    def running_procs(self):
        return self._procs

    @property
    def sb_threads(self):
        return self._threads

    @property
    def log_dir(self):
        return self._dir

    @staticmethod
    def _tob(size_str):
        """
        This function converts a size with unit in string into a number
        :param size_str:   '32G', '16800M', etc.
        :return: a int number represents the bytes
        """
        matrix = {'K': 1024, 'M': 1024 ** 2, 'G': 1024 ** 3, 'T': 1024 ** 4}
        # It may raise IndexError here if the size_str is None. However, let's
        # just raise this error and quit the program as soon as possible, as I
        # have no idea how to continue with such an invalid value.
        unit = size_str[-1].upper()
        if unit == 'B':
            size_str = size_str[:-1]
            unit = size_str[-1].upper()

        if unit not in matrix.keys():
            # Ask for forgiveness, not permission.
            # Returns the number if it can be converted to a number. e,g. '123'
            # Otherwise just raise ValueError. e,g, '123abc'
            return int(size_str)
        size = int(size_str[:-1])
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
        exit_status = -1
        result = ''

        if not suppress:
            log.debug('[db] {}'.format(cmd))
        # Reuse the Transport object if there is already there.
        if self._trans is None:
            self._trans = paramiko.Transport(self._db_ip, self.SSH_PORT)

        # Reconnect to remote server if the connection is inactive.
        try:
            if not self._trans.is_active():
                key_path = os.path.expanduser(self.ID_RSA_PATH)
                key = paramiko.RSAKey.from_private_key_file(key_path)
                # The default banner timeout in paramiko is 15 sec
                self._trans.connect(username=self._sys_user, pkey=key)

            # Each command needs a separate session
            session = self._trans.open_channel("session", timeout=60)
        except (socket.error,
                socket.timeout,
                paramiko.SSHException,
                EOFError,
                RuntimeError) as e:
            self._trans_fails += 1
            # Quit the sweep if 1. ssh fails twice. and 2. sysbench has not started.
            # If the sysbench has started we will ignore the ssh error and let it run.
            if self._trans_fails >= 2 and not self._sb_launched:
                log.error('socket error ({}): ({})'.format(self._trans_fails, e))
                raise SweepFatalError

            return exit_status, result

        # session.get_pty() -- Do I need this?
        session.exec_command(cmd)

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
        self._trans_fails = 0
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

    def db_cmd(self, cmd, suppress=False):
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
            time.sleep(0.2)

        self._procs.extend(running_procs)
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
                            self._procs.remove(proc)
                            p.unregister(proc.stdout)
                        except ValueError:
                            pass

                        _, errors_raw = proc.communicate()
                        errors = errors_raw.rstrip()

                        if ret != 0:  # Process failed.
                            log.warning('Command failed: {}'.format(proc.args))
                            log.warning('(errs={})'.format(errors))
                            # watcher.cancel()

                            # Check if sysbench is failed and do fast-fail if so.
                            # As sysbench failure is a critical error.
                            if 'sysbench' in proc.args:
                                log.error('Fatal error found in sysbench.')

                                # Clean the running process list to quit the loop,
                                # as all the processes have been killed in self.close()
                                self._running_sb -= 1
                                self._success = False
                                _timeout = 0
                            else:  # Just ignore failures from the other commands
                                #  self._running_procs.remove(proc)
                                pass
                        else:
                            log.info('Done: (cmd={})'.format(proc.args))
                            if 'sysbench' in proc.args:
                                log.info('Sysbench done ({}s).'.format(int(time.time() - start)))
                                self._running_sb -= 1
                                # All the sysbench processes have finished.
                                if self._running_sb == 0:
                                    self._success = True
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
                self._procs.remove(proc)
            except (ProcessLookupError, BrokenPipeError, ValueError) as e:
                print('Error in killing ({}) ({})'.format(proc.pid, e))

    def copy_mysql_err_logs(self):
        """
        This function copies MySQL logs to sweep directory when sysbench is failed.
        :return:
        """
        exit_status, hostname = self.db_cmd('hostname', suppress=True)
        if exit_status == 0:
            hostname = hostname.rstrip('\n')
            for idx in range(1, self._db_num + 1):
                mysql_path = os.path.join(self._db_base, 'mysql{idx}'.format(idx=idx))
                mysql_errlog = '{hostname}.err'.format(hostname=hostname)
                m_log_path = os.path.join(mysql_path, mysql_errlog)
                local_log = 'mysql{idx}_{name}'.format(idx=idx, name=mysql_errlog)
                log.debug('Copying {} from db to local: {}'.format(m_log_path, local_log))
                self.copy_db_file(m_log_path, local_log)

    def teardown_procs(self):
        """
        Kill all the running processes. For now this function is only called
        when the Sweep object is closing.
        :return:
        """
        try:
            procs = self._procs
            self._procs = []
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
        # However it will be ignored if there has been some error in the SSH connection.
        # Ignore this step if the sysbench has not been started yet.
        if self._sb_launched:
            if self._trans_fails == 0:
                # We must kill these two commands otherwise they may run forever.
                self.db_cmd('killall tdctl monitor')
            else:
                log.warning('Ignored killing tdctl and monitor on remote server. ')

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
        log.info('Running database clean-up program.')
        cleanup_script = os.path.join(self._script_dir, 'cleandb.py')

        skip_db_recreation = '-o skip_db_recreation' if self._skip_db_recreation else ''
        staging_dir = '/tmp/{}'.format(self._dir)
        params = ' '.join(['{}={}'.format(k, v) for k, v in self._db_params.items()])
        cmd_template = '{cleanup_script} {db_num} {skip_db_recreation} -n {staging_dir} ' \
                       '-d {base_dir} -z {tarball} -t {timeout} -s {strips} -v -p "{params}" ' \
                       '2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_num=self._db_num,
                                           skip_db_recreation=skip_db_recreation,
                                           staging_dir=staging_dir,
                                           base_dir=self._db_base,
                                           tarball=self._tarball,
                                           timeout=self._dbstart_timeout,
                                           strips=self._tar_strips,
                                           params=params,
                                           log_path=self._script_dir)

        exit_status, result = self.db_cmd(clean_db_cmd)
        # log.debug('clean_db exit status: {}'.format(exit_status))

        # The cleanup.py from the database server is failed.
        if not self.db_ready(exit_status):
            log.error('Database cleanup failed, err_code: {}'.format(exit_status))
            self._success = False
            raise SweepFatalError
        log.info('Done: Database is ready.')

    @staticmethod
    def db_ready(ret_code):
        """
        Check if the database cleanup script has returned successfully.
        :param ret_code:
        :return:
        """
        log.debug('Database cleanup script returns: {}'.format(ret_code))
        return ret_code == 0

    @staticmethod
    def kill_procs_by_name(proc_names, skip_pid=0):
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
                        log.debug('Kill:({}) {}'.format(proc.pid, ' '.join(proc.cmdline())))
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
        # Clean previously launched sweep script
        _, self_name = os.path.split(__file__)
        proc_names = ["iostat",
                      "mpstat",
                      "vmstat",
                      "tdctl",
                      "sysbench",
                      "mysql",
                      self_name]

        self.kill_procs_by_name(proc_names, self_pid)

        time.sleep(5)
        log.info('Done: Client is ready.')

    def run_once(self, thread_cnt):
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
        log.info('Go to {ip}:/tmp/{dir} for db side logs.'.format(ip=self._db_ip,
                                                                  dir=self._dir))

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
                       '--rand-init={rand_init} {rnd_type} ' \
                       'run > {log_name}'

        rand_init = 'off' if self._rand_t == 'off' else 'on'
        rnd_type = '' if self._rand_t == 'off' else '--rand-type={}'.format(self._rand_t)
        oltp_read_only = 'on' if self._read_only else 'off'

        for port in range(self._mysql_port, self._mysql_port + self._db_num):
            db_idx = port - self._mysql_port + 1
            # For each instance, record sysbench logs and innodb status logs, etc.
            # 1. the sysbench logs:
            sb_log_name = 'sb_{}_{}_db{}.log'.format(self._target,
                                                     thread_cnt,
                                                     db_idx)
            sb_log = os.path.join(self._dir, sb_log_name)

            sb_cmd = cmd_template.format(lua_script=self._lua,
                                         oltp_table_size=self._table_rows,
                                         oltp_tables_count=self._table_num,
                                         mysql_host=self._db_ip,
                                         mysql_port=port,
                                         db_name=self._db_name,
                                         db_user=self._db_user,
                                         db_pwd=self._db_pwd,
                                         thread_num=thread_cnt,
                                         max_time=self._duration,
                                         sysbench_poll_interval=self._sb_poll,
                                         oltp_read_only=oltp_read_only,
                                         oltp_point_selects=self._point_selects,
                                         oltp_simple_ranges=self._simple_ranges,
                                         oltp_sum_ranges=self._sum_ranges,
                                         oltp_order_ranges=self._order_ranges,
                                         oltp_distinct_ranges=self._distinct_ranges,
                                         oltp_index_updates=self._idx_updates,
                                         oltp_non_index_updates=self._nonidx_updates,
                                         rand_init=rand_init,
                                         rnd_type=rnd_type,
                                         log_name=sb_log)
            all_cmds.append(sb_cmd)
            curr_logs.append(sb_log)

            # 2. The innodb status logs - every 60 seconds------------------------------
            innodb_log_name = 'innodb_status_db{}.log'.format(db_idx)
            innodb_log = os.path.join(self._dir, innodb_log_name)
            innodb_cmd = "while true; " \
                         "do " \
                         "ssh {user}@{ip} \"mysql -S {socket_prefix}{db_idx} -e " \
                         "'show engine innodb status\G' | grep -A 28 -E 'LOG|END' " \
                         "&>> /tmp/{log_name}\"; " \
                         "  sleep {innodb_poll_interval}; " \
                         "done".format(user=self._sys_user,
                                       ip=self._db_ip,
                                       socket_prefix=self._mysql_socket,
                                       db_idx=db_idx,
                                       log_dir=self._dir,
                                       log_name=innodb_log,
                                       innodb_poll_interval=self._innodb_poll)

            all_cmds.append(innodb_cmd)
            curr_logs.append(innodb_log)

        # 3. Commands for system monitoring-------------------------------------------------
        os_cmds = ('iostat -dmx {} -y'.format(self._ssd),
                   'mpstat',
                   'vmstat -S M -w',
                   'tdctl -v --dp +')
        for cmd in os_cmds:
            sys_log_name = '{}_{}_{}.log'.format(cmd.split()[0], self._target, thread_cnt)
            sys_log = os.path.join(self._dir, sys_log_name)
            count = '' if 'tdctl' in cmd else int(self._duration / 10)
            sys_cmd = 'ssh {user}@{ip} "{cmd} 10 {count} ' \
                      '&> /tmp/{log_name}"'.format(user=self._sys_user,
                                                   ip=self._db_ip,
                                                   cmd=cmd,
                                                   count=count,
                                                   log_dir=self._dir,
                                                   log_name=sys_log)
            all_cmds.append(sys_cmd)
            curr_logs.append(sys_log)

        # 4. Commands for client monitoring--------------------------------------------------
        client_cmds = ('vmstat -S M -w',)
        for cmd in client_cmds:
            client_log_name = '{}_{}_{}_client.log'.format(cmd.split()[0],
                                                           self._target, thread_cnt)
            client_log = os.path.join(self._dir, client_log_name)
            count = int(self._duration / 10)
            full_client_cmd = '{cmd} 10 {count} &> {log}'.format(cmd=cmd,
                                                                 count=count,
                                                                 log=client_log)
            all_cmds.append(full_client_cmd)
            curr_logs.append(client_log)

        # 5. The dmx monitoring logs: barf --fr - every 10 seconds----------------------------
        barf_fr_log = os.path.join(self._dir, 'barffr_.log')
        barf_fr_cmd = "while true; " \
                      "do " \
                      "ssh {user}@{ip} 'barf --fr &>> /tmp/{log_name}'; " \
                      "sleep 10; " \
                      "done".format(user=self._sys_user,
                                    ip=self._db_ip,
                                    log_dir=self._dir,
                                    log_name=barf_fr_log)

        all_cmds.append(barf_fr_cmd)
        curr_logs.append(barf_fr_log)

        # 6. The dmx monitoring logs: barf -a --ct algo - every 10 seconds--------------------
        barf_act_algo_log = os.path.join(self._dir, 'barf_a_ct_algo.log')
        barf_act_algo_cmd = "while true; " \
                            "do " \
                            "ssh {user}@{ip} 'barf -a --ct algo &>> /tmp/{log_name}'; " \
                            "sleep 10; " \
                            "done".format(user=self._sys_user,
                                          ip=self._db_ip,
                                          log_dir=self._dir,
                                          log_name=barf_act_algo_log)

        all_cmds.append(barf_act_algo_cmd)
        curr_logs.append(barf_act_algo_log)

        # 7. The dmx monitoring logs: barf -a --ct bf - every 10 seconds---------------------
        barf_act_bf_log = os.path.join(self._dir, 'barf_a_ct_bf.log')
        barf_act_bf_cmd = "while true; " \
                          "do " \
                          "ssh {user}@{ip} 'barf -a --ct bf &>> /tmp/{log_name}'; " \
                          "sleep 10; " \
                          "done".format(user=self._sys_user,
                                        ip=self._db_ip,
                                        log_dir=self._dir,
                                        log_name=barf_act_bf_log)

        all_cmds.append(barf_act_bf_cmd)
        curr_logs.append(barf_act_bf_log)

        # 8. The dmx monitoring logs: monitor, every 10 seconds------------------------------
        pids = self.get_database_pid()
        for idx, pid in enumerate(pids, 1):
            if not pid:
                continue
            monitor_log = os.path.join(self._dir, 'monitor_p_db{}.log'.format(idx))
            monitor = 'monitor -p {pid} -D 10'.format(pid=pid)
            monitor_cmd = 'ssh {user}@{ip} ' \
                          '"{cmd} &> /tmp/{log}"'.format(user=self._sys_user,
                                                         ip=self._db_ip,
                                                         cmd=monitor,
                                                         log_dir=self._dir,
                                                         log=monitor_log)
            all_cmds.append(monitor_cmd)
            curr_logs.append(monitor_log)

        # 9. The network traffic logs: sar, every 10 seconds-----------------------

        sar_log = os.path.join(self._dir, 'network_traffic.log')
        sar = "sar -n DEV 10 {cnt} " \
              "|grep -E `ip addr show | grep {ip} | awk '{{print $NF}}'` " \
              "&> {log_name}"
        sar_cmd = sar.format(cnt=int(self._duration / 10),
                             ip=self._client_ip,
                             log_name=sar_log)

        all_cmds.append(sar_cmd)
        curr_logs.append(sar_log)

        # 10. Shoot the commands out-----------------------------------------------
        self._running_sb = self._db_num
        self._sb_launched = True
        # Allow 180 seconds more time to let the commands quit by themselves.
        self.run_client_cmd(all_cmds, self._duration + 180)
        self._logs.extend(curr_logs)
        return curr_logs

    def plot(self):
        """
        Plot the logs: sysbench, system monitor logs (iostat, mpstat, vmstat, etc)
        :return:
        """
        log.info('We are now plotting the sweep.')
        plot_files = ' '.join(self._logs)
        plot_cmd = './plotter.py ' \
                   '-p {prefix} ' \
                   '-b {buffer_size} ' \
                   '-r {redo_size} ' \
                   '{files}'.format(prefix=self._dir,
                                    buffer_size=self._bp,
                                    redo_size=self._log_size,
                                    files=plot_files)

        # The timeout of plot is 600 seconds, it will be killed if
        # not return before timeout
        self.run_client_cmd(plot_cmd, 600)

    def _compress(self):
        """
        Compress the raw logs and graphs.
        :return:
        """
        zip_file = '{}.zip'.format(self._dir)
        with zipfile.ZipFile(zip_file, "w", zipfile.ZIP_DEFLATED) as zipped:
            for fname in os.listdir(self._dir):
                absname = os.path.join(self._dir, fname)
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
        subject_str = "Logs and graphs for sweep {}".format(self._dir)
        msg_body = 'Please see attached.'
        sendmail_cmd = [cmd_template.format(sender=self._mail_from,
                                            recipients=self._mail_to,
                                            smtp_server=self._smtp_ip,
                                            smtp_port=self._smtp_port,
                                            subject=subject_str,
                                            attachment=attachment,
                                            msg_body=msg_body)]
        self.run_client_cmd(sendmail_cmd, timeout=600)

    def post_check(self, curr_logs):
        """
        Check if the benchmark has been done successfully and raise an RuntimeError
        if some error happens. It's considered good if the sb_*.log (sysbench logs)
        contains 'execution time' in the tail.
        Set the parameter self._success accordingly.
        :param curr_logs:
        :return: True of False
        """
        if not self._success:
            return False

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
                # A simple hard-coded check to the sysbench logs
                if 'execution time' not in started:
                    log.warning('Found error in {}.'.format(file))
                    self._success = False
                    return False
        return True

    def copy_db_file(self, remote, local):
        """
        Inner function to cat db files from remote server
        :param remote: remote path
        :param local: local path
        :return:
        """
        if not os.path.isabs(local):
            local = os.path.abspath(os.path.join(self._dir, local))
        if not os.path.isabs(remote):
            remote = os.path.abspath(os.path.join(self._dir, remote))

        cmd = 'scp -r {user}@{ip}:{remote} {local} '.format(user=self._sys_user,
                                                            ip=self._db_ip,
                                                            remote=remote,
                                                            local=local)
        self.run_client_cmd(cmd, timeout=120)

    def get_db_cnf_by_cmd(self, cmd, save_to):
        """Inner function to get database config from a specific command
        """
        save_to = os.path.join(self._dir, save_to)
        exit_status, result = self.db_cmd(cmd, suppress=True)

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
        exit_status, pids = self.db_cmd(cmd, suppress=True)
        if exit_status == 0:
            pids = pids.split('\n')
        else:
            pids = None
        return pids

    def start(self):
        """
        Start the sweep. The entry point of the benchmark(s).
        :return:
        """
        if not self._success:
            log.warning('Sweep has already failed. Aborting it.')
            return

        try:
            os.mkdir(self._dir)
            # Copy the sweep config file to the log directory.
            cnf_dest = os.path.join(self._dir, self._cnf_file)
            shutil.copy2(self._cnf_file, cnf_dest)
        except FileExistsError:
            pass
        except FileNotFoundError as e:
            log.warning('Sweep config file is gone now! {}'.format(e))

        log.info('Sweep <{}> started, check logs in directory.'.format(self._dir))

        self.clean_client()
        self.clean_db()
        # Run the benchmark and check results, self._success will be set in it.
        self.post_check(self.run_once(self._threads))
        log.info('Benchmark for {} threads has stopped.'.format(self._threads))

        # Copy server config files
        self.copy_db_file('/etc/my.cnf', 'my.cnf')

        # Copy remote logs from staging area
        staging_logs = '/tmp/{}/*'.format(self._dir)
        self.copy_db_file(staging_logs, './')

        if self._target == 'DMX':
            log.debug('Copying mysqld config file under bfapp.d and bfcs.d')
            self.copy_db_file('/dmx/etc/bfapp.d/mysqld', 'bfappd.mysqld')
            self.copy_db_file('/dmx/etc/bfcs.d/mysqld', 'bfcsd.mysqld')
            self.copy_db_file('/dmx/etc/config', 'dmx_etc_config')

        # Get the database server configurations and write to a log file
        log.debug('Fetching database h/w and driver information. ')
        self.get_db_cnf_by_cmd('barf --dv', 'barf.out')
        self.get_db_cnf_by_cmd('barf -v -l', 'barf.out')
        self.get_db_cnf_by_cmd('free', 'server_os_info.out')

        lscpu = "lscpu | grep -Ev 'Architecture|Order|cache|[F|f]amily|Vendor" \
                "|Stepping|op-mode|Model:|node[0-9]|MIPS'"
        self.get_db_cnf_by_cmd(lscpu, 'server_os_info.out')

        # Plot the logs after sweep.
        if self._plot and self._success:
            self.plot()

        if self._push:
            self.send_mail()

        # Change the .cnf file to .done if it's successful.
        if self._success:
            try:
                pure_fname, _ = os.path.splitext(self._cnf_file)
                os.rename(self._cnf_file, pure_fname + '.done')
            except (OSError, FileExistsError) as e:
                log.warning('Failed to rename the config file: {}'.format(e))
        else:
            # Copy MySQL logs from db server for further diagnosis.
            self.copy_mysql_err_logs()
            # Rename the log directory with a prefix 'failed_'
            try:
                os.rename(self._dir, self._dir + '_FAILED')
                log.info('Marked the log directory with suffix _FAILED.')
                self._dir += '_FAILED'
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

        coloredlogs.install(level=log_level, fmt='%(asctime)s: %(message)s')

    logging.basicConfig(level=log_level,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m-%d %H:%M:%S')
    # I don't want to see paramiko debug logs, unless they are WARNING or worse
    # than that.
    logging.getLogger("paramiko").setLevel(logging.WARNING)

    log.info('-----------------------------------------------------------')
    log.info('------New sweep config file found, preparing to start------')
    log.info('-----------------------------------------------------------')

    status = 'unknown'
    start_at = time.time()
    with Sweep(args.config) as sweep:
        try:
            sweep.start()
            status = 'finished' if sweep.success else 'failed'

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

    elapsed = int(time.time() - start_at)
    log.info('The sweep is {} (time taken: {}s). Bye.'.format(status, elapsed))
    sys.exit(0)
