#!/usr/bin/env python3
""" This sweep program is used to run MySQL benchmarks with various configurations
    in client/server. Run this command on the client.
    External dependencies: plotter.py, cleandb.py
    ...
"""
import os
import time
import re
import sys
import logging
import psutil
import signal
import argparse
import paramiko
import zipfile
import shutil
import shlex
import socket
import itertools
import random
import contextlib
from subprocess import Popen, check_output, PIPE
from configparser import ConfigParser, NoSectionError, NoOptionError
from select import epoll, POLLIN, POLLERR, POLLHUP
from collections import OrderedDict

log = logging.getLogger('')


class SweepError(Exception):
    """Base exception class"""
    pass


class SweepConfigError(SweepError):
    """Config file base exception"""
    pass


class SweepFatalError(Exception):
    """Fatal error which causes the sweep cannot be resumed.
    """
    pass


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
    _ID_RSA_PATH = '~/.ssh/id_rsa'
    _SSH_PORT = 22
    _TARGETS = ('DMX', 'RAM')
    _WORKLOADS = ('RO', 'RW', 'WO')
    _DB_START_TIMEOUT = 1800
    _LONG_POLL = 60
    _LARGE_POLL = 30
    _MID_POLL = 10
    _SHORT_POLL = 1
    _DEFAULT_RND_TYPE = 'uniform'
    _DEFAULT_USER = 'root'
    _MAX_TRANS_FAILS = 2

    # default values of MySQL parameters
    _MY_MAX_CONN = 151
    _MY_LOGSIZE = 50331648
    _MY_LOGS = 2
    _MY_BP = 134217728
    # When for some reason the tps keeps 0, sysbench may run more time than expected.
    # So an adjustment is made to wait a bit longer.
    # Update: This is deprecated. I does not use it anymore.
    # _DURATION_ADJUSTMENT = 1.05
    _PLOT_TIMEOUT = 600
    _SSH_BURST_INTERVAL = 0.2
    _REMOTE_OUT_CHK_INTERVAL = 0.01

    def __init__(self, cnf_file):
        self._logs = []
        self._procs = {}
        self._running_sb = 0
        # Paramiko Transport object
        self._trans = None
        # Record the number of failures of ssh connection
        self._trans_fails = 0
        # Fast-fail before sysbench is launched but try not to fail after it's running.
        self._sb_launched = False
        self._success = True
        self._active_db_pool = {}  # The format of the elements: port: subprocess.POpen struct

        if not isinstance(cnf_file, str):
            raise SweepConfigError('cnf_file should be a string.')

        cnf = ConfigParser()

        try:
            self._cnf_file = cnf_file
            # Files cannot be read will be silently ignored, not exception is raised.
            if not cnf.read(cnf_file):
                raise SweepConfigError('Failed to read {}'.format(cnf_file))

            log.info('Sweep config file: {} loaded.'.format(cnf_file))

            # Section: server
            self._db_ip = cnf.get('server', 'db_ip')
            self._db_port = cnf.getint('server', 'db_port')
            self._client_ip = cnf.get('server', 'client_ip')
            self._script_dir = cnf.get('server', 'dbscript_path')
            self._sys_user = cnf.get('server', 'dbserver_user',
                                     fallback=self._DEFAULT_USER)

            # Section: benchmark
            cnf_name, _ = os.path.splitext(self._cnf_file)
            self._dir = '{}_{}'.format(cnf_name, time.strftime('%Y%m%d%H%M%S'))
            self._threads = cnf.getint('benchmark', 'sysbench_threads')
            self._db_num = cnf.getint('benchmark', 'db_num')
            # This _db_port_pool is a set which contains only the port numbers. However,
            # The corresponding active_db_pool is a dictionary contains both ports and
            # process objects. This is because the program needs to check if the 'active'
            # database is really active by checking the status of the sysbench process
            # attaching to it.
            self._db_port_pool = set(range(self._db_port, self._db_port + self._db_num))

            self._target = cnf.get('benchmark', 'target')
            self._duration = cnf.getint('benchmark', 'duration')
            self._lua = cnf.get('benchmark', 'lua_script')
            self._tarball = cnf.get('benchmark', 'tarball_path')
            self._tar_strips = cnf.getint('benchmark', 'tar_strip_components',
                                          fallback=0)
            self._db_name = cnf.get('benchmark', 'db_name', fallback='sbtest')
            self._db_user = cnf.get('benchmark', 'db_user', fallback='sbtest')
            self._db_pwd = cnf.get('benchmark', 'db_pwd', fallback='sbtest')
            self._table_rows = cnf.getint('benchmark', 'table_rows')
            self._table_num = cnf.getint('benchmark', 'table_num')
            self._db_base = cnf.get('benchmark', 'mysql_base_dir',
                                    fallback='/var/lib/mysql')
            try:
                self._mysql_sock = cnf.get('benchmark', 'sock_prefix')
            except NoOptionError:
                self._mysql_sock = cnf.get('benchmark', 'mysql_socket_file_prefix',
                                           fallback='/tmp/mysql.sock')

            self._db_start_timeout = cnf.getint('benchmark', 'db_start_timeout',
                                                fallback=self._DB_START_TIMEOUT)
            self._warmup_time = cnf.getint('benchmark', 'warmup_time', fallback=0)
            self._fast_mode = cnf.getboolean('benchmark', 'fast_mode', fallback=False)

            # Section: poll_intervals
            self._sb_poll = cnf.getint('poll_intervals', 'sysbench',
                                       fallback=self._SHORT_POLL)
            self._innodb_poll = cnf.getint('poll_intervals', 'innodb',
                                           fallback=self._LONG_POLL)
            self._iostat_poll = cnf.getint('poll_intervals', 'iostat',
                                           fallback=self._MID_POLL)
            self._vmstat_poll = cnf.getint('poll_intervals', 'vmstat',
                                           fallback=self._MID_POLL)
            self._mpstat_poll = cnf.getint('poll_intervals', 'mpstat',
                                           fallback=self._MID_POLL)
            self._tdctl_poll = cnf.getint('poll_intervals', 'tdctl',
                                          fallback=self._MID_POLL)
            self._network_poll = cnf.getint('poll_intervals', 'network',
                                            fallback=self._MID_POLL)
            self._monitor_poll = cnf.getint('poll_intervals', 'monitor',
                                            fallback=self._MID_POLL)
            self._barf_act_bf_poll = cnf.getint('poll_intervals', 'barf_act_bf_poll',
                                                fallback=self._MID_POLL)
            self._barf_act_algo_poll = cnf.getint('poll_intervals', 'barf_act_algo_poll',
                                                  fallback=self._MID_POLL)
            self._barf_fr_poll = cnf.getint('poll_intervals', 'barf_fr_poll',
                                            fallback=self._LARGE_POLL)

            # Section: workload
            self._workload = cnf.get('workload', 'workload_type')
            self._read_only = (self._workload == 'RO')
            self._rand_t = cnf.get('workload', 'rand_type',
                                   fallback=self._DEFAULT_RND_TYPE)

            if self._rand_t == 'off':
                self.rand_init = 'off'
                self.rnd_type = ''
            # All other strings except off will be treated as a valid random type.
            else:
                self.rand_init = 'on'
                self.rnd_type = '--rand-type={}'.format(self._rand_t)
            self.oltp_read_only = 'on' if self._read_only else 'off'

            self._point_selects = cnf.getint('workload', 'oltp_point_selects')
            self._simple_ranges = cnf.getint('workload', 'oltp_simple_ranges')
            self._sum_ranges = cnf.getint('workload', 'oltp_sum_ranges')
            self._order_ranges = cnf.getint('workload', 'oltp_order_ranges')
            self._distinct_ranges = cnf.getint('workload', 'oltp_distinct_ranges')
            self._idx_updates = cnf.getint('workload', 'oltp_index_updates')
            self._nonidx_updates = cnf.getint('workload', 'oltp_non_index_updates')

            self._active_ratio = cnf.getint('workload', 'db_active_pct', fallback=100)
            self._toggle_time = cnf.getint('workload', 'db_toggle_time', fallback=0)
            self._toggle_pct = cnf.getint('workload', 'db_toggle_pct', fallback=0)

            # Section: database
            self._db_params = OrderedDict(cnf.items('database'))
            size = self._tob(self.merge_dbcnf('innodb_log_file_size', self._MY_LOGSIZE))
            num = int(self.merge_dbcnf('innodb_log_files_in_group', self._MY_LOGS))
            self._log_size = size * num
            self._bp = self._tob(self.merge_dbcnf('innodb_buffer_pool_size', self._MY_BP))

            self._ta = cnf.getint('database', 'track_active', fallback=80)

            # Section: misc
            self._plot = cnf.getboolean('misc', 'plot', fallback=None)
            self._push = cnf.getboolean('misc', 'send_mail', fallback=None)

            self._mail_from = cnf.get('misc', 'mail_sender', fallback=None)
            self._mail_to = cnf.get('misc', 'mail_recipients', fallback=None)
            self._smtp_ip = cnf.get('misc', 'smtp_server', fallback=None)
            self._smtp_port = cnf.getint('misc', 'smtp_port', fallback=None)

            self._ssd = cnf.get('misc', 'ssd_device')
            self._skip_db_recreation = cnf.getboolean('misc', 'skip_db_recreation')
            self._chk_cnf = cnf.getboolean('misc', 'check_config', fallback=True)
            self._change_cnf_ext = cnf.getboolean('misc', 'change_cnf_ext', fallback=False)

            # for epoll:
            self.p = None
            self.stdout_dict = {}
            self.running_p = []

            # for toggle
            self.toggle_base_time = 0

            # Check if there are any logical errors in the configurations.
            if self._chk_cnf:
                self._pre_chk()

        except (NoSectionError,
                NoOptionError,
                TypeError,
                KeyError,
                ValueError) as e:
            # log.error('Invalid or missing file/option: ({})'.format(cnf_file))
            # _, _, exc_tb = sys.exc_info()
            # log.error('Line {}:{}'.format(exc_tb.tb_lineno, e))
            self._success = False
            raise SweepConfigError from e

    def reset_toggle_base_time(self):
        """Reset toggle base time"""
        if hasattr(self, 'warmup') and self.warmup:
            return None
        self.toggle_base_time = time.time()
        return self.toggle_base_time

    def need_warmup(self):
        """Check if warm-up is needed"""
        return not (self._warmup_time == 0)

    def sb_template(self, mysql_port, run_time=None):
        """Create sysbench command string"""
        duration = self._duration if run_time is None else run_time

        return 'sysbench ' \
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
               'run'.format(lua_script=self._lua,
                            oltp_table_size=self._table_rows,
                            oltp_tables_count=self._table_num,
                            mysql_host=self._db_ip,
                            mysql_port=mysql_port,
                            db_name=self._db_name,
                            db_user=self._db_user,
                            db_pwd=self._db_pwd,
                            thread_num=self._threads,
                            max_time=duration,
                            sysbench_poll_interval=self._sb_poll,
                            oltp_read_only=self.oltp_read_only,
                            oltp_point_selects=self._point_selects,
                            oltp_simple_ranges=self._simple_ranges,
                            oltp_sum_ranges=self._sum_ranges,
                            oltp_order_ranges=self._order_ranges,
                            oltp_distinct_ranges=self._distinct_ranges,
                            oltp_index_updates=self._idx_updates,
                            oltp_non_index_updates=self._nonidx_updates,
                            rand_init=self.rand_init,
                            rnd_type=self.rnd_type)

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
            cmd = "grep {v} /etc/my.cnf.default|awk -F='{{print $NF}}'".format(v=var)
            exit_status, value = self.db_cmd(cmd, suppress=True)
            if exit_status == 0:
                value = value.rstrip('\n')
            else:
                value = None
        # If it's still None, return the default value.
        return value if value else default

    def _pre_chk(self):
        """
        This function does some basic sanity check to eliminate some
        configuration errors. If anything is error, it will raise
        ValueError (may not be an appropriate exception)

        We need to do pre-check to eliminate config errors as the
        following steps are time-consuming.
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
        if self._active_ratio + self._toggle_pct > 100:
            raise ValueError('The active+toggle of db cannot exceed 100%')

        if not self._sb_poll > 0:
            raise ValueError('Bad sysbench poll interval: {}'.format(self._sb_poll))

        # If target is one of DMX/RAM
        if self._target not in self._TARGETS:
            raise ValueError('Target: {}. Supported: {}.'.format(self._target,
                                                                 self._TARGETS))
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
        db_max_cnt = int(self.merge_dbcnf('max_connections', self._MY_MAX_CONN))

        if self._threads <= 0:
            raise ValueError('Invalid thread count: {}'.format(self._threads))

        if self._threads >= db_max_cnt:
            raise ValueError('Too many sysbench threads: {}, '
                             'max_connections@my.cnf is {}'.format(self._threads,
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
    def fast_mode(self):
        return self._fast_mode

    @fast_mode.setter
    def fast_mode(self, val):
        self._fast_mode = val

    @property
    def procs(self):
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
        # I bet you won't need a PB.
        matrix = {'K': 1024,
                  'M': 1024 ** 2,
                  'G': 1024 ** 3,
                  'T': 1024 ** 4}
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

    def _run_remote(self, cmd, suppress=False):
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
            self._trans = paramiko.Transport(self._db_ip, self._SSH_PORT)

        # Reconnect to remote server if the connection is inactive.
        try:
            if not self._trans.is_active():
                key_path = os.path.expanduser(self._ID_RSA_PATH)
                key = paramiko.RSAKey.from_private_key_file(key_path)
                # The default banner timeout in paramiko is 15 sec
                self._trans.connect(username=self._sys_user, pkey=key)

            # Each command needs a separate session
            session = self._trans.open_channel("session", timeout=60)
        except (socket.error,
                socket.timeout,
                paramiko.SSHException,
                EOFError,
                RuntimeError,
                ConnectionError,
                ) as e:
            self._trans_fails += 1
            # Quit the sweep if 1. ssh fails twice. and 2. sysbench has not started.
            # If the sysbench has started we will ignore the ssh error and let it run.
            if (self._trans_fails >= self._MAX_TRANS_FAILS and
                    not self._sb_launched):
                raise SweepFatalError from e

            return exit_status, result

        # session.get_pty() -- Do I need this?
        session.exec_command(cmd)

        while True:
            if session.recv_ready():
                buff = session.recv(4096).decode('utf-8')
                buff = buff.strip().replace('\r', '')
                if not suppress:
                    for line in buff.split('\n'):
                        log.debug('[db] {}'.format(line))
                result += buff
            # We can break out if there is no buffered data and the process
            # has exited.
            elif session.exit_status_ready():
                break
            time.sleep(self._REMOTE_OUT_CHK_INTERVAL)

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
        # AttributeError may happen if an error happens in __init__()
        with contextlib.suppress(AttributeError):
            if self._trans:
                self._trans.close()
                self._trans = None

    def db_cmd(self, cmd, suppress=False):
        """
        Run a command remotely on the database server
        :param suppress:
        :param cmd:
        :return: exit_status
        """
        # return self._run_remote(cmd)
        return self._run_remote(cmd, suppress)

    def toggle_enabled(self):
        """
        Check if the toggle feature is enabled.
        :return:
        """
        if hasattr(self, 'warmup') and self.warmup:
            return False
        return not (self._toggle_pct == 0)

    def toggle_in_list(self):
        """
        Return a bunch of db ports to start and replace the old ones
        :return:
        """
        candidates = set(self._db_port_pool) - set(self._active_db_pool)
        return sorted(random.sample(candidates,
            int(self._toggle_pct * len(self._db_port_pool)/100)))

    def toggle_out_list(self):
        """
        Return the db ports which will be killed then.
        :return:
        """
        return sorted(random.sample(set(self._active_db_pool),
            int(self._toggle_pct * len(self._db_port_pool)/100)))

    def toggle_timeout(self):
        """
        Check for toggle timeout.
        :return:
        """
        return (time.time() - self.toggle_base_time) >= self._toggle_time

    def kill_proc(self, proc):
        """
        Just kill the process.
        :param proc:
        :return:
        """
        if proc not in self._procs:
            log.warning('Not found proc:{}, self._procs:{}'.format(proc, self._procs))
            return

        if isinstance(self._procs[proc], Command):
            self._procs[proc].stop()
        else:
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
            except (ProcessLookupError, BrokenPipeError, ValueError) as e:
                log.warning('Failed to kill ({}) ({})'.format(proc.pid, e))

        args_join = self.get_joined_args(proc)
        log.debug('Stopped: ({}) {}.'.format(proc.pid, self.digest(args_join)))

    def digest(self, cmd_str):
        """Return a digest of a long command string. Note that this function accepts
        only str type parameter"""
        return '{head}...{tail}'.format(head=cmd_str[:20], tail=cmd_str[-20:])

    def epoll_register(self, proc):
        """
        Register itself in the epoll structure.
        :param proc:
        :return:
        """
        self.p.register(proc.stdout, POLLIN | POLLERR | POLLHUP)
        stdout_fd = proc.stdout.fileno()
        self.stdout_dict[stdout_fd] = proc

    def epoll_unregister(self, proc):
        """
        Unregister from the epoll.
        :param proc:
        :return:
        """
        try:
            self.p.unregister(proc.stdout)
        except ValueError:
            # log.warning('Warning in epoll_unregister:{}'.format(e))
            pass

        stdout_fd = proc.stdout.fileno()
        del self.stdout_dict[stdout_fd]

    def _start_proc(self, cmd):
        """Do nothing other than start a process"""
        if isinstance(cmd, str):
            proc = Popen(shlex.split(cmd), shell=False, stdout=PIPE, stderr=PIPE,
                         universal_newlines=True, close_fds=True,
                         preexec_fn=os.setsid)
        elif isinstance(cmd, Popen):
            proc = cmd
        elif isinstance(cmd, Command):
            if not (hasattr(self, 'warmup') and self.warmup):
                cmd.benchmark_start = self.start_time
            proc = cmd.start()
        else:
            raise ValueError('Bad type in Sweep._start_proc: {}'.format(type(cmd)))
        return proc

    def start_proc(self, cmd, port=0):
        """
        Start the process, add it to some lists, then register itself to the epoll
        If the process is a sysbench process, the port should also be added to the
        self._active_db_pool set.
        :param cmd:
        :param port:
        :return:
        """
        proc = self._start_proc(cmd)
        self.register_struct(proc, cmd, port)
        return proc

    def register_struct(self, proc, cmd, port=0):
        """Register the process into a bunch of structs"""
        self.running_p.append(proc)
        cmd_obj = cmd if isinstance(cmd, Command) else None
        self._procs[proc] = cmd_obj
        self.epoll_register(proc)
        if port:
            self._active_db_pool[port] = proc

    def release_struct(self, proc, port=0):
        """Release the structs around this process
        """
        self.epoll_unregister(proc)
        self.running_p.remove(proc)
        self._procs.pop(proc)
        # Remove the deactivated db from the active_db list
        if port:
            self._active_db_pool.pop(port, None)

    def release_proc(self, proc, port=0):
        """
        Kill process, unregister from epoll and remove itself from certain lists.
        :param proc:
        :param port:
        :return:
        """
        self.kill_proc(proc)
        self.release_struct(proc, port)

    def get_sb_log_name(self, port):
        """
        Get sysbench logfile name.
        :param port:
        :return:
        """
        sb_log_name = 'sb_{}_{}_db{}.log'.format(self._target,
                                                 self._threads,
                                                 port)
        return os.path.join(self._dir, sb_log_name)

    def launch_sysbench(self, port):
        """ Launch a sysbench process against 'port' based on the predefined
        parameters in Sweep instance.
        It returns a subprocess.POpen instance
        :param port:
        :return:
        """
        sb_log = self.get_sb_log_name(port)
        sb_cmd = self.sb_template(port)
        return self.start_proc(SysbenchCommand(sb_cmd, sb_log, self.start_time), port)

    def cmd_is_sysbench(self, cmd):
        """Check if the cmd is a sysbench command"""
        cmd = self.get_joined_args(cmd)
        return 'sysbench ' == cmd[:len('sysbench ')]

    def get_joined_args(self, proc):
        """Join args list with blankspace"""
        if isinstance(proc, Command):
            return proc.cmd_str
        elif isinstance(proc, Popen):
            return ' '.join(proc.args)
        elif isinstance(proc, list):
            return ' '.join(proc)
        elif isinstance(proc, str):
            return proc
        else:
            return ''

    def get_proc_port(self, proc):
        """Check if the process is a sysbench, returns its port if it is,
        otherwise returns 0. The proc parameter may be a string of the command
        or a subprocess.POpen object.
        """
        port = 0
        cmd = self.get_joined_args(proc)

        if self.cmd_is_sysbench(cmd):
            try:
                port = int(re.search(r'--mysql-port=(\d{1,5})', cmd).group(1))
            except AttributeError:
                pass
        return port

    def check_proc_print(self, result):
        """Check the poll result and print if anthing is in it"""
        if len(result):
            # result --> a list of processes structs
            # m[0]   --> file_no of the stdout of that process.
            #            the stderr is also redirected to PIPE
            # m[1]   --> signal
            for fd, event in result:
                if event & POLLIN:
                    cmd = self._procs[self.stdout_dict[fd]]
                    if cmd is None:
                        out_str = self.stdout_dict[fd].stdout.readline().strip()
                        log.debug('(id:{}) {}'.format(fd, out_str))
                    elif isinstance(cmd, Command):
                        cmd.stdout_handler(self.stdout_dict[fd].stdout)

    def check_proc_print_err(self, proc):
        """Check and print the error message of a process"""
        if not isinstance(proc, Popen):
            return
        out_msg, err_msg = proc.communicate()
        reason = '{out} {err}'.format(out=out_msg.rstrip(), err=err_msg.rstrip())
        args_join = self.get_joined_args(proc)
        log.warning('Command failed:({}) {}'.format(proc.pid, self.digest(args_join)))
        log.warning('(Reason: {})'.format(reason))

    def toggle_action(self):
        """Start a toggle action"""
        # deactivate some db then activate the same number db
        in_list = self.toggle_in_list()
        out_list = self.toggle_out_list()
        log.info('Start to toggle. Out: {}; In: {}'.format(out_list, in_list))

        # kill sysbench for old active db

        for proc in list(self.running_p):
            port = self.get_proc_port(proc)
            if port in out_list:
                log.info('Toggle out database: {}'.format(port))
                self.release_proc(proc, port)

        # Start sysbench for new active db. The port of the new active db will be added
        # to the self._active_db_pool set inside the launch_sysbench function.
        for port in in_list:
            proc = self.launch_sysbench(port)
            args_join = self.get_joined_args(proc)
            log.debug('Toggle in (pid:{}) cmd=({})'.format(proc.pid, args_join))
        active = sorted(set(self._active_db_pool))
        act_list = ['{}/{}'.format(port, self.get_pid_from_port(port)) for port in active]
        act_list = '[{}]'.format(', '.join(act_list))

        toggle_msg = 'Toggle finished, active db (port/pid): {}, time remaining: {}/{}'
        remaining_t = self._duration - int(time.time() - self.start_time)
        log.info(toggle_msg.format(act_list, remaining_t, self._duration))

    def calculate_dbpid_cache(self):
        """Get the port-pid dict and cache it"""
        # cmd = "ps aux|grep 'mysqld '|grep -v grep|awk '{print $1, $20}'"
        cmd = "ps -C mysqld -o pid,cmd"
        exit_status, pids = self.db_cmd(cmd, suppress=True)
        if exit_status == 0:
            pids = dict(re.findall(r'(\d+).+--port=(\d+)', pids))
            pids = {int(v): int(k) for k, v in pids.items()}
            setattr(self, 'dbpid_cache', pids)
            return True
        else:
            return None

    def get_pid_from_port(self, port):
        """Get the process id from the port number"""
        if not hasattr(self, 'dbpid_cache'):
            if not self.calculate_dbpid_cache():
                return None
        return getattr(self, 'dbpid_cache').get(int(port))

    def _run_local(self, cmd_set, timeout, msg=None):
        """
        Accept a bunch of commands and run them concurrently in background.
        :param cmd_set:
        :return:
        """
        assert cmd_set
        if not isinstance(cmd_set, list):
            if isinstance(cmd_set, str):
                cmd_set = [cmd_set]
            else:
                log.error("Invalid cmd: {}".format(cmd_set))
                return

        _timeout = timeout
        start = time.time()
        if hasattr(self, 'warmup') and self.warmup:
            pass
        else:
            self.start_time = start

        # Use context management to close epoll object in the end.
        # with I/O multiplexing we can run and check multiple commands in parallel.
        with epoll() as p:
            self.p = p
            # pipe_dict = {}
            for cmd in cmd_set:
                port = self.get_proc_port(cmd)
                proc = self.start_proc(cmd, port=port)
                args_join = self.get_joined_args(proc)
                log.debug('Started: (pid:{}) cmd=({})'.format(proc.pid, args_join))
                local_cmd = True if port else False
                if not local_cmd:
                    time.sleep(self._SSH_BURST_INTERVAL)

            if msg:
                log.info(msg)

            self.reset_toggle_base_time()
            while self.running_p and (time.time() - start) < _timeout:
                # #1. First let us check if the toggle feature is enabled.
                #     Check the current time if so.
                if not self.toggle_enabled():
                    # No toggle needed, skip the clumsy processing.
                    pass
                elif self.toggle_timeout():
                    self.reset_toggle_base_time()  # reset the base time
                    self.toggle_action()

                # #2. Get the processes list which have printed something.
                # Note that this 'poll' is a function of epoll, which is not the same thing as
                # the proc.poll() in the next a few lines.
                self.check_proc_print(self.p.poll(timeout=1))
                # #3. Check the running status of the processes.

                for proc in list(self.running_p):
                    ret = proc.poll()
                    if ret is not None:  # Process finished - check the status then.
                        # Remove finished process ASAP from local and global lists,
                        # as well as epoll list
                        with contextlib.suppress(ValueError):
                            port = self.get_proc_port(proc)
                            self.release_struct(proc, port)

                        if ret != 0:  # Process failed.
                            self.check_proc_print_err(proc)

                            # Check if sysbench is failed and do fast-fail if so.
                            # As sysbench failure is a critical error.
                            if self.cmd_is_sysbench(proc):
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
                            args_join = self.get_joined_args(proc)
                            log.debug('Done: (cmd={})'.format(self.digest(args_join)))
                            if self.cmd_is_sysbench(proc):
                                elapsed = int(time.time() - start)
                                log.info('Sysbench done ({}s).'.format(elapsed))
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
                        # proc.poll() == None means: This process is still running.
                        # So we check the next command in the running_procs list.
                        continue
        # Kill all the local running processes when the sweep is successfully finished.
        # When a fatal error happens, the 'running_procs' will not be empty here, all the
        # local processes will be killed outside of this function, when the Sweep object
        #  is released.

        for proc in list(self.running_p):
            port = self.get_proc_port(proc)
            self.release_proc(proc, port)
        # We may not need this, but anyways... Let's clean up the context
        self.reset_structures()

    def reset_structures(self):
        """Reset all process related structures"""
        self.running_p = []
        self.p = None
        self.stdout_dict = {}
        self._active_db_pool = {}
        self.toggle_base_time = 0

    def copy_mysql_err_logs(self):
        """
        This function copies MySQL logs to sweep directory when sysbench is failed.
        :return:
        """
        exit_status, hostname = self.db_cmd('hostname', suppress=True)
        if exit_status == 0:
            hostname = hostname.rstrip('\n')
            for idx in range(1, self._db_num + 1):
                db_dir = os.path.join(self._db_base, 'mysql{idx}'.format(idx=idx))
                log_name = '{hostname}.err'.format(hostname=hostname)
                db_f = os.path.join(db_dir, log_name)
                local_f = 'mysql{idx}_{name}'.format(idx=idx, name=log_name)
                log.debug('Copying {} db->local: {}'.format(db_f, local_f))
                self.copy_db_file(db_f, local_f)

    def teardown_procs(self):
        """
        Kill all the running processes. For now this function is only called
        when the Sweep object is closing.
        :return:
        """
        try:
            procs = list(self._procs)
        # An error happens in __init__() before all_running_procs is defined.
        except AttributeError:
            return

        for proc in procs:
            # log.debug('Killing: ({}) {}.'.format(proc.pid, proc.args))
            # proc.kill()  # This would not work for 'shell=True'
            self.kill_proc(proc)

        self._procs = {}
        # Kill tdctl and monitor as these two commands won't exit by themselves.
        # However it will be ignored if there has been some error in the SSH connection.
        # Ignore this step if the sysbench has not been started yet.
        if self._sb_launched:
            if self._trans_fails == 0:
                # We must kill these two commands otherwise they may run forever.
                self.db_cmd('killall tdctl monitor', suppress=True)
            else:
                log.warning('Ignored killing tdctl and monitor on db server. ')

    def run_client_cmd(self, cmds, timeout, msg=None):
        """
        Run a bunch of commands on the client server.
        :param msg:
        :param cmds:
        :param timeout:
        :return:
        """
        self._run_local(cmds, timeout, msg)

    def clean_db(self):
        """
        Clean up the database environment
        :return:
        """
        log.info('Running database clean-up program, which may take a long while.')
        cleanup_script = os.path.join(self._script_dir, 'cleandb.py')

        if self._skip_db_recreation:
            skip_db_recreation = '-o skip_db_recreation'
        else:
            skip_db_recreation = ''

        staging_dir = '/tmp/{}'.format(self._dir)
        db_p = self._db_params.copy()
        db_p['db_name'] = self._db_name
        db_p['sock_prefix'] = self._mysql_sock

        params = ' '.join(['{}={}'.format(k, v) for k, v in db_p.items()])
        cmd_template = '{cleanup_script} {db_num} {skip_db_recreation} ' \
                       '-n {staging_dir} -d {base_dir} -z {tarball} ' \
                       '-t {timeout} -s {strips} -v -p "{params}" 2>&1'
        clean_db_cmd = cmd_template.format(cleanup_script=cleanup_script,
                                           db_num=self._db_num,
                                           skip_db_recreation=skip_db_recreation,
                                           staging_dir=staging_dir,
                                           base_dir=self._db_base,
                                           tarball=self._tarball,
                                           timeout=self._db_start_timeout,
                                           strips=self._tar_strips,
                                           params=params,
                                           log_path=self._script_dir)

        exit_status, result = self.db_cmd(clean_db_cmd)
        # log.debug('clean_db exit status: {}'.format(exit_status))

        # The cleanup.py from the database server is failed.
        if not self.db_ready(exit_status):
            log.error('Database cleanup failed with {}'.format(exit_status))
            self._success = False
            raise SweepFatalError('Fatal error happened in clean_db()')
        log.info('Done: Database is ready.')

    @staticmethod
    def db_ready(ret_code):
        """
        Check if the database cleanup script has returned successfully.
        :param ret_code:
        :return:
        """
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
                return

        for proc in psutil.process_iter():
            for name in proc_names:
                with contextlib.suppress(psutil.NoSuchProcess):
                    cmd = ' '.join(proc.cmdline())
                    pid = proc.pid
                    if name in cmd and pid != skip_pid:
                        log.debug('Kill:({}) {}'.format(pid, cmd))
                        proc.kill()
                        continue

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

    def initial_db_ports(self):
        """
        Return the initial set of active databases.
        :return:
        """
        return sorted(random.sample(self._db_port_pool,
                                    int(self._active_ratio * self._db_num / 100)))

    def run_once(self):
        """
        Run a benchmark with sysbench thread number=thread_num. This function
        will launch a bunch of commands (sysbench, other system monitoring
        commands.) and record the logs to remote database server. After the
        benchmark is finished, the logs will be copied from database server
        to this server.
        - and may plot and compress them.
        - and send an email.
        :return:
        """
        log.info('*****Benchmark of {} threads started*****'.format(self._threads))
        log.info('Database logs in {ip}:/tmp/{dir}'.format(ip=self._db_ip,
                                                           dir=self._dir))

        # 0. list to store all commands and logs----------------------------
        curr_logs = []  # Record the file names of all current logs.
        all_cmds = []  # All the commands need to be executed

        # Warm up the database
        if self.need_warmup():
            log.info('Start to warm up the db for {} sec'.format(self._warmup_time))
            self.warmup = True
            warmup_cmds = []
            for port in self._db_port_pool:
                warmup_log = '/dev/null'
                warmup_cmd = self.sb_template(port, run_time=self._warmup_time)
                warmup_cmds.append(SysbenchCommand(warmup_cmd, warmup_log))

            real_timeout = int(self._warmup_time)
            msg = 'The database is warming up. Wait for {} seconds'.format(self._warmup_time)
            self.run_client_cmd(warmup_cmds, real_timeout, msg)
            del self.warmup
            log.info('Warm-up done [{} seconds].'.format(self._warmup_time))

        # Select a certain number of databases to run sysbench. The default is 100 percent.
        initial_db_ports = self.initial_db_ports()
        log.info('Initial active db: {}'.format(initial_db_ports))

        # 1. sysbench commands ---------------------------------------------
        for port in initial_db_ports:
            # For each instance, record sysbench logs and innodb status logs, etc.
            # 1. the sysbench logs:
            sb_log = self.get_sb_log_name(port)
            sb_cmd = self.sb_template(port)
            all_cmds.append(SysbenchCommand(sb_cmd, sb_log))
            curr_logs.append(sb_log)

        if self._innodb_poll:
            for port in self._db_port_pool:
                # 2. The innodb status logs - every 60 seconds------------------------------
                # We monitor all the databases although not all of them have workload.
                innodb_log_name = 'innodb_status_db{}.log'.format(port)
                innodb_log = os.path.join(self._dir, innodb_log_name)
                innodb_cmd = "while true; " \
                             "do " \
                             "ssh {user}@{ip} \"mysql -S {socket_prefix}{db_idx} -e " \
                             "'show engine innodb status\G' | grep -A 28 -E 'LOG|END' " \
                             "&>> /tmp/{log_name}\"; " \
                             "  sleep {innodb_poll_interval}; " \
                             "done".format(user=self._sys_user,
                                           ip=self._db_ip,
                                           socket_prefix=self._mysql_sock,
                                           db_idx=port,
                                           log_dir=self._dir,
                                           log_name=innodb_log,
                                           innodb_poll_interval=self._innodb_poll)

                all_cmds.append(innodb_cmd)
                curr_logs.append(innodb_log)

        # 3. Commands for system monitoring-------------------------------------------------
        os_cmds = {'iostat -dmx {} -y'.format(self._ssd): self._iostat_poll,
                   'mpstat': self._mpstat_poll,
                   'vmstat -S M -w': self._vmstat_poll,
                   'tdctl -v --dp +': self._tdctl_poll}
        for cmd in os_cmds:
            if os_cmds[cmd] != 0:
                os_log = '{}_{}_{}.log'.format(cmd.split()[0],
                                               self._target,
                                               self._threads)
                sys_log = os.path.join(self._dir, os_log)
                count = '' if 'tdctl' in cmd else int(self._duration / os_cmds[cmd])
                sys_cmd = 'ssh {user}@{ip} "{cmd} {poll} {count} ' \
                          '&> /tmp/{log_name}"'.format(user=self._sys_user,
                                                       ip=self._db_ip,
                                                       cmd=cmd,
                                                       poll=os_cmds[cmd],
                                                       count=count,
                                                       log_dir=self._dir,
                                                       log_name=sys_log)
                all_cmds.append(sys_cmd)
                curr_logs.append(sys_log)

        # Only capture client vmstat and network traffic when client and server are
        # deployed separately.
        if self._db_ip != '127.0.0.1':
            # 4. Commands for client monitoring--------------------------------------------------
            if self._vmstat_poll:
                client_cmds = ('vmstat -S M -w',)
                for cmd in client_cmds:
                    client_log_name = '{}_{}_{}_client.log'.format(cmd.split()[0],
                                                                   self._target, self._threads)
                    client_log = os.path.join(self._dir, client_log_name)
                    count = int(self._duration / self._vmstat_poll)
                    full_client_cmd = '{cmd} 10 {count} &> {log}'.format(cmd=cmd,
                                                                         count=count,
                                                                         log=client_log)
                    all_cmds.append(full_client_cmd)
                    curr_logs.append(client_log)

            if self._network_poll:
                # 5. The network traffic logs: sar, every 10 seconds-----------------------
                sar_log = os.path.join(self._dir, 'network_traffic.log')
                sar = "sar -n DEV 10 {cnt} " \
                      "|grep -E `ip addr show | grep {ip} | awk '{{print $NF}}'` " \
                      "&> {log_name}"
                sar_cmd = sar.format(cnt=int(self._duration / self._network_poll),
                                     ip=self._client_ip,
                                     log_name=sar_log)

                all_cmds.append(sar_cmd)
                curr_logs.append(sar_log)
        else:  # if self._db_ip == '127.0.0.1'
            pass

        # Only capture DMX logs when the benchmark target is DMX.
        if self._target == 'DMX':
            if self._barf_fr_poll:
                # 6. The dmx monitoring logs: barf --fr - every 10 seconds----------------------------
                barf_fr_log = os.path.join(self._dir, 'barffr_.log')
                barf_fr_cmd = "while true; " \
                              "do " \
                              "ssh {user}@{ip} 'barf --fr &>> /tmp/{log_name}'; " \
                              "sleep {poll}; " \
                              "done".format(user=self._sys_user,
                                            ip=self._db_ip,
                                            log_dir=self._dir,
                                            log_name=barf_fr_log,
                                            poll=self._barf_fr_poll)

                all_cmds.append(barf_fr_cmd)
                curr_logs.append(barf_fr_log)

            if self._barf_act_algo_poll:
                # 7. The dmx monitoring logs: barf -a --ct algo - every 10 seconds--------------------
                barf_act_algo_log = os.path.join(self._dir, 'barf_a_ct_algo.log')
                barf_act_algo_cmd = "while true; " \
                                    "do " \
                                    "ssh {user}@{ip} 'barf -a --ct algo &>> /tmp/{log_name}'; " \
                                    "sleep {poll}; " \
                                    "done".format(user=self._sys_user,
                                                  ip=self._db_ip,
                                                  log_dir=self._dir,
                                                  log_name=barf_act_algo_log,
                                                  poll=self._barf_act_algo_poll)

                all_cmds.append(barf_act_algo_cmd)
                curr_logs.append(barf_act_algo_log)

            if self._barf_act_bf_poll:
                # 8. The dmx monitoring logs: barf -a --ct bf - every 10 seconds---------------------
                barf_act_bf_log = os.path.join(self._dir, 'barf_a_ct_bf.log')
                barf_act_bf_cmd = "while true; " \
                                  "do " \
                                  "ssh {user}@{ip} 'barf -a --ct bf &>> /tmp/{log_name}'; " \
                                  "sleep {poll}; " \
                                  "done".format(user=self._sys_user,
                                                ip=self._db_ip,
                                                log_dir=self._dir,
                                                log_name=barf_act_bf_log,
                                                poll=self._barf_act_bf_poll)

                all_cmds.append(barf_act_bf_cmd)
                curr_logs.append(barf_act_bf_log)

            # 9. The dmx monitoring logs: monitor, every 10 seconds------------------------------
            if self._monitor_poll:
                pids = self.get_database_pid()
                for idx, pid in enumerate(pids, self._db_port):
                    if not pid:
                        continue
                    monitor_log = os.path.join(self._dir, 'monitor_p_db{}.log'.format(idx))
                    monitor = 'monitor -p {pid} -D {poll}'.format(pid=pid, poll=self._monitor_poll)
                    monitor_cmd = 'ssh {user}@{ip} ' \
                                  '"{cmd} &> /tmp/{log}"'.format(user=self._sys_user,
                                                                 ip=self._db_ip,
                                                                 cmd=monitor,
                                                                 log_dir=self._dir,
                                                                 log=monitor_log)
                    all_cmds.append(monitor_cmd)
                    curr_logs.append(monitor_log)
        else:  # if self._target == 'DMX'
            pass

        # 10. Shoot the commands out-----------------------------------------------
        self._running_sb = self._db_num
        self._sb_launched = True
        # Adjust the duration to let the commands quit by themselves.
        real_timeout = int(self._duration)
        msg = 'Benchmark is now running. Check logs in {}'.format(self._dir)
        self.run_client_cmd(all_cmds, real_timeout, msg)
        self._logs.extend(curr_logs)
        return curr_logs

    def plot(self):
        """
        Plot the logs: sysbench, system monitor logs (iostat, mpstat, vmstat, etc)
        :return:
        """
        log.info('We are now plotting the sweep.')
        plot_files = []
        for x in os.listdir(self._dir):
            if x.endswith('.log'):
                plot_files.append(os.path.join(self._dir, x))

        plot_files = ' '.join(plot_files)
        plot_cmd = './plotter.py ' \
                   '-p {prefix} ' \
                   '-b {buffer_size} ' \
                   '-s {sb_poll_step} ' \
                   '-d {duration} ' \
                   '-r {redo_size} ' \
                   '{files}'.format(prefix=self._dir,
                                    buffer_size=self._bp,
                                    sb_poll_step=self._sb_poll,
                                    duration=self._duration,
                                    redo_size=self._log_size,
                                    files=plot_files)

        # The timeout of plot is 600 seconds, it will be killed if
        # not return before timeout
        self.run_client_cmd(plot_cmd, self._PLOT_TIMEOUT)

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
            # Check sysbench logs
            if tail.startswith('sb'):
                check_cmd = "tail -2 {} | awk '{{print $1, $2}}'".format(file)
                # The return value of check_output will be a string since
                # universal_newlines is True
                started = check_output(check_cmd,
                                       shell=True,
                                       universal_newlines=True)
                started = started.replace('\n', ' ')
                # A simple hard-coded check to the sysbench logs
                if 'FATAL' in started:
                    log.warning('FATAL error found in {}.'.format(file))
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
        self.run_client_cmd(cmd, timeout=180)

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
            log.warning('Cannot open {} for db output'.format(save_to))

    def get_database_pid(self):
        """
        This function returns a list which contains the pid of all the
        MySQL processes
        :return:
        """
        # cmd = "grep pid-file /etc/my.cnf | awk -F= '{{print $NF}}' " \
        #       "| head -{} | xargs cat".format(self._db_num)
        # exit_status, pids = self.db_cmd(cmd, suppress=True)
        # if exit_status == 0:
        #     pids = pids.split('\n')
        # else:
        #     pids = None
        # return pids
        if not hasattr(self, 'dbpid_cache'):
            if not self.calculate_dbpid_cache():
                return None
        return getattr(self, 'dbpid_cache').values()

    def start(self):
        """
        Start the sweep. The entry point of the benchmark(s).
        :return:
        """
        if not self._success:
            log.warning('Sweep has already failed. Aborting it.')
            return

        with contextlib.suppress(FileExistsError):
            os.mkdir(self._dir)

        try:
            # Copy the sweep config file to the log directory.
            cnf_dest = os.path.join(self._dir, self._cnf_file)
            shutil.copy2(self._cnf_file, cnf_dest)
        except FileNotFoundError as e:
            log.warning('Sweep config file is gone now! {}'.format(e))

        log.info('Sweep started. Logs in directory {}.'.format(self._dir))

        self.clean_client()
        if self.fast_mode:
            fast_msg = """
            You are running in fast mode!
            Double check the environment and make sure you know what you are doing.
            - Skip cleaning up the database, db parameters may not be applied.
            - Skip cleaning up monitoring processes, if any."""
            log.warning(fast_msg)
            staging_dir = '/tmp/{}'.format(self._dir)
            create_staging = 'mkdir {}'.format(staging_dir)
            self.db_cmd(create_staging)
        else:
            self.clean_db()

        # Run the benchmark and check results, self._success will be set in it.
        self.post_check(self.run_once())
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
            if self._change_cnf_ext:
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


class Command:
    """The Command class represents the command that would be executed during
    the benchmark. There are two types of commands (but not yet classified):
    1. Commands to generate workload:
        - sysbench
    2. Commands to monitor the system
        - iostat
        - vmstat
        - sar
        - barf
        ...
    """
    BUF_SIZE = 1 << 16
    log_filter = set()

    def __init__(self, cmd, outfile=None, benchmark_start=None):
        if isinstance(cmd, str):
            self.cmd_str = cmd
        elif isinstance(cmd, self.__class__):
            self.cmd_str = cmd.cmd_str
            self.outfile = cmd.outfile

        if outfile:
            self.outfile = outfile
        if benchmark_start:
            self._benchmark_start = benchmark_start
        self.stdout_fd = None
        self.proc = None

    @property
    def benchmark_start(self):
        """This property records the start timestamp of the benchmark.
        Note that this time may be earlier than the start time of this
        command"""
        return self._benchmark_start

    @benchmark_start.setter
    def benchmark_start(self, start_time):
        if start_time is None:
            raise RuntimeError('Benchmark start time should not be None.')

        # When the benchmark time is records, the timestamp is also recorded
        # into the log file.
        self._benchmark_start = start_time
        if not os.path.isfile(self.outfile):
            zero = 'START_TIME: {}\n'.format(int(self._benchmark_start))
            with open(self.outfile, 'a') as f:
                f.write(zero)

    def start(self):
        """Start the command. This function opens the process for this command
        and record the start time of this time.
        There may be multiple START_TIME stamps as the same command may be started
        multiple times."""
        self.proc = Popen(shlex.split(self.cmd_str), shell=False, stdout=PIPE,
                          stderr=PIPE, universal_newlines=True, close_fds=True,
                          preexec_fn=os.setsid)
        # Open output file descriptor
        try:
            self.stdout_fd = open(self.outfile, 'a')
            start_t = 'START_TIME: {}\n'.format(int(time.time()))
            self.stdout_fd.write(start_t)
            self.stdout_fd.flush()
        except OSError as e:
            raise e
        return self.proc

    def stop(self):
        """Stop the command and close the file descriptor."""
        try:
            os.killpg(os.getpgid(self.proc.pid), signal.SIGTERM)
        except (ProcessLookupError, BrokenPipeError, ValueError) as e:
            log.warning('Failed to kill ({}) ({})'.format(self.proc.pid, e))

        try:
            # Close the files
            self.stdout_fd.close()
        except OSError:
            pass

    def stdout_handler(self, out):
        """This function is called when the epoll gets a signal
        Basically it will filter some unwanted output then print
        others to the outfile.
        A flush needs to be called each time after the output as
        otherwise the data will be buffered in memory."""
        if not (hasattr(self, 'proc') and hasattr(self, 'stdout_fd')):
            err_msg = 'stdout_handler called without fd: {}'.format(self.cmd_str)
            raise RuntimeError(err_msg)
        line = out.readline()
        if self.filter(line):
            self.stdout_fd.write(line)
            self.stdout_fd.flush()

    def filter(self, line):
        """We may need only a certain types of lines. The subclass of this class
        may redefine the .log_filter attribute to change the filter criteria."""
        for ptn in self.log_filter:
            if line.startswith(ptn):
                return True
        return False


class SysbenchCommand(Command):
    log_filter = {'[', 'ALERT', 'FATAL'}


def get_args():
    """
    Parse arguments and return an args object
    :return:
    """
    cmd_desc = "This program runs the benchmarks defined by a config file."
    parser = argparse.ArgumentParser(description=cmd_desc)
    parser.add_argument("config", help="config file name/path")
    parser.add_argument("-v", help="verbose ( -v: info, -vv: debug, -vvv: colored)",
                        action='count', default=0)

    return parser.parse_args()


def set_log_level(level):
    """Set log level according to the argument"""
    if level == 0:
        log_level = logging.ERROR
    elif level == 1:
        log_level = logging.INFO
    elif level == 2:
        log_level = logging.DEBUG
    elif level >= 3:
        log_level = logging.DEBUG
        import coloredlogs
        coloredlogs.install(level=log_level, fmt='%(asctime)s: %(message)s')
    else:
        log_level = logging.INFO

    logging.basicConfig(level=log_level,
                        stream=sys.stdout,
                        format='%(asctime)s %(levelname)s: %(message)s',
                        datefmt='%m-%d %H:%M:%S')
    # I don't want to see paramiko debug logs, unless they are WARNING or worse
    # than that.
    logging.getLogger("paramiko").setLevel(logging.WARNING)


def print_banner():
    """Print the banner of the benchmark."""
    log.info('-----------------------------------------------------------')
    log.info('------New sweep config file found, preparing to start------')
    log.info('-----------------------------------------------------------')


def main():
    """The main function to run the sweep.
    """
    args = get_args()
    set_log_level(args.v)
    print_banner()

    start_at = time.time()
    log_dir = None
    status = 'unknown'

    try:
        with Sweep(args.config) as sweep:
            log_dir = sweep.log_dir
            sweep.start()
            status = 'finished' if sweep.success else 'failed'
    except KeyboardInterrupt:
        # We cannot use logging here as the pipe is already broken
        status = 'canceled'
        print('Ctrl-C pressed by user. I will kill the running processes')
        if log_dir:
            with contextlib.suppress(OSError, FileExistsError, FileNotFoundError):
                os.rename(log_dir, log_dir + '_CANCELED')
    except SweepFatalError as err:
        status = 'failed'
        log.error(err)

    overall_t = int(time.time() - start_at)
    log.info('The sweep is {} (time taken: {}s). Bye.'.format(status, overall_t))
    return 0


if __name__ == "__main__":
    sys.exit(main())
