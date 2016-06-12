#!/usr/bin/env python3
# bmk.py

import os
import sys
import atexit
import signal
import psutil
from collections import OrderedDict
from configparser import ConfigParser, NoSectionError, NoOptionError
import logging
import logging.handlers
import time
import paramiko
import pexpect
import getpass


class BmkConfig:
    def __init__(self):
        self._cnf_file = ''

        self._db_ip = ''
        self._db_port = ''
        self._db_user = ''
        self._client_ip = ''
        self._client_port = ''
        self._client_user = ''

        self._sysbench_threads = []
        self._db_inst_num = 0
        self._benchmark_target = ''
        self._benchmark_type = ''
        self._benchmark_time = 0
        self._db_size = 0

        self._loaded = False

    @property
    def cnf_file(self):
        return self._cnf_file

    @property
    def db_ip(self):
        return self._db_ip

    @property
    def db_port(self):
        return self._db_port

    @property
    def db_user(self):
        return self._db_user

    @property
    def client_ip(self):
        return self._client_ip

    @property
    def client_port(self):
        return self._client_port

    @property
    def client_user(self):
        return self._client_user

    @property
    def sysbench_threads(self):
        return self._sysbench_threads

    @property
    def db_inst_num(self):
        return self._db_inst_num

    @property
    def benchmark_target(self):
        return self._benchmark_target

    @property
    def benchmark_type(self):
        return self._benchmark_type

    @property
    def benchmark_time(self):
        return self._benchmark_time

    @property
    def db_size(self):
        return self._db_size

    @property
    def loaded(self):
        return self._loaded

    def load_config(self, cnf_file):  # TODO: Add my.cnf and bfapp.d config for the sweep
        cnf = ConfigParser()

        try:
            cnf.read(cnf_file)

            self._db_ip = cnf.get('server', 'db_ip')
            self._db_port = cnf.getint('server', 'db_port')
            self._db_user = cnf.get('server', 'db_user')
            self._client_ip = cnf.get('server', 'client_ip')
            self._client_port = cnf.getint('server', 'client_port')
            self._client_user = cnf.get('server', 'client_user')

            self._sysbench_threads = [int(x.strip()) for x in cnf.get('benchmark', 'sysbench_threads').split(sep=',')]
            self._db_inst_num = cnf.getint('benchmark', 'db_inst_num')
            self._benchmark_target = cnf.get('benchmark', 'benchmark_target')
            self._benchmark_type = cnf.get('benchmark', 'benchmark_type')
            self._benchmark_time = cnf.getint('benchmark', 'benchmark_time')

            self._db_size = cnf.get('benchmark', 'db_size')[:-1]

            self._loaded = True
            self._cnf_file = cnf_file
        except (NoSectionError, NoOptionError) as e:
            logging.error('Invalid config file: %s. %s', cnf_file, e)
            self.__init__()
            prefix, extension = os.path.splitext(cnf_file)
            os.rename(cnf_file, prefix + '.bad')
            return False

        return True

    def __str__(self):
        return str(OrderedDict({
            'config_file': self._cnf_file,
            'sysbench_threads': self._sysbench_threads,
            'db_inst_num': self._db_inst_num,
            'benchmark_target': self._benchmark_target,
            'benchmark_type': self._benchmark_type,
            'benchmark_time': self._benchmark_time,
            'db_size': self._db_size,
            'db_ip': self._db_ip,
            'db_port': self._db_port,
            'db_user': self._db_user,
            'client_ip': self._client_ip,
            'client_port': self._client_port,
            'client_user': self._client_user,
            'Loaded?': self._loaded
        }))


class Sweep:
    def __init__(self, bmk_cfg):
        self._bmk_config = bmk_cfg
        self._current_cfg_file = ''
        self._current_status = 'Idle'
        self._result_path = None
        self._ssh_key = '/root/.ssh/id_rsa'
        self._matrix = {
            '84': ['make_clean_84G_db', '350'],
            '75': ['make_clean_75G_db', '310'],
            '11': ['make_clean_11G_db', '44'],
            '9': ['make_clean_9G_db', '38']
        }
        self._benchmark_script = {
            'RW': 'run-sysbench',
            'RO': 'run-readonly',
            'WO': 'run-writeonly'
        }

    @property
    def current_config(self):
        return self._bmk_config

    @property
    def current_cfg_file(self):
        return self._current_cfg_file

    @property
    def current_status(self):
        return self._current_status

    @property
    def result_path(self):
        return self._result_path

    @property
    def ssh_key(self):
        return self._ssh_key

    def run(self):
        if self._bmk_config is None:
            logging.error('Sweep cannot run before loading the config!')
            return

        self._current_status = 'Running'
        logging.info('\n-----------------Another Sweep Started-----------------\n')

        for threads in self._bmk_config.sysbench_threads:
            logging.info('----Benchmark with threads: %s', str(threads))
            self._clean_client()
            self._clean_db()
            self._run_single(threads)

        logging.info('\n---------------------End of Sweep----------------------\n')

    def _ssh_run(self, ip, port, user, cmd):  # TODO: exception handling
        conn = paramiko.SSHClient()
        conn.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn.connect(ip, port, user, key_filename=self.ssh_key)
        try:
            logging.info('Command issued[%s]: %s', ip, cmd)
            # stdin, stdout, stderr = conn.exec_command(cmd)
            # TODO: change back to the real command
            stdin, stdout, stderr = conn.exec_command('uptime')
            err, out = stderr.readlines(), stdout.readlines()
            logging.info('Return Code: %s', str(err))
            logging.info('Command Output: %s', ''.join(out))
        finally:
            conn.close()

        return err, out

    def _db_cmd(self, cmd):
        return self._ssh_run(self._bmk_config.db_ip, self._bmk_config.db_port, self._bmk_config.db_user, cmd)

    def _client_cmd(self, cmd):
        return self._ssh_run(self._bmk_config.client_ip, self._bmk_config.client_port, self._bmk_config.client_user,
                             cmd)

    def _clean_db(self):
        logging.info('Running database housekeeping scripts...')

        clean_script_name = self._matrix[self._bmk_config.db_size][0]
        cmd_str = "pkill -9 'mysqld|mysqld_safe|bfcs|mysqladmin|%s|tar|gzip'" % clean_script_name
        logging.info('Killing database processes.')

        self._db_cmd(cmd_str)
        time.sleep(5)
        self._db_cmd("ps aux|grep mysqld")

        cmd_str = "/root/%s %s" % (clean_script_name, str(self._bmk_config.db_inst_num))
        logging.info('Rebuilding the database.')
        self._db_cmd(cmd_str)

        logging.info('Starting MySQL instances on database server')
        self._db_cmd("mysqld_multi start " + ','.join([str(x) for x in range(1, self._bmk_config.db_inst_num + 1)]))

        logging.info('Sleep for 240s...')
        time.sleep(240)

        while True:
            time.sleep(5)
            logging.info('Checking database logs...')
            ret = self._db_cmd("tail /var/lib/mysql/mysql*/*err | grep 'ready for connections' |wc -l")
            # TODO: change back:  if(ret[1] == [str(self._bmk_config.db_inst_num)+'\n']):
            break

        logging.info('Database ready.')

    def _clean_client(self):
        logging.info('Running client housekeeping scripts.')
        self._client_cmd(
            "pkill -9 'iostat|mpstat|vmstat|tdctl|sysbench|run-readonly|run-sysbench|run-writeonly|mysqladmin'")

        time.sleep(5)
        logging.info('Client ready.')

    def _run_single(self, threads):
        benchmark_script = self._benchmark_script[self._bmk_config.benchmark_type]
        db_inst_num = str(self._bmk_config.db_inst_num)
        table_num = self._matrix[self._bmk_config.db_size][1]
        benchmark_time = self._bmk_config.benchmark_duration
        benchmark_target = self._bmk_config.benchmark_target
        benchmark_type = self._bmk_config.benchmark_type
        db_size = self._bmk_config.db_size

        sysbench_cmd = u"/root/{0:s} -H 172.16.80.49 -N {1:s} -T {2:s} -S 1000000 -t {3:s} -r {4:s} -d /root" \
                       u" -s {5:s}_{6:s}_{7:s}G_{8:s} -Z -z -D -G {9:s} -R uniform" \
            .format(benchmark_script, db_inst_num, table_num, str(threads), str(benchmark_time), benchmark_target,
                    benchmark_type, db_size, str(threads), db_size)

        logging.info('Start to run a single point test')
        self._client_cmd(sysbench_cmd)
        logging.info('Script run-readonly finished.')

        while True:
            ret = self._client_cmd("pgrep run-readonly")

            # TODO: change back: if(ret[1] == []):
            break
            time.sleep(5)  # TODO: change back to 60s

        logging.info('Done. END OF THE BENCHMARK.')


class Bmk:
    def __init__(self):
        self._current_sweep = 0
        self._current_status = 'Idle'
        self._pid_file = PID_FILE
        self._log_file = '/root/auto/bmk.log'
        self._checking_interval = 60

    @property
    def current_sweep(self):
        return self._current_sweep

    @property
    def current_status(self):
        return self._current_status

    @property
    def pid_file(self):
        return self._pid_file

    @property
    def log_file(self):
        return self._log_file

    @property
    def checking_interval(self):
        return self._checking_interval

    @staticmethod
    def find_next_cnf(start, suffix):
        for relpath, dirs, files in os.walk(start):
            for file in files:
                if file.endswith(suffix):
                    full_path = os.path.join(start, relpath, file)
                    return os.path.normpath(os.path.abspath(full_path))

    def daemonize(self):
        if os.path.exists(self.pid_file):
            raise RuntimeError('Already up. To restart it, run stop first or remove {} in case of being '
                               'killed previously'
                               .format(self.pid_file)
                               )

        try:
            if os.fork() > 0:
                raise SystemExit(0)
        except OSError as e:
            raise RuntimeError('Fork #1 failed.')

        os.chdir('/')
        os.umask(0)
        os.setsid()

        try:
            if os.fork() > 0:
                raise SystemExit(0)
        except OSError as e:
            raise RuntimeError('Fork #2 failed.')

        sys.stdout.flush()
        sys.stderr.flush()

        with open(self.log_file, 'rb', 0) as f:
            os.dup2(f.fileno(), sys.stdin.fileno())
        with open(self.log_file, 'ab', 0) as f:
            os.dup2(f.fileno(), sys.stdout.fileno())
        with open(self.log_file, 'ab', 0) as f:
            os.dup2(f.fileno(), sys.stderr.fileno())

        with open(self.pid_file, 'w') as f:
            print(os.getpid(), file=f)

        atexit.register(lambda: os.remove(self.pid_file))

        # TODO: not finished!
        def sigterm_handler(signo, frame):
            logging.info('Dmk daemon pid {} stopped by user. Kill sysbench and make_clean* processes by yourself'
                         .format(os.getpid()))
            raise SystemExit(1)

        signal.signal(signal.SIGTERM, sigterm_handler)

    def loop(self):
        logging.info('Bmk daemon started with pid {}, checking jobs for every {} seconds'
                     .format(os.getpid(), self.checking_interval))

        while True:
            next_cnf = self.find_next_cnf('/root/auto/jobs', '.cnf')

            if next_cnf is not None:
                logging.info('Found: %s' % next_cnf)
                cfg = BmkConfig()
                if cfg.load_config(next_cnf):
                    pre, ext = os.path.splitext(next_cnf)
                    os.rename(next_cnf, pre + '.running')
                    sweep = Sweep(cfg)
                    self._current_sweep = sweep
                    self._current_status = 'SweepRunning'
                    sweep.run()
                    # After that we can append that sweep to a list,
                    # and there should be another process to handle the plot stuff..
                    os.rename(pre + '.running', pre + '.done')
                    self._current_status = 'SweepDone'

            time.sleep(self.checking_interval)  # Sleep for a while and check new jobs periodically.

    @staticmethod
    def get_pid():
        if os.path.exists(PID_FILE):
            try:
                with open(PID_FILE) as f:
                    pid = int(f.read())
                    proc_name = psutil.Process(pid).cmdline()[1]
                    if APP_NAME == os.path.basename(proc_name):
                        return pid
                    else:
                        logging.error('Error in stopping or checking status of bmk: pid={} but procname={}.'
                                      .format(pid, proc_name))
                        logging.error('Bmk may be killed previously, delete the /tmp/bmk.pid and check process list.')
                        return None

            except (ValueError, psutil.NoSuchProcess, psutil.ZombieProcess, psutil.AccessDenied):
                logging.error('Invalid pid in {}.'.format(PID_FILE))
                return None
        else:
            # No bmk process found
            return None

    @staticmethod
    def ssh_keybased_setup(hosta, hostb):
        '''
        Setup key-based login from usera@hosta to userb@hostb, but not vise versa.
        hosta|hostb is a list with the definition as follows:
        [ip_address, user_name, password]
        '''
        hosta_user = hosta[0]
        hosta_ip = hosta[1]
        hosta_pwd = hosta[2]

        hostb_user = hostb[0]
        hostb_ip = hostb[1]
        hostb_pwd = hostb[2]

        try:
            # Generate the public-private key pair if not exist. The option StrictHostKeyChecking=no is used
            # to avoid the strict key checking.
            cmd = 'ssh -o StrictHostKeyChecking=no {}@{} ssh-keygen -t rsa'.format(hosta_user, hosta_ip)
            logging.info('Creating key pair for {}, command: {}'.format(hosta_ip, cmd))
            child = pexpect.spawnu(cmd)
            #child.logfile = sys.stdout
            i = child.expect(['password:\s$', 'Enter file in which to save the key'])
            logging.info('Expect found: {} || {}'.format(child.before, child.after))

            if i == 0:
                logging.info('Password required from localhost to host: {}'.format(hosta_ip))
                child.sendline(hosta_pwd)
                child.expect('Enter file in which to save the key')
                logging.info('Expect found: {} || {}'.format(child.before, child.after))

            # Use the default file path.
            child.sendline('')
            i = child.expect(['Enter passphrase*', 'Overwrite'])
            if i == 0:
                logging.info('No existing key file found, generating a new one.')
                child.sendline('')
                child.expect('Enter same passphrase*')
                logging.info('Expect found: {} || {}'.format(child.before, child.after))
                child.sendline('')

            else:
                logging.info('Existing key file found.')
                child.sendline('n')
        except pexpect.TIMEOUT:
            logging.error('Timeout after 30s. Please check your network.')
            raise SystemExit(1)

        logging.info('Now copying the public key from {} to {}'.format(hosta_ip, hostb_ip))
        conn_a = paramiko.SSHClient()
        conn_a.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn_a.connect(hosta_ip, 22, hosta_user, hosta_pwd)

        conn_b = paramiko.SSHClient()
        conn_b.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        conn_b.connect(hostb_ip, 22, hostb_user, hostb_pwd)

        try:
            copy_id = 'ssh-copy-id -o StrictHostKeyChecking=no {}@{}'.format(hostb_user, hostb_ip)
            logging.info('Copying with: {}'.format(copy_id))

            chan = conn_a.invoke_shell()
            chan.send('{0}\n'.format(copy_id))
            buff = ''
            skip_copy = False
            while not buff.endswith('password: '):
                resp = chan.recv(9999)
                logging.info(resp)
                buff += resp.decode("utf-8")
                if 'they already exist on the remote system.' in buff:
                    skip_copy = True
                    break

            if not skip_copy:
                logging.info('Sending password of host: {}'.format(hostb_ip))
                chan.send('{0}\n'.format(hostb_pwd))
                buff = ''
                while 'Number of key(s) added: 1' not in buff:
                    resp = chan.recv(9999)
                    logging.info(resp)
                    buff += resp.decode("utf-8")

            logging.info('Testing if it works.')
            test_cmd = 'ssh -o StrictHostKeyChecking=no {}@{} hostname -I'.format(hosta_user, hosta_ip)
            stdin, stdout, stderr = conn_a.exec_command(test_cmd)
            err, out = stderr.readlines(), stdout.readlines()
            logging.info('Test Command RetCode: {}'.format(stderr))
            logging.info('Test Command Output: {}'.format(''.join(out)))
            if hostb_ip == out[0].strip():
                logging.info('Done.')
            else:
                logging.error('Hmm. Something is wrong, check the log please.')
        finally:
            conn_a.close()
            conn_b.close()


if __name__ == '__main__':
    SSH_KEY = '/root/.ssh/id_rsa'
    PID_FILE = '/tmp/bmk.pid'
    APP_NAME = 'bmk'

    if len(sys.argv) < 2:
        print('Usage: {} [start|stop|status|util]'.format(sys.argv[0]), file=sys.stderr)
        raise SystemExit(1)

    if sys.argv[1] == 'start':
        logging.basicConfig(
            filename='bmk.log',
            level=logging.INFO,
            format='%(levelname)s:%(asctime)s:%(message)s'
        )

        try:
            bmk = Bmk()
            bmk.daemonize()
        except RuntimeError as e:
            logging.error(e)
            raise SystemExit(1)
        bmk.loop()

    elif sys.argv[1] == 'stop':
        logging.basicConfig(
            filename='bmk.log',
            level=logging.INFO,
            format='%(levelname)s:%(asctime)s:%(message)s'
        )

        pid = Bmk.get_pid()

        if pid is not None:
            os.kill(pid, signal.SIGTERM)  # TODO: check return value
            logging.info('Damon bmk with pid={} stopped'.format(pid))
            print('Damon bmk with pid={} stopped'.format(pid), file=sys.stdout)

        else:
            print('Not running, no stop needed.', file=sys.stderr)
            raise SystemExit(1)

    elif sys.argv[1] == 'status':
        pid = Bmk.get_pid()
        if pid is not None:
            print('Daemon bmk is running, pid={}'.format(pid), file=sys.stdout)
        else:
            print('Daemon bmk is not running', file=sys.stdout)

    elif sys.argv[1] == 'util':
        if len(sys.argv) == 5 and sys.argv[2] == '-s':
            logging.basicConfig(
                filename='/dev/stdout',
                level=logging.INFO,
                format='%(levelname)s:%(asctime)s:%(message)s'
            )

            hosta = sys.argv[3].split('@')
            hostb = sys.argv[4].split('@')

            pwd = getpass.getpass('Password of {}@{}: '.format(hosta[0], hosta[1]))
            hosta.append(pwd)
            pwd = getpass.getpass('Password of {}@{}: '.format(hostb[0], hostb[1]))
            hostb.append(pwd)

            Bmk.ssh_keybased_setup(hosta, hostb)
        else:
            print('Invalid option for util.')

    else:
        print('Unknown or invalid option: {!r}'.format(sys.argv[1]), file=sys.stderr)
        raise SystemExit(1)
