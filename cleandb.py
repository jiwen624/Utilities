#!/usr/bin/env python3
"""Clean the database server environment and setup a new one
    - kill all the database processes
    - remove old database files
    - create new databases from the zipped archive

    Author: Jiwen (Eric) Yang
    Date: June 18, 2016
"""

import psutil
import os
import sys
import shutil
import re
import time
import argparse
import logging
import configparser
from subprocess import Popen, PIPE, STDOUT, check_output

log = logging.getLogger(__name__)
CleanDbFatalError = RuntimeError


def kill_proc(proc_names, skip_pids=None):
    """
    Kill a process by name.
    :param proc_names:
    :param skip_pids:
    :return:
    """
    assert proc_names is not None
    if skip_pids is None:
        skip_pids = []

    for proc in psutil.process_iter():
        # if proc.name() == proc_name and proc.pid != skip_pid:
        #     log.debug('Kill process: {} pid={}'.format(proc.name(), proc.pid))
        #     pids.append(proc.pid)
        #     proc.kill()
        if proc.pid not in skip_pids:
            full_cmd = ' '.join(proc.cmdline())
            for name in proc_names:
                if name in full_cmd:
                    log.info('Cleaning: {} pid={}'.format(full_cmd, proc.pid))
                    proc.kill()
                    break


def remove_db(basedir):
    """
    Remove all the database files under 'base_dir'
    :param basedir:
    :return:
    """
    assert basedir is not None

    log.info('Removing mysql datafiles and directories under {}'.format(basedir))
    pattern = r'^mysql\d+$'
    for file in os.listdir(basedir):
        abspath = os.path.join(basedir, file)
        if os.path.isdir(abspath) and re.match(pattern, file):
            log.debug('Remove directory: {}'.format(abspath))
            shutil.rmtree(abspath)


def create_db(basedir, dbnum, tar_file, strips):
    """
    Create database directories and untar databases to them.
    :param strips:
    :param basedir:
    :param dbnum:
    :param tar_file:
    :return:
    """
    assert basedir and tar_file and dbnum

    for i in range(1, dbnum + 1):
        abspath = os.path.join(basedir, 'mysql{}'.format(i))
        log.debug('Creating directory: {}'.format(abspath))
        try:
            os.mkdir(abspath)
        except FileExistsError:
            pass

    log.info('Untar database 1-{} to {} from {}. This is SLOW.'.format(dbnum, db_dir, tar_file))
    start = time.time()
    # untar_cmd = 'tar zxf {} --strip-components 1 -C {}/mysql{}'
    untar_cmd = 'tar -xf {tarball} --use-compress-program=pigz ' \
                '--strip-components {strips} -C {base_dir}/mysql{i}'
    running_procs = [Popen(untar_cmd.format(tarball=tar_file, base_dir=db_dir, strips=strips, i=i),
                           shell=True, stdout=PIPE, stderr=PIPE)
                     for i in range(1, dbnum + 1)]
    while running_procs:
        for proc in running_procs:
            retcode = proc.poll()
            if retcode is not None:  # Process finished.
                results, errors = proc.communicate()

                if retcode != 0:
                    log.error('Untar failed: ({}) {}'.format(retcode, results.decode('utf-8')))
                    raise CleanDbFatalError(errors.decode('utf-8'))
                else:
                    elapsed = int(time.time() - start)
                    log.info('Done: ({} seconds used)'.format(elapsed))

                running_procs.remove(proc)
                break  # This just breaks out of the for loop, not the while.
            else:  # No process is done, wait a bit longer and check again.
                time.sleep(10)
                continue

    for i in range(1, dbnum + 1):
        abspath = os.path.join(basedir, 'mysql{}'.format(i))
        log.debug('Changing the owner of {} to mysql:mysql'.format(abspath))
        try:
            shutil.chown(abspath, user='mysql', group='mysql')
        except PermissionError as e:
            log.error(e)
            raise CleanDbFatalError('Failed to change owner of {} to mysql'.format(abspath))

    log.info('Finished to prepare the databases.')


def prepare_db(basedir, dbnum, tar_file, tar_strips, opt=None):
    """
    Clean up the database environment and setup a new one.
    :param tar_strips:
    :param basedir:
    :param dbnum:
    :param opt:
    :param tar_file:
    :return:
    """
    assert dbnum is not None
    if not opt:
        opt = []

    skip_pids = [os.getpid(), os.getppid()]
    log.info('Cleaning process leftovers (skipping myself with pid and ppid: {})'.format(skip_pids))

    _, exec_file = os.path.split(__file__)
    to_be_killed = ['mysqld',
                    'tar ',
                    'pigz ',
                    'tdctl',
                    'mpstat',
                    'vmstat',
                    'iostat',
                    'barf',
                    'monitor',
                    'show engine innodb status',
                    exec_file]

    kill_proc(to_be_killed, skip_pids)

    if 'skip_db_recreation' in opt:
        #  An error will be throwed out if skip_db_recreation is specified
        # but there was not so many instances created in the previous benchmark.
        for i in range(1, int(dbnum) + 1):
            db_dir = os.path.join(basedir, 'mysql{}'.format(i))
            if not os.path.isdir(db_dir):
                log.error('***skip_db_recreation detected but db in {} is not there.***'.format(db_dir))
                raise CleanDbFatalError('Invalid option, see above error print')

        log.info('Skipping database recreation but waiting 60 seconds before restarting the instances.')
        time.sleep(60)  # wait for mysqld zombie process to quit completely
    else:
        log.info('Removing database.')
        remove_db(basedir)
        log.info('Creating database which may take a few minutes. Please be patient.')
        create_db(basedir, dbnum, tar_file, tar_strips)


def start_db(dbnum):
    """
    Start the database instances and check the logs. This function should only be run after prepare_db()
    :param dbnum:
    :return:
    """
    assert dbnum is not None
    startdb_cmd = 'mysqld_multi start {}'.format(','.join([str(x) for x in range(1, dbnum + 1)]))
    log.info('Starting db: {}  (check MySQL logs for current progress.)'.format(startdb_cmd))
    Popen(startdb_cmd, shell=True, stdout=PIPE, stderr=STDOUT)

    # Check if the databases have been up and running, wait for 200*5 seconds
    started = ''
    for _ in range(200):
        started = check_output("mysqld_multi report | grep 'is running' | awk '{print$5}'", shell=True)
        started = started.decode('utf-8').replace('\n', ' ')
        log.debug('Started instances: {}'.format(started))
        if len(started.split()) >= db_num:
            break
        time.sleep(10)

    started = started.replace('\n', ' ')
    log.info('Started: {}'.format(started))
    if len(started.split()) < dbnum:
        log.error('Failed to start all the databases after 2000 seconds')
        raise CleanDbFatalError


def parse_args():
    """
    Parse command line parameters
    :return:
    """
    parser = argparse.ArgumentParser(
        description="The program to prepare database environment for sysbench test.")

    parser.add_argument("num", help="the number of database instances", type=int)

    parser.add_argument("-v", help="detailed print( -v: info, -vv: debug)",
                        action='count', default=0)
    parser.add_argument("-d", help="the MySQL base directory", default='/var/lib/mysql')
    parser.add_argument("-s", help="--strip-components", default='1')
    parser.add_argument("-z", help="the path of database backup file")
    parser.add_argument("-p",
                        help="parameters(no spaces before and after =): "
                             "'track_active=\"38\" mysql_innodb_buffer_pool_size=\"10240M\"' ",
                        default='')
    parser.add_argument("-o", nargs='*', help="supported options: skip_db_recreation")
    parser.add_argument("-n", help="the sweep log directory", default='/tmp')

    args = parser.parse_args()

    sys_parms = dict(item.split('=') for item in args.p.split())

    log.info('Found options: {}'.format(args.o))
    return args.v, args.d, args.num, sys_parms, args.o, args.z, args.s, args.n


def set_track_active(args):
    """
    Set track active (if track_active == 0, disable DMX)
    lsmod | awk '{print $1}'| grep bf
    :param args:
    :return:
    """
    assert args is not None
    # Check if dmx is running.
    bf_mod = check_output("lsmod | awk '{print $1}'| grep bf", shell=True).decode('utf-8').rstrip()
    if bf_mod != 'bf':
        log.error('bf module is not loaded: {} - try load-driver and start-bf.'.format(bf_mod))
        raise CleanDbFatalError('Seems that the bf is not loaded.')

    # Set track active and mysql config file
    track_active = args.get('track_active', '0')
    # log.info('Set track active to {}'.format(track_active))

    if track_active != '0':
        try:
            shutil.copy2('/dmx/etc/bfapp.d/bak.mysqld', '/dmx/etc/bfapp.d/mysqld')
        except (FileNotFoundError, FileExistsError):
            pass
        # Set track active
        set_ta_cmd = 'memcli process settings --set-max {}'.format(track_active)
        ret = check_output(set_ta_cmd, shell=True, stderr=STDOUT)
        log.info('Track active: {}'.format(ret.decode('utf-8').strip()))
    else:
        # 0 or None means DMX should be disabled, I'll remove /dmx/etc/bfapp.d/mysqld here.
        try:
            shutil.move('/dmx/etc/bfapp.d/mysqld', '/dmx/etc/bfapp.d/bak.mysqld')
        except FileNotFoundError:
            pass
        log.info('Set track active to 0, bfapp.d/mysqld gets renamed.')


def set_mysql_cnf(args):
    """
    Set my.cnf with args parameters
    :param args:
    :return:
    """
    conf = configparser.ConfigParser()
    log.info('Restore default MySQL config file from /etc/my.cnf.baseline to /etc/my.cnf')
    try:
        shutil.copy2('/etc/my.cnf.baseline', '/etc/my.cnf')
    except FileNotFoundError:
        log.error(e)
        raise CleanDbFatalError('The baseline config: /etc/my.cnf.baseline is not found.')

    log.info('Modifying my.cnf.')
    conf.read('/etc/my.cnf')

    assert args is not None
    prefix = 'mysql_'
    for key in args.keys():
        if key.startswith(prefix):
            real_key = key[len(prefix):]
            conf['mysqld'][real_key] = args[key]
            log.info('Changed my.cnf key {}={}'.format(real_key, args[key]))

    with open('/etc/my.cnf', 'w') as my_cnf:
        conf.write(my_cnf)


def prepare_sys(args, log_dir):
    """
    Prepare the system environment: DMX/RAM, my.cnf? track active, etc.
    {'track_active': '38',
     'mysql_innodb_buffer_pool_size': '102400M'
     }
    :param args:
    :return:
    """
    assert args is not None

    try:
        os.mkdir(log_dir)
        log.info('Directory {} created on db server as logs staging area.'.format(log_dir))
    except FileExistsError:
        pass

    set_track_active(args)
    set_mysql_cnf(args)


def trans_log_level(level_int=1):
    """
    Translate the log level from number of -v to enumerations
    :param level_int:
    :return:
    """
    if level_int == 0:
        level = logging.ERROR
    elif level_int == 1:
        level = logging.INFO
    else:
        level = logging.DEBUG
    return level


if __name__ == "__main__":
    try:
        log_level, db_dir, db_num, sys_args, opt, tarball, strips, log_dir = parse_args()
        # Set the log level of this module
        logging.basicConfig(level=trans_log_level(log_level), format='%(levelname)s: %(message)s')

        prepare_sys(sys_args, log_dir)
        prepare_db(db_dir, db_num, tarball, strips, opt)
        start_db(db_num)

        log.info('***Database is ready.***')
        sys.exit(0)
    except CleanDbFatalError as e:
        log.error(e)
        raise
        #  sys.exit(1)
