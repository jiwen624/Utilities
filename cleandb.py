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
import shutil
import re
import time
import argparse
import logging
import configparser
from subprocess import Popen, PIPE, STDOUT, check_output

log = logging.getLogger(__name__)


def kill_proc(proc_name, skip_pid=0):
    """
    Kill a process by name.
    :param proc_name:
    :param skip_pid:
    :return:
    """
    assert proc_name is not None

    pids = []
    for proc in psutil.process_iter():
        if proc.name() == proc_name and proc.pid != skip_pid:
            log.debug('Kill process: {} pid={}'.format(proc.name(), proc.pid))
            pids.append(proc.pid)
            proc.kill()

            # But the process may be restarted by a daemon.
            # time.sleep(5)
            # for pid in pids:
            #     if psutil.pid_exists(pid):
            #         raise RuntimeError('Failed to kill process {} with pid {}'.format(proc_name, pid))
            #


def remove_db(basedir):
    """
    Remove all the database files under 'base_dir'
    :param basedir:
    :return:
    """
    assert basedir is not None

    log.info('Removing mysql directories under {}'.format(basedir))
    pattern = r'^mysql\d+$'
    for file in os.listdir(basedir):
        abspath = os.path.join(basedir, file)
        if os.path.isdir(abspath) and re.match(pattern, file):
            log.debug('Remove directory: {}'.format(abspath))
            shutil.rmtree(abspath)


def create_db(basedir, dbnum, tarball):
    """
    Create database directories and untar databases to them.
    :param basedir:
    :param dbnum:
    :param tarball:
    :return:
    """
    assert basedir and tarball and dbnum

    for i in range(1, dbnum + 1):
        abspath = os.path.join(basedir, 'mysql{}'.format(i))
        log.debug('Creating directory: {}'.format(abspath))
        try:
            os.mkdir(abspath)
        except FileExistsError:
            pass

    log.info('Untar database 1-{} to {} from {}.'.format(dbnum, base_dir, tarball))
    # untar_cmd = 'tar zxf {} --strip-components 1 -C {}/mysql{}'
    untar_cmd = 'tar -xf {tarball} --use-compress-program=pigz --strip-components 1 -C {base_dir}/mysql{i}'
    running_procs = [Popen(untar_cmd.format(tarball=tarball, base_dir=base_dir, i=i),
                           shell=True, stdout=PIPE, stderr=PIPE)
                     for i in range(1, dbnum + 1)]
    while running_procs:
        for proc in running_procs:
            retcode = proc.poll()
            if retcode is not None:  # Process finished.
                results, errors = proc.communicate()

                if retcode != 0:
                    log.error('Untar failed: ({}) {}'.format(retcode, results.decode('utf-8')))
                    raise RuntimeError(errors)
                else:
                    log.info('Untar finished: (ret={}) {}'.format(retcode, results.decode('utf-8')))

                running_procs.remove(proc)
                break
            else:  # No process is done, wait a bit and check again.
                time.sleep(10)
                continue

    for i in range(1, dbnum + 1):
        abspath = os.path.join(basedir, 'mysql{}'.format(i))
        log.debug('Changing the owner of {} to mysql:mysql'.format(abspath))
        shutil.chown(abspath, user='mysql', group='mysql')

    log.info('Finished to prepare the database.')


def prepare_db(basedir, dbnum, tarball, options=None):
    """
    Clean up the database environment and setup a new one.
    :param basedir:
    :param dbnum:
    :param options:
    :param tarball:
    :return:
    """
    assert dbnum is not None
    if not options:
        options = []

    log.info('Killing processes: mysqld_safe, mysqld, mysql, mysqladmin, tdctl, cleandb.py')
    kill_proc('mysqld_safe')
    kill_proc('mysqld')
    kill_proc('mysql')
    kill_proc('mysqladmin')
    kill_proc('tar')
    kill_proc('pigz')
    kill_proc('tdctl')

    _, exec_file = os.path.split(__file__)
    self_pid = os.getpid()
    kill_proc(exec_file, self_pid)

    if 'skip_db_recreation' in options:
        log.info('Skipping database recreation but waiting 40 seconds before restart the instances.')
        time.sleep(40)  # wait for mysqld zombie process to quit completely
    else:
        log.info('Removing database.')
        remove_db(basedir)
        log.info('Creating database.')
        create_db(basedir, dbnum, tarball)


def start_db(dbnum):
    """
    Start the database instances and check the logs. This function should only be run after prepare_db()
    :param dbnum:
    :return:
    """
    assert dbnum is not None
    startdb_cmd = 'mysqld_multi start {}'.format(','.join([str(x) for x in range(1, dbnum + 1)]))
    log.info('Starting db: {}'.format(startdb_cmd))
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

    if len(started.split()) < dbnum:
        raise RuntimeError('Failed to start all the databases after 2000 seconds')


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
    parser.add_argument("-z", help="the path of database backup file")
    parser.add_argument("-p",
                        help="parameters(no spaces before and after =): "
                             "'track_active=\"38\" mysql_innodb_buffer_pool_size=\"10240M\"' ",
                        default='')
    parser.add_argument("-o", nargs='*', help="supported options: skip_db_recreation")

    args = parser.parse_args()

    sys_args = dict(item.split('=') for item in args.p.split())

    log.debug('Found options: {}'.format(args.o))
    return args.v, args.d, args.num, sys_args, args.o, args.z


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
        raise RuntimeError('Seems that the bf is not loaded.')

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
        log.debug('Set track active to {}'.format(ret.decode('utf-8').strip()))
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
    log.info('Modifying my.cnf.')
    conf = configparser.ConfigParser()
    try:
        shutil.copy2('/etc/my.cnf.bak', '/etc/my.cnf')
    except FileNotFoundError:
        pass

    conf.read('/etc/my.cnf')

    assert args is not None
    for key in args.keys():
        if key.startswith('mysql_'):
            real_key = key.lstrip('mysql_')
            conf['mysqld'][real_key] = args[key]
            log.debug('Changed my.cnf key {}={}'.format(real_key, args[key]))

    with open('/etc/my.cnf', 'w') as my_cnf:
        conf.write(my_cnf)


def prepare_sys(args):
    """
    Prepare the system environment: DMX/RAM, my.cnf? track active, etc.
    {'track_active': '38',
     'mysql_innodb_buffer_pool_size': '102400M'
     }
    :param args:
    :return:
    """
    assert args is not None
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
    log_level, base_dir, db_num, sys_args, options, tarball = parse_args()
    # Set the log level of this module
    logging.basicConfig(level=trans_log_level(log_level), format='%(levelname)s: %(message)s')

    prepare_sys(sys_args)
    prepare_db(base_dir, db_num, tarball, options)
    start_db(db_num)
