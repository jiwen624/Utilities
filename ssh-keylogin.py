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
