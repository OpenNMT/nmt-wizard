import time
import json
import re
import logging
import six

import paramiko

logger = logging.getLogger(__name__)

def add_log_handler(fh):
    logger.addHandler(fh)

def run_command(client, cmd, stdin_content=None, sudo=False):
    if sudo:
        cmd = "sudo " + cmd
    logger.debug("RUN %s", cmd)
    stdin, stdout, stderr = client.exec_command(cmd)
    if stdin_content is not None:
        stdin.write(stdin_content)
        stdin.flush()
    exit_status = stdout.channel.recv_exit_status()
    return exit_status, stdout, stderr

def run_and_check_command(client, cmd, stdin_content=None, sudo=False):
    exit_status, _, _ = run_command(
        client, cmd, stdin_content=stdin_content, sudo=sudo)
    return exit_status == 0

def program_exists(client, program):
    return run_and_check_command(client, "command -v %s" % program)

def has_gpu_support(client):
    return run_and_check_command(client, "nvidia-smi")

def ssh_connect_with_retry(client,
                           hostname,
                           username,
                           key_path,
                           delay=0,
                           retry=0,
                           login_cmd=None):
    """Wrap the SSH connect method with a delay and retry mechanism. This is
    useful when connecting to an instance that was freshly started.
    """
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    while True:
        if delay > 0:
            time.sleep(delay)
        try:
            client.load_system_host_keys()
            logger.info("Connecting to %s via SSH...", hostname)
            client.connect(
                hostname,
                username=username,
                key_filename=key_path,
                look_for_keys=False)
            if login_cmd is not None:
                if not run_and_check_command(client, login_cmd):
                    raise RuntimeError("failed to run login command")
            return client
        except Exception as e:
            retry -= 1
            if retry < 0:
                raise e
            else:
                logger.warning("Failed to connect to %s via SSH (%s), retrying...",
                               hostname, str(e))

def fuse_s3_bucket(client, corpus):
    if not program_exists(client, "s3fs"):
        raise EnvironmentError("s3fs is not installed")
    if not run_and_check_command(
            client,
            "sed 's/# *user_allow_other/user_allow_other/' -i /etc/fuse.conf",
            sudo=True):
        raise RuntimeError("failed to configure s3fs")
    status, _, stderr = run_command(client, "mkdir -p %s && chmod -R 775 %s" % (
        corpus["mount"], corpus["mount"]))
    if status != 0:
        return RuntimeError('failed to created mount directory: %s' % stderr.read())
    status, _, stderr = run_command(client, "echo %s:%s > s3_passwd && chmod 600 s3_passwd" % (
        corpus["credentials"]["AWS_ACCESS_KEY_ID"],
        corpus["credentials"]["AWS_SECRET_ACCESS_KEY"]))
    if status != 0:
        raise RuntimeError('failed to store S3 credentials: %s' % stderr.read())
    status, _, stderr = run_command(
        client, "s3fs %s %s -o allow_other -o passwd_file=s3_passwd" % (
            corpus["bucket"], corpus["mount"]))
    if status != 0:
        raise RuntimeError('failed to fuse S3 bucket: %s' % stderr.read())

def check_environment(client, gpu_id, log_dir, docker_registries):
    """Check that the environment contains all the tools necessary to launch a task
    """
    for registry in six.itervalues(docker_registries):
        if registry['type'] == 'aws' and not program_exists(client, 'aws'):
            raise EnvironmentError("missing aws client")

    # check log_dir
    if not run_and_check_command(client, "test -d '%s'" % log_dir):
        raise EnvironmentError("incorrect log directory: %s" % log_dir)

    if gpu_id == 0:
        if not program_exists(client, "docker"):
            raise EnvironmentError("docker not available")
        return ''
    else:
        if not program_exists(client, "nvidia-docker"):
            raise EnvironmentError("nvidia-docker not available")

        exit_status, stdout, stderr = run_command(
            client, 'nvidia-smi -q -i %d -d UTILIZATION,MEMORY' % (gpu_id - 1))
        if exit_status != 0:
            raise EnvironmentError("nvidia-smi exited with status %d: %s" % (
                exit_status, stderr.read()))

        out = stdout.read()
        gpu = '?'
        mem = '?'
        m = re.search(b'Gpu *: (.*) *\n', out)
        if m:
            gpu = m.group(1).decode('utf-8')
        m = re.search(b'Free *: (.*) *\n', out)
        if m:
            mem = m.group(1).decode('utf-8')

        return 'gpu usage: %s, free mem: %s' % (gpu, mem)

def cmd_connect_private_registry(docker_registry):
    if docker_registry['type'] == "aws":
        return ('$(AWS_ACCESS_KEY_ID=%s AWS_SECRET_ACCESS_KEY=%s '
                'aws ecr get-login --no-include-email --region %s)') % (
                    docker_registry['credentials']['AWS_ACCESS_KEY_ID'],
                    docker_registry['credentials']['AWS_SECRET_ACCESS_KEY'],
                    docker_registry['region'])
    username = docker_registry['credentials']['username']
    password = docker_registry['credentials']['password']
    return ('docker login --username %s --password %s') % (username, password)

def cmd_docker_pull(image_ref, docker_path=None):
    path = ""
    if docker_path is not None:
        path = docker_path + "/"
    return '%sdocker pull %s' % (path, image_ref)

def _protect_arg(arg):
    return "'" + re.sub(r"(')", r"\\\1", arg.strip()) + "'"

def cmd_docker_run(gpu_id, docker_options, task_id,
                   image_ref, callback_url, callback_interval,
                   storages, docker_command, log_dir=None):
    if docker_options.get('dev') == 1:
        return "sleep 35"
    else:
        docker_cmd = 'docker' if gpu_id == 0 else 'nvidia-docker'
        docker_path = docker_options.get('path')
        if docker_path:
            docker_cmd = docker_path +'/' + docker_cmd

        # launch the task
        cmd = '%s run -i --rm' % docker_cmd
        if 'mount' in docker_options:
            for k in docker_options['mount']:
                cmd += ' -v %s' % k
        if 'envvar' in docker_options:
            for k in docker_options['envvar']:
                cmd += ' -e %s=%s' % (k, docker_options['envvar'][k])

        # mount TMP_DIR used to store potential transfered files
        cmd += ' -e TMP_DIR=/root/tmp/%s' % task_id

        cmd += ' %s' % image_ref

        if storages is not None and storages != {}:
            v = json.dumps(storages)
            v = v.replace("<TASK_ID>", task_id)
            v = v.replace("<CALLBACK_URL>", callback_url)
            cmd += ' -s \'%s\'' % v

            # if model storage is not specified, check if there is a default
            # model storage
            if '-ms' not in docker_command:
                for s in storages:
                    if storages[s].get('default_ms'):
                        docker_command = ['-ms', s + ':'] + docker_command
                        break

        cmd += ' -g %s' % gpu_id
        cmd += ' -t %s' % task_id
        if callback_url is not None and callback_url != '':
            cmd += ' -b \'%s\'' % callback_url
            if callback_interval is not None:
                cmd += ' -bi %d' % callback_interval

        cmd += ' -i %s' % image_ref

        for arg in docker_command:
            if arg.startswith('${TMP_DIR}'):
                arg = '/root/tmp/%s%s' % (task_id, arg[10:])
            cmd += ' ' + _protect_arg(arg)

        if log_dir is not None:
            cmd += ' > %s/\"%s.log\" 2>&1' % (log_dir, task_id)

        return cmd

def launch_task(task_id,
                client,
                gpu_id,
                log_dir,
                docker_options,
                the_docker_registry,
                docker_image,
                docker_tag,
                docker_command,
                docker_files,
                wait_for_immediate_failure=2,
                storages=None,
                callback_url=None,
                callback_interval=None):
    """Launch a task:
        * `task_id`: assigned id for the task and used for logging
        * `client`: ssh client
        * `gpu_id`: if > 0, the id (eventually ids) of the GPU to use on the host
        * `log_dir`: host docker log
        * `docker_options`: environment and mounting points
        * `docker_image`: image name of the docker
        * `docker_tag`: tag - by default latest
        * `docker_command`: the actual command to launch
        * `wait_for_immediate_failure`: time to wait and monitor launched process
        * `storages`: dictionary of storage to use in the docker execution
        * `callback_url`: server to callback for beat of activity
        * `callback_interval`: time between 2 beats
    """
    image_ref = ""
    if docker_options.get('dev') != 1:
        docker_registry = docker_options['registries'][the_docker_registry]

        registry_uri = docker_registry['uri']

        # connect to a registry
        if docker_registry['type'] != 'dockerhub':
            exit_status, stdout, stderr = run_command(
                client,
                cmd_connect_private_registry(docker_registry)
            )
            if exit_status != 0:
                raise RuntimeError("cannot connect to private registry: %s" % stderr.read())

        # pull the docker image
        registry_urip = '' if registry_uri == '' else registry_uri + '/'
        image_ref = '%s%s:%s' % (registry_urip, docker_image, docker_tag)
        logger.debug("pulling docker image: %s - %s", docker_registry['type'], docker_image)
        docker_cmd = cmd_docker_pull(image_ref, docker_path=docker_options.get('path'))
        exit_status, stdout, stderr = run_command(client, docker_cmd)
        if exit_status != 0:
            raise RuntimeError("error pulling the image %s: %s" % (image_ref, stderr.read()))

    if len(docker_files):
        # we have files to synchronize locally
        assert 'mount' in docker_options, "mount point should be defined for passing files"
        assert callback_url is not None, "callback_url needed for passing files"
        mount_tmpdir = None
        for m in docker_options['mount']:
            if m.endswith('/root/tmp'):
                mount_tmpdir = m[:-10]
                break
        assert mount_tmpdir is not None, "mount points need to include /root/tmp for passing files"
        cmd_mkdir = "mkdir -p %s/%s" % (mount_tmpdir, task_id)
        exit_status, stdout, stderr = run_command(client, cmd_mkdir)
        if exit_status != 0:
            raise RuntimeError("error build task tmp dir: %s, %s" % (cmd_mkdir, stderr.read()))
        for f in docker_files:
            logger.info("retrieve file %s -> %s/%s", f, mount_tmpdir, task_id)
            cmd_get_files = 'curl "%s/file/%s/%s" > %s/%s/%s' % (
                callback_url,
                task_id,
                f,
                mount_tmpdir,
                task_id,
                f)
            exit_status, stdout, stderr = run_command(client, cmd_get_files)
            if exit_status != 0:
                raise RuntimeError("error retrieving files: %s, %s" % (cmd_get_files, stderr.read()))

    cmd = 'nohup ' + cmd_docker_run(gpu_id, docker_options, task_id,
                                    image_ref, callback_url, callback_interval,
                                    storages, docker_command, log_dir)
    log_file = "%s/%s.log" % (log_dir, task_id)
    if callback_url is not None:
        cmd = '(%s ; status=$?' % cmd
        if log_dir is not None and log_dir != '':
            cmd = '%s ; curl -X POST "%s/file/%s/log" --data-binary "@%s"' % (
                cmd,
                callback_url,
                task_id,
                log_file)
        cmd =  ('%s ; if [[ $status = 0 ]]; then curl -X GET "%s/terminate/%s?phase=completed";' +
                ' else curl -X GET "%s/terminate/%s?phase=error"; fi )') % (
            cmd, callback_url, task_id, callback_url, task_id)

    # get the process group id
    cmd += ' & ps -o pgid -p $!'

    exit_status, stdout, stderr = run_command(client, cmd)
    if exit_status != 0:
        raise RuntimeError("%s run failed: %s" % (cmd, stderr.read()))

    # read ps header
    outpgid = stdout.readline()
    # read pgid
    outpgid = stdout.readline()
    m = re.search(r'(\d+)', outpgid)
    if not m:
        raise RuntimeError("cannot get PGID")
    pgid = int(m.group(1))
    logger.info("Process launched with pgid %d.", pgid)

    # check what is happening 1s later - just to check immediate failure
    if wait_for_immediate_failure > 0:
        logger.info("Wait for %d seconds and check process status.", wait_for_immediate_failure)
        time.sleep(wait_for_immediate_failure)
        if not run_and_check_command(client, 'kill -0 -%d' % pgid):
            _, stdout, stderr = run_command(client, 'cat %s' % log_file)
            raise RuntimeError("process exited early: %s" % stdout.read())

    return {"model": task_id, "pgid": pgid}
