import logging
import os
import re

import paramiko

from nmtwizard import common
from nmtwizard.service import Service

logger = logging.getLogger(__name__)


def _get_params(config, options):
    server_cfg = config['variables']

    if 'master_node' not in server_cfg:
        raise ValueError('missing master_node in configuration')
    if 'log_dir' not in server_cfg:
        raise ValueError('missing log_dir in configuration')
    if 'torque_install_path' not in server_cfg:
        raise ValueError('missing torque_install_path in configuration')
    if 'login' not in server_cfg and 'login' not in options:
        raise ValueError('missing login in one of configuration and user options')
    if 'mem' not in options:
        raise ValueError('missing mem in user options')
    if 'priority' not in options:
        raise ValueError('missing priority in user options')

    params = {}
    params['master_node'] = server_cfg['master_node']
    params['login'] = server_cfg.get('login', options.get('login'))
    params['mem'] = options['mem']
    params['priority'] = options['priority']
    params['log_dir'] = server_cfg['log_dir']
    params['torque_install_path'] = server_cfg['torque_install_path']
    return params


class TorqueService(Service):

    def list_resources(self):
        return {'torque': self._config['maxInstance']}

    @staticmethod
    def get_resource_from_options(options):  # pylint: disable=unused-argument
        return "torque"

    def describe(self):
        desc = {}
        if 'login' not in self._config['variables']:
            desc['login'] = {
                "type": "string",
                "title": "login",
                "description": "login to use to access the server"
            }
        desc['mem'] = {
            "type": "integer",
            "default": 4,
            "title": "required memory (Gb)",
            "minimum": 1

        }
        desc['priority'] = {
            "type": "integer",
            "default": 0,
            "title": "Priority of the job",
            "minimum": -1024,
            "maximum": 1023
        }
        return desc

    def check(self, options):  # pylint: disable=arguments-differ
        params = _get_params(self._config, options)

        client = common.ssh_connect_with_retry(
            params['master_node'],
            params['login'],
            self._config['privateKey'])

        # check log_dir
        if not common.run_and_check_command(client, "test -d '%s'" % params['log_dir']):
            client.close()
            raise ValueError("incorrect log directory: %s" % params['log_dir'])

        status, stdout, _ = common.run_command(
            client, os.path.join(params['torque_install_path'], "qstat"))

        client.close()
        if status != 0:
            raise RuntimeError('qstat exited with code %s' % status)
        return "%s jobs(s) in the queue" % (len(stdout.read().split('\n')) - 2)

    # pylint: disable=unused-argument, arguments-differ
    def launch(self,
               task_id,
               options,
               gpulist,
               resource,
               storages,
               docker_config,
               docker_registry,
               docker_image,
               docker_tag,
               docker_command,
               docker_files,
               wait_after_launch):
        params = _get_params(self._config, options)

        client = common.ssh_connect_with_retry(
            params['master_node'],
            params['login'],
            self._config['privateKey'])

        cmd = "cat <<-'EOF'\n"
        cmd += "#!/bin/bash\n"
        cmd += "#PBS -l nodes=1:ppn=2:gpus=1,mem=%sG,walltime=10000:00:00\n" % params['mem']
        cmd += "#PBS -p %d\n" % params['priority']
        cmd += "#PBS -N infTraining\n"
        cmd += "#PBS -o %s/%s.log -j oe\n" % (params['log_dir'], task_id)

        cmd += "guessdevice(){\n"
        cmd += "    if [ -e \"${PBS_GPUFILE}\" ]\n"
        cmd += "    then\n"
        cmd += "        GPUS=`cat ${PBS_GPUFILE} | perl -pe 's/[^-]+-gpu//g' |"
        cmd += r" perl -pe 's/\s+/ /g' | perl -pe 's/,$//g'`\n"
        cmd += "        GPUS=`echo \"${GPUS}+1\" | bc `\n"
        cmd += "        echo $GPUS;\n"
        cmd += "    else\n"
        cmd += "        echo \"error: No available GPU\"\n"
        cmd += "    fi\n"
        cmd += "}\n"

        cmd += "DEVICE=$(guessdevice)\n"
        cmd += "echo \"RUN ON GPU ${DEVICE}\"\n"
        registry = docker_config['registries'][docker_registry]
        registry_uri = registry['uri']
        registry_urip = '' if registry_uri == '' else registry_uri + '/'
        image_ref = '%s%s:%s' % (registry_urip, docker_image, docker_tag)

        if registry['type'] != 'dockerhub':
            cmd_connect = common.cmd_connect_private_registry(registry)
            cmd += "echo '=> " + cmd_connect + "'\n"
            cmd += cmd_connect + '\n'

        cmd_docker_pull = common.cmd_docker_pull(image_ref)
        cmd += "echo '=> " + cmd_docker_pull + "'\n"
        cmd += cmd_docker_pull + '\n'
        docker_cmd = "echo | " + common.cmd_docker_run(
            "$DEVICE",
            docker_config,
            task_id,
            image_ref,
            storages,
            self._config['callback_url'],
            self._config['callback_interval'],
            docker_command)

        cmd += "echo \"=> " + docker_cmd.replace("\"", "\"") + "\"\n"
        cmd += docker_cmd + '\n'

        if self._config['callback_url']:
            callback_cmd = ''
            if params['log_dir'] is not None and params['log_dir'] != '':
                callback_cmd = 'curl -X POST "%s/log/%s" --data-binary "@%s/%s.log" ; ' % (
                    self._config['callback_url'],
                    task_id,
                    params['log_dir'],
                    task_id)

            callback_cmd += 'curl "%s/terminate/%s?phase=completed"' % (
                self._config['callback_url'], task_id)
            cmd += "echo \"=> " + callback_cmd.replace("\"", "\\\"") + "\"\n"
            cmd += callback_cmd + '\n'

        cmd += "EOF\n"

        qsub_cmd = "echo \"$(%s)\" | %s" % (
            cmd, os.path.join(params['torque_install_path'], "qsub -V"))

        exit_status, stdout, stderr = common.run_command(client, qsub_cmd)
        if exit_status != 0:
            client.close()
            raise RuntimeError('run exited with code %d: %s' % (exit_status, stderr.read()))

        client.close()
        params['model'] = task_id
        params['qsub_id'] = stdout.read().strip()
        return params

    @staticmethod
    def status(task_id, params):
        logger.info("Check status of process with qsub id %s.", params['qsub_id'])

        client = paramiko.client.SSHClient()
        client.load_system_host_keys()
        client.connect(params['master_node'], username=params['login'])
        _, stdout, _ = client.exec_command(
            '%s -f %s' % (os.path.join(params['torque_install_path'], "qstat"), params['qsub_id']))
        outstatus = stdout.read()
        client.close()

        m = re.search(r'job_state = (.)\n', outstatus)

        if m is None or m.group(1) == "C":
            return "dead"

        status = m.group(1)
        m = re.search(r'exec_gpus = (.*?)\n', outstatus)
        host = '?'

        if m is not None:
            host = m.group(1)

        return "%s (%s)" % (status, host)
    # pylint: enable=unused-argument,arguments-differ

    @staticmethod
    def terminate(params):
        client = paramiko.client.SSHClient()
        client.load_system_host_keys()
        client.connect(params['master_node'], username=params['login'])
        client.exec_command(
            '%s %s' % (os.path.join(params['torque_install_path'], "qdel"), params['qsub_id']))
        client.close()

    def get_server_detail(self, server, field_name):
        raise NotImplementedError()


def init(config):
    return TorqueService(config)  # pylint: disable=abstract-class-instantiated
