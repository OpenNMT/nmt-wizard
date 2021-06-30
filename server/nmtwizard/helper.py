from datetime import datetime
import json
import time
import uuid

from bson import json_util
from flask import abort, jsonify, make_response

from nmtwizard import configuration as config

from nmtwizard.funnynames.german import generate_name_de
from nmtwizard.funnynames.english import generate_name_en
from nmtwizard.funnynames.french import generate_name_fr
from nmtwizard.funnynames.chinese import generate_name_zh


def our_default_encoder(obj):
    """Override the json encoder that comes with pymongo (bson)
    So that datetime objects are encoded as ISO-8601"""
    if isinstance(obj, datetime):
        return time.mktime(obj.timetuple())

    return json_util.default(obj)


def cust_jsondump(obj):
    return json.dumps(obj, default=our_default_encoder)


def _generate_name(xxyy, length=15):
    if xxyy.startswith("de") or xxyy[2:].find("de") != -1:
        return generate_name_de(length)
    if xxyy.startswith("zh") or xxyy[2:].find("zh") != -1:
        return generate_name_zh(length)
    if xxyy.startswith("fr") or xxyy[2:].find("fr") != -1:
        return generate_name_fr(length)
    return generate_name_en(length)


def get_docker_action(command):
    # docker command starts with docker option
    idx = 0
    while idx < len(command) and command[idx].startswith('-'):
        if (command[idx] == '-d' or
                command[idx] == '--disable-content-trust' or
                command[idx] == '--help' or
                command[idx] == '--init' or
                command[idx].startswith('-i') or command[idx] == '--interactive' or
                command[idx] == '--privileged' or
                command[idx].startswith('-P') or command[idx] == '--publish-all' or
                command[idx] == '--read-only' or
                command[idx] == '--sig-proxy' or
                command[idx] == '--rm' or
                command[idx].startswith('-t') or command[idx] == '--tty'):
            idx += 1
        else:
            idx += 2
    # reach the index boundary, if there is no action (score, align, etc.)
    if idx >= len(command):
        return None
    if command[idx] == '--':
        idx += 1
    # possible entrypoint parameters
    while idx < len(command) and command[idx].startswith('-'):
        if command[idx] == '--no_push':
            idx += 1
        idx += 1
    # now, there should be the command name
    if idx < len(command):
        return command[idx]
    return None


def shallow_command_analysis(command):
    i = 0
    xx = 'xx'
    yy = 'yy'
    parent_task = None
    while i < len(command):
        if (command[i] == '-m' or command[i] == '--model') and i + 1 < len(command):
            parent_task = command[i + 1]
            i += 1
        elif (command[i] == '-c' or command[i] == '--config') and i + 1 < len(command):
            config = json.loads(command[i + 1])
            if not parent_task and "model" in config:
                parent_task = config["model"]
            if "source" in config:
                xx = config["source"]
            if "target" in config:
                yy = config["target"]
            i += 1
        i += 1
    return xx + yy, parent_task


def change_parent_task(command, task_id):
    i = 0
    while i < len(command):
        if (command[i] == '-m' or command[i] == '--model') and i + 1 < len(command):
            command[i + 1] = task_id
            return
        i += 1
    command.insert(0, task_id)
    command.insert(0, '-m')


def remove_config_option(command):
    i = 0
    while i < len(command):
        if (command[i] == '-c' or command[i] == '--config') and i + 1 < len(command):
            del command[i:i + 2]
            return
        i += 1


model_types = [
    "trans",
    "train",
    "preprocess",
    "vocab",
    "release"
]
model_type_map = {t[0:5]: t for t in model_types}


def model_name_analysis(model):
    task_type = None
    struct = {}
    lst = model.split("_")
    if lst[-1] in model_types or len(lst) == 6:
        task_type = lst[-1][:5]
        lst.pop(-1)
    else:
        task_type = "train"
    if len(lst) < 4 or len(lst) > 5:
        return None, None
    struct["trid"] = lst.pop(0)
    struct["xxyy"] = lst.pop(0)
    struct["name"] = lst.pop(0)
    struct["nn"] = lst.pop(0)
    try:
        int(struct["nn"])
    except ValueError:
        if len(lst) == 0:
            lst.append(struct["nn"])
            del struct["nn"]
    uuid = lst.pop(0)
    usplit = uuid.split('-')
    if len(usplit) > 1:
        struct["uuid"] = usplit[0]
        struct["parent_uuid"] = usplit[-1]
    else:
        struct["uuid"] = uuid
    return struct, task_type


def build_task_id(content, xxyy, task_type, parent_task):
    # let us build a meaningful name for the task
    # name will be TRID_XXYY_NAME_NN_UUID(:UUID)-TYPE with:
    # * TRID - the trainer ID
    # * XXYY - the language pair
    # * NAME - user provided or generated name
    # * NN - the iteration (epoch) - automatically incremented for training task
    # * UUID - one or 2 parts - parent:child or child
    # * TYPE - trans|prepr|vocab|relea or other 5 letter action

    # first find nature of the task - train or not
    is_train = task_type == "train"
    is_buildvocab = task_type == "vocab"
    trid = 'XXXX'
    if 'trainer_id' in content and content['trainer_id']:
        trid = content['trainer_id']

    name = content["name"] if "name" in content else None
    if name is not None:
        name = name.replace("-", "").replace("_", "")[:15]
    parent_uuid = ''
    nn = None
    parent_task_type = None
    if parent_task is not None:
        struct_name, parent_task_type = model_name_analysis(parent_task)
        if name is None and "name" in struct_name:
            name = struct_name["name"]
        if (xxyy is None or xxyy == 'xxyy') and "xxyy" in struct_name:
            xxyy = struct_name["xxyy"]
        if "uuid" in struct_name:
            parent_uuid = '-' + struct_name["uuid"][0:5]
        if "nn" in struct_name:
            nn = int(struct_name["nn"])

    if nn is None:
        if is_buildvocab:
            nn = 0
        else:
            nn = 1
    else:
        if task_type == "prepr" or (task_type == "train" and parent_task_type != "prepr"):
            nn += 1

    explicitname = None
    if not name:
        name = _generate_name(xxyy)
        if isinstance(name, tuple):
            explicitname = name[1] + " (" + name[2] + ")"
            name = name[0]

    the_uuid = str(uuid.uuid4()).replace("-", "")

    if nn is None:
        task_id = '%s_%s_%s_%s' % (trid, xxyy, name, the_uuid)
    else:
        task_id = '%s_%s_%s_%02d_%s' % (trid, xxyy, name, nn, the_uuid)
    task_id = task_id[0:47 - len(parent_uuid)] + parent_uuid
    if task_type != "train":
        task_id += '_' + model_type_map.get(task_type, task_type)
    return task_id, explicitname


def get_cpu_count(config, ngpus, task):
    if "cpu_allocation" in config:
        if ngpus > 0:
            return config["cpu_allocation"].get("gpu_task", 2)
        return config["cpu_allocation"].get("%s_task" % task, 2)
    return 2


def get_gpu_count(config, task):
    if "gpu_allocation" in config:
        return config["gpu_allocation"].get("%s_task" % task, 1)
    return 1


def get_params(lparam, listcmd):
    res = []
    idx = 0
    while idx < len(listcmd):
        if listcmd[idx] in lparam:
            idx = idx + 1
            while idx < len(listcmd) and not listcmd[idx].startswith('-'):
                res.append(listcmd[idx])
                idx += 1
            continue
        idx += 1
    return res


def boolean_param(value):
    return not (value is None or
                value is False or
                value == "" or
                value == "0" or
                value == "False" or
                value == "false")


def get_registry(service_module, image):
    from app import mongo_client
    p = image.find("/")
    if p == -1:
        abort(make_response(jsonify(message="image should be repository/name"), 400))
    repository = image[:p]
    registry = None
    docker_registries = config.get_registries(mongo_client, service_module.name)
    for r in docker_registries:
        v = docker_registries[r]
        if "default_for" in v and repository in v['default_for']:
            registry = r
            break
    if registry is None:
        abort(make_response(jsonify(message="cannot find registry for repository %s" % repository), 400))
    return registry
