import random
import json
import uuid

adjectives = ['Blue', 'Green', 'Pink', 'Red', 'Yellow']
nouns = ['Dog', 'Cat', 'Mouse', 'Girafe', 'Squirrel']

def _generate_name():
	return random.choice(adjectives)+random.choice(nouns)

def _shallow_command_analysis(command):
    i = 0
    xx = 'xx'
    yy = 'yy'
    parent_model = None
    while i < len(command):
        if command[i] == '-m' and i+1 < len(command):
            parent_model = command[i+1]
            i += 1
        elif command[i] == '-c' and i+1 < len(command):
            config = json.loads(command[i+1])
            if not parent_model and "model" in config:
                parent_model = config["model"]
            if "source" in config:
                xx = config["source"]
            if "target" in config:
                yy = config["target"]
            i += 1
        i += 1
    return (xx+yy, parent_model)

def _model_name_analysis(model):
    if model:
        struct = {}
        l = model.split("_")
        if len(l) < 4 or len(l)>5:
            return
        if len(l) == 5:
            struct["trid"] = l.pop(0)
        else:
            struct["trid"] = None
        struct["xxyy"] = l.pop(0)
        struct["name"] = l.pop(0)
        struct["nn"] = l.pop(0)
        struct["uuid"] = l.pop(0)
        usplit = struct["uuid"].split(':')
        if len(usplit) > 1:
            struct["uuid"] = usplit[-1]
            struct["parent_uuid"] = usplit[0]
        return struct

def build_task_id(content):
    # let us build a meaningful name for the task
    # name will be TRID_XXYY_NAME_NN_UUID(:UUID) with:
    # * TRID - the trainer ID
    # * XXYY - the language pair
    # * NAME - user provided or generated name 
    # * NN - the iteration (epoch) - automatically incremented for training task
    # * UUID - one or 2 parts - parent:child or child

    # first find nature of the task - train or not
    is_train = "train" in content["docker"]["command"]
    (xxyy, parent_model) = _shallow_command_analysis(content["docker"]["command"])
    trid = 'XXXX'
    if 'trainer_id' in content and content['trainer_id']:
        trid = content['trainer_id']
    nn = 0
    name = content["name"]
    parent_uuid = ''
    if parent_model is not None:
        struct_name = _model_name_analysis(parent_model)
        if name is None and "name" in struct_name:
            name = struct_name["name"]
        if xxyy is None and "xxyy" in struct_name:
            xxyy = struct_name["xxyy"]
        if "uuid" in struct_name:
            parent_uuid = struct_name["uuid"][0:5]+':'
        if "nn" in struct_name:
            nn = int(struct_name["nn"])

    if is_train:
        nn += 1
        if not name:
            name = _generate_name()

    the_uuid = str(uuid.uuid4()).replace("-","")

    task_id = '%s_%s_%s_%02d_%s%s' % (trid, xxyy, name, nn, parent_uuid, the_uuid)
    task_id = task_id[0:41]
    return task_id