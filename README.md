# nmt-wizard

`nmt-wizard` is a Docker-based task launcher and monitor on a variety of remote platforms (called *services*) such as SSH servers, Torque clusters, or EC2 instances. Each *service* is providing access to compute *resources*. The launcher is meant to be used with `nmt-wizard-docker` images, but without a strong dependency.

The project provides:

* a RESTful server that queues incoming requests in a Redis database;
* a client to the REST server providing a simple textual visualization interface;
* workers that launch and manage tasks on the requested service and updates their status.

Once launched, the tasks are sending progress and updated information to the launcher.

## Services configuration

Service configurations are provided by the system administrators. A service is declared and configured by a JSON file in the `config` directory. The REST server and worker automatically discover existing configuration files, provided that their filename ends with `.json`. A special `default.json` file defines parameters shared by all services.

The configuration file has the following structure:

```
{
    "name": "my-service",  // The short name the user will select.
    "description": "My service",  // Display name of the service.
    "module": "services.XXX",  // Name of the Python module managing the service.
    "variables": { // Non-Service specific configurations.
        "key1": [ "value1", "value2" ],
        ...
    },
    "docker": {
        "registries": {  // Docker registries: ECS, Docker Hub.
            "aws": {
                "type": "aws",
                "credentials": {
                    "AWS_ACCESS_KEY_ID": "XXXXX",
                    "AWS_SECRET_ACCESS_KEY": "XXXXX"
                },
                "uri": "XXXXX.dkr.ecr.eu-west-3.amazonaws.com",
                "region": "eu-west-3"
            },
            "dockerhub": {
                "type": "dockerhub",
                "uri": "",
                "default_for": [ "nmtwizard" ]
            },
            "mydockerprivate": {
                "type": "dockerprivate",
                "uri": "",
                "credentials": {
                    "password": "XXXXX",
                    "username": "XXXXX"
                },
                "default_for": [ "systran" ]
            }
        },
        "mount": [  // Volumes to mount when running the Docker image.
            "/home/devling/corpus:/root/corpus",
            "/home/devling/models:/root/models"
        ],
        "envvar": {  // Environment variables to set when running the Docker image.
            "key1": "value1",
            "specific": {
                "image": {
                    "key": "value"
                }
            }
        },
        "path": "" // if provided, will be used to select default path for docker command on remote service.
    },
    "skey1": "svalue1",  // Non-Service specific configurations.
    ...,
    "disabled": [01],  // Boolean field to disable/enable the service.
    "storages": {  // Storage configuration as described in single-training-docker.
    },
    "cpu_allocation": { // define default number of cpu allocated for the different tasks
          "gpu_task": 2,
          "preprocess_task": 4,
          "trans_task": 4,
          "buildvocab_task": 4,
          "release_task": 2
    },
    "callback_url": "http://LAUNCHER_URL",
    "callback_interval": 60,
    "ttl_policy" : [ // define the time to live policy for completed task in the database
        { 
            "pattern": {
                "status": "stopped",
                "message": "dependency_error"
            },
            "ttl": 10
        }
    ]
}
```

where `variables` is a list of possible options for the service. The structure of some of these options is specific to each service. Common options are described below.

Template files are provided in `config/templates` and can be used as a basis for configuring services.

**Note**:
* it is possible to add a field `"default_ms":true` to one storage definition. If no model storage parameter (`-ms`) is provided to the docker, this storage will be used by default.
* to avoid exposing credentials in the task logs, annotate with "[[private:xxxx]]" all the values that are sensitive, these values will be masked as "[[private]]" in training logs.
* specific environment variables can be passed to specific image (for instance credentials) - use "specific" dict for that.
* `ttl_policy` defines the time to leave (ttl) for stopped tasks in the redis database. Each pattern defines a config to match on the stopped task, if all conditions applies, the corresponding ttl is applied.

## Server configuration

* The REST server and workers are configured by `settings.ini`. The default configuration for all of the pools is stored in `default.json`. This file is read by the launcher when the launcher is started and stored in REDIS database together with a timestamp. Whenever this file is changed, the corresponding timestamp must be modified - and this modification will be detected by all workers that are then restarting.

## Using the launcher

### Worker

The first component to launch is the worker that should always be running. It handles:

* the launch of tasks
* the termination of tasks
* the update of active resources

```bash
cd server && python worker.py configfile.json
```

One worker has to be launched for every service pool. To monitor each worker and allow to be relaunched in case it stops (either because of an issue/exception in the code, or simply for updating its parameter) - the worker should be launcher using `runworker.py` as following:

```bash
python runworker.py service_name
```

`runworker.py` and `worker.py` are requesting following:
* a `configurations` directory in which the different configurations for the service will be copied
* a `logs` directory in which worker logs will be saved
* an active configuration file for the service in the current directory with name `service_name.json`. Not that a service name should be composed of letters, digits, - and _ characters only
* the settings.ini file
* the logger configuration file `logging.conf`

To administer multiple services: create a main directory with above-mentioned structure, and launch as many workers as pool to create.

### Server

The server has the following HTTP routes:

| METHOD    | ROUTE | PARAM | Description |
| ---       | --- | --- | --- |
| `GET`     | `status` | | Check service availability |
| `GET`     | `service/list` | | Returns available services |
| `GET`     | `service/describe/{service_id}` | | Returns user selectable options for a specified service |
| `GET`     | `service/check/{service_id}` | | Checks availability of a given service with provided user options |
| `POST`    | `task/launch` | |  Launches a task on a given service with provided user options
| `GET`     | `task/status/{task_id}` | |  Checks the status of a task |
| `GET`     | `task/list` | prefix | Returns the list of tasks in the database |
| `GET`     | `task/terminate/{task_id}` | |  Terminates a task, the process and/or instance associated with a task |
| `DELETE`  | `task/{task_id}` | | Delete a task from the database |
| `PUT`     | `task/beat/{task_id}` | | Provides a `beat` back to the launcher to notify the task activity and announce the next beat to expect |
| `GET`<br>`POST` | `task/file/{task_id}` | | Gets or set a file associated to a task |
| `POST` | `task/stat/{task_id}` | | post json statistics for a task |
| `GET`<br>`PATCH`<br>`POST` | `task/log/{task_id}` | | Gets, appends or sets the log associated to a task |
| `GET`     | `service/listconfig/{service}` | | List all configurations associated to a given service | 
| `GET`     | `service/selectconfig/{service}/{configname}` | | Select a named configuration associated to a given service | 
| `POST`     | `service/addconfig/{service}/{configname}` | `config` | Register a new named configuration for a given service | 
| `GET`     | `service/delconfig/{service}/{configname}` | | Delete a new named configuration for a given service | 
| `GET`     | `service/restart/{service}` | | Restart a service | 
| `GET`     | `service/disable/{service}/{resource}` | `message`| Disable a resource for a given service  | 
| `GET`     | `service/enable/{service}/{resource}` | | Enable a disabled resource for a given service  |


The server uses Flask. See the [Flask documentation](http://flask.pocoo.org/docs/0.12/deploying/) to deploy it for production. For development purpose, it can be run as follows (single thread):

```bash
export LAUNCHER_CONFIG=${PWD}/settings.ini
FLASK_APP=main.py flask run [--host=0.0.0.0]
```

Here are the available routes. Also see the next section

#### `GET /service/list`

Lists available services.

* **Arguments:** None
* **Input:** None
* **Output:** A dictionary of service name to description (JSON), their usage and capacity
* **Example:**

```
$ curl -X GET 'http://127.0.0.1:5000/service/list'
{
    "demogpu02": {
        "capacity": 1,
        "name": "OVH-hosted extra training server",
        "queued": 2,
        "usage": 1
    },
    "localhost": {
        "capacity": 2,
        "name": "test local environment",
        "queued": 0,
        "usage": 0
    },
    "ec2": {
        "capacity": 15,
        "name": "Instance on AWS EC2",
        "queued": 0,
        "usage": 7
    }
}
```

#### `GET /service/describe/<service_name>`

Returns possible options for a service as a [JSON Form](https://github.com/joshfire/jsonform). This can be used to easily implement a GUI to select options for the target service.

* **Arguments:**
  * `service_name`: the service name
* **Input:** None
* **Output:** A JSON form (or an empty dictionary if the service has no possible options).
* **Example:**

```
$ curl -X GET 'http://127.0.0.1:5000/service/describe/ec2'
{
  "launchTemplate": {
    "description": "The name of the EC2 launch template to use",
    "enum": [
      "SingleTrainingDev"
    ],
    "title": "EC2 Launch Template",
    "type": "string"
  }
}
```

#### `GET /service/check/<service_name>`

Checks if the service is available and can be used with the provided options. In case of success, it returns information about the service and the corresponding resource.

* **Arguments:**
  * `service_name`: the service name
* **Input:** The selected service options (see `service/describe/<service_name>`) (JSON)
* **Output:**
  * On invalid option, a HTTP 400 code with the error message (JSON)
  * On server error, a HTTP 500 code with the error message (JSON)
  * On success, an optional message with details about the service (JSON)
* **Example:**

```
$ curl -X GET http://127.0.0.1:5000/service/check/ec2
{
  "message": "missing launchTemplateName option",
}
$ curl -X GET -d '{"launchTemplateName": "InvalidLaunchTemplate"}' \
    -H "Content-Type: application/json" 'http://127.0.0.1:5000/service/check/ec2'
{
  "message": "An error occurred (InvalidLaunchTemplateId.NotFound) when calling the RunInstances operation: LaunchTemplate null not found"
}
$ curl -X GET -d '{"launchTemplateName": "SingleTrainingDev"}' \
    -H "Content-Type: application/json" 'http://127.0.0.1:5000/service/check/ec2'
{
  "message": ""
}
```

#### `POST /task/launch/<service_name>`

Launches a Docker-based task on the specified service. In case of success, it returns a task identifier that can be used to monitor the task using the `status` or `terminate` routes.

* **Arguments:**
  * `service_name`: the service name
* **Input:** the input is either a simple json body or a multi-part request with `content` field containing JSON task configuration. The other fields of the multi-part requests are binary files to be uploaded on the remote service at task-launch time.

The task configuration (JSON)

```
$ cat body.json
{
  "docker": {
    "registry": "dockerhub"
    "image": "opennmt/opennmt-lua",
    "tag": "latest",
    "command": [
      ...
    ]
  },
  "wait_after_launch": 2,
  "trainer_id": "OpenNMT",
  "options": {
    "launchTemplateName": "SingleTrainingDev"
  },
  "name": 'TaskName', // optional
  "iterations": 4,    // number of training iterations, default 1
  "priority": 100     // task priority
}
```

`docker.tag` and `wait_after_launch` are optional.
* **Output:**
  * On invalid task configuration, a HTTP 400 code with an error message (JSON)
  * On success, a task identifier (string)
* **Example:**

```
$ curl -X POST -d @invalid_body.json -H "Content-Type: application/json" \
    http://127.0.0.1:5000/task/launch/ec2
{
  "message": "missing trainer_id field"
}
$ curl -X POST -d @body.json -H "Content-Type: application/json" \
    'http://127.0.0.1:5000/task/launch/ec2'
"SSJS_enyy_HelloWorld_01_0f32d3f6b84ab91d4"
$ curl -X POST -d content=@body.json -F input.txt=@input.txt 'http://127.0.0.1:5000/task/launch/ec2'
"SSJS_xxyy_GreenCat_01_085a8:60c06412a2b74"
```

**Notes**: 

* the task identifier is structured, when possible, and contains the following 5 fields separated by `_`:
  * `TID` - trainer identifier provided through client application
  * `XXYY` - the language pair, found in the configuration file or from parent model
  * `NAME` - model name: generated randomly, or set manually, or inherited from parent
  * `NN` - number showing the number of iterations
  * `UUID` or `UUID:PRID`, unique identifier (possibly suffixed by initial of parent UUID)
* if a `iterations` value is passed to the launch service, several tasks will be created each one starting with previous generated one. The tasks are executed iteratively. It is also possible to use a non-yet generated model as a starting point to launch another task: in that case, the task will start only upon successful termination of the parent task.

#### `GET /task/list/<pattern>`

Lists available services.

* **Arguments:**
  * `pattern`: pattern for the tasks to match. See [KEYS pattern](https://redis.io/commands/keys) for syntax.
* **Input:** None
* **Output:** A list of tasks matching the pattern with minimal information (`task_id`, `queued_time`, `status`, `service`, `message`, `pid`)
* **Example:**

```
$ curl -X GET 'http://127.0.0.1:5000/task/list/jean_*'
[
  {
    "message": "completed", 
    "queued_time": "1519652594.957615", 
    "status": "stopped",
    "service": "ec2",
    "task_id": "jean_5af69495-3304-4118-bd6c-37d0e6"
  }, 
  {
    "message": "error", 
    "queued_time": "1519652097.672299", 
    "status": "stopped",
    "service": "mysshgpu", 
    "task_id": "jean_99b822bc-51ac-4049-ba39-980541"
  }
]
```

#### `DELETE /task/<pattern>`

Lists available services.

* **Arguments:**
  * `pattern`: pattern for the tasks to match - only stopped tasks will be deleted. See [KEYS pattern](https://redis.io/commands/keys) for syntax.
* **Input:** None
* **Output:** list of deleted tasks
* **Example:**

```
$ curl -X DELETE 'http://127.0.0.1:5000/task/jean_*'
[
  "jean_5af69495-3304-4118-bd6c-37d0e6",
  "jean_99b822bc-51ac-4049-ba39-980541"
]
```

#### `GET /task/status/<task_id>`

Returns the status of a task.

* **Arguments:**
  * `task_id`: the task ID returned by `/task/launch/<service_name>`
* **Input:** None
* **Output:**
  * On invalid `task_id`, a HTTP 404 code dictionary with an error message (JSON)
  * On success, a dictionary with the task status (JSON)
* **Example:**

```
curl -X GET http://127.0.0.1:5000/task/status/unknwon-task-id
{
  "message": "task unknwon-task-id unknown"
}
curl -X GET http://127.0.0.1:5000/task/status/130d4400-9aad-4654-b124-d258cbe4b1e3
{
  "allocated_time": "1519148201.9924579",
  "content": "{\"docker\": {\"command\": [], \"registry\": \"dockerhub\", \"image\": \"opennmt/opennmt-lua\", \"tag\": \"latest\"}, \"service\": \"ec2\", \"wait_after_launch\": 2, \"trainer_id\": \"OpenNMT\", \"options\": {\"launchTemplateName\": \"SingleTrainingDev\"}}", 
  "message": "unknown registry",
  "queued_time": "1519148144.483396",
  "resource": "SingleTrainingDev",
  "service": "ec2",
  "status": "stopped",
  "stopped_time": "1519148201.9977396",
  "ttl": null
}
```

(Here the task was quickly stopped due to an incorrect Docker registry.)

The main fields are:

* `status`: (timestamp for each status can be found in `<status>_time`)
  * `queued`,
  * `allocated`,
  * `running`,
  * `terminating`,
  * `stopped` (additional information can be found in `message` field);
* `service`: name of the service the task is running on;
* `resource`: name of the resource the task is using;
* `content`: the actual task definition;
* `update_time`: if the task is sending beat requests;
* `ttl` if a time to live was passed in the beat request.

#### `GET /task/terminate/<task_id>(?phase=status)`

Terminates a task. If the task is already stopped, it does nothing. Otherwise, it changes the status of the task to `terminating` (actual termination is asynchronous) and returns a success message.

* **Arguments:**
  * `task_id`: the task identifier returned by `/task/launch/<service_name>`
  * (optionnal) `phase`: indicate if the termination command is corresponding to an error or natural completion (`completed`)
* **Input**: None
* **Output**:
  * On invalid `task_id`, a HTTP 404 code with an error message (JSON)
  * On success, a HTTP 200 code with a message (JSON)

```
curl -X GET http://127.0.0.1:5000/task/terminate/130d4400-9aad-4654-b124-d258cbe4b1e3
{
  "message": "130d4400-9aad-4654-b124-d258cbe4b1e3 already stopped"
}
```

#### `DELETE /task/<task_id>`

Deletes a task. If the task is not stopped, it does nothing.

* **Arguments:**
  * `task_id`: the task identifier returned by `/task/launch/<service_name>`
* **Input**: None
* **Output**:
  * On invalid `task_id`, a HTTP 404 code with an error message (JSON)
  * On success, a HTTP 200 code with a message (JSON)

#### `PUT /task/beat/<task_id>(?duration=XXX&container_id=CID)`

Notifies a *beat* back to the launcher. Tasks should invoke this route wih a specific interval to notify that they are still alive and working. This makes it easier for the launcher to identify and handle dead tasks.

* **Arguments**
  * `task_id`: the task identifier returned by `/task/launch/<service_name>`
  * (optional) `duration`: if no beat is received for this task after this duration the task is assumed to be dead
  * (optional) `container_id`: the ID of the Docker container
* **Input:** None
* **Output:**
  * On invalid `duration`, a HTTP 400 code with an error message (JSON)
  * On invalid `task_id`, a HTTP 404 code with an error message (JSON)
  * On success, a HTTP 200 code

#### `POST /task/file/<task_id>/<filename>`

Registers a file for a task - typically used for log, or posting translation output using http storage.

* **Arguments**
  * `task_id`: the task identifier returned by `/task/launch/<service_name>`
  * `filename`: a filename
* **Input:** None
* **Output:**
  * On invalid `task_id`, a HTTP 404 code with an error message (JSON)
  * On success, a HTTP 200 code

#### `GET /task/file/<task_id>/<filename>`

Retrieves file attached to a task

* **Arguments**
  * `task_id`: the task identifier returned by `/task/launch/<service_name>`
  * `filename`: a filename
* **Input:** None
* **Output:**
  * On invalid `task_id`, a HTTP 404 code with an error message (JSON)
  * On missing files, a HTTP 404 code with an error message (JSON)
  * On success, the actual file

#### `GET/POST/PATCH /task/log/<task_id>`

Gets/Posts/Append log attached to a task. Logs are saved in a special `log:<task_id>` key in the redis table allowing for fast implementation of append operation.

### Launcher

The launcher is a simple client to the REST server. See:

```bash
python client/launcher.py -h
```

**Notes:**

* The address of the launcher REST service is provided either by the environment variable `LAUNCHER_URL` or the command line parameter `-u URL`.
* By default, the request response are formatted in text-table for better readibility, the option `-j` displays raw JSON response
* The `trainer_id` field to the `launch` command is either coming from `--trainer_id` option or using `LAUNCHER_TID` environment variable. Also, by default, the same environment variable is used as a default value of the `prefix` parameter of the `lt` command.
* By default, the command parameter are expected as inline values, but can also be obtained from a file, in that case, the corresponding option will take the value `@FILEPATH`.
* File identified as local files, are transfered to the launcher using `TMP_DIR` on the remote server

## Development

### Redis database

The Redis database contains the following task fields:

| Field | Type | Description |
| --- | --- | --- |
| `active:<service>` | list | Active tasks for a given service |
| `beat:<task_id>` | int | Specific ttl-key for a given task |
| `lock:<resource...,task:...>` | value | Temporary lock on a resource or task |
| `queued:<service>` | list | Tasks waiting for a resource |
| `gpu_resource:<service>:<resourceid>` | dict | Tasks using GPU on this resource, key is (pseudo) GPU-id |
| `cpu_resource:<service>:<resourceid>` | list | Tasks using CPU (only) on this resource |
| `reserved:<service>:<resourceid>` | value | id of a task reserving a specific resource |
| `ncpus:<service>:<resourceid>` | value | number of cpus available for a specific resource |
| `busy:<service>:<resourceid>` | string | expirable timestamp on a resource indicating that the resource has been seen unavailable |
| `task:<taskid>` | dict | <ul><li>status: [queued, allocated, allocating, running, terminating, stopped]</li><li>job: json of jobid (if status>=waiting)</li><li>service:the name of the service</li><li>resource: the name of the resource - or auto before allocating one message: error message (if any), ‘completed’ if successfully finished</li><li>container_id: container in which the task run send back by docker notifier</li><li>(queued|allocated|allocating|running|updated|stopped)_time: time for each event</li><li>parent: parent task id, if any</li>type: task type id (trans, train, ...)</li><li>priority: task priority (higher better)</li><li>alloc_(resource|lgpu): allocated resource and gpu list</li><li>(ncpus|ngpus): number of cpus/gpus used by the task</li><li>duration: duration of the task in seconds reported by framework</li><li>statistics: statistics on the task reported by the framework</li></ul> |
| `files:<task_id>` | dict | files associated to a task, "log" is generated when training is complete |
| `queue:<task_id>` | str | expirable timestamp on the task - is used to regularily check status |
| `work:<service>` | list | Tasks being processed on a given service |

and the following `admin` fields - all prefixed by `admin:`:

| Field | Type | Description |
| --- | --- | --- |
| `admin:storages` | JSON | Description of storages block as defined in `default.json` |
| `admin:service:<service>` | dict | <ul><li>`beat_time`: last beat time of the corresponding worker</li><li>`worker_pid`: pid of the corresponding worker</li><li>`launch_time`: time of the worker launch</li><li>`def`: pickle of the corresponding service class definition</li></ul> |
| `admin:resources:<service>` | list | list of the attached resources |

