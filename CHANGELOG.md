## Unreleased

## [v1.4.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.4.1) (2018-12-17)

### Fixes and improvements
* Add French and German funny names
* (worker) fix issue with CPU allocation making tasks fail when all CPU were allocated
* (worker) worker does not fail if configuration is invalid, pool is not disabled
* (worker) do not set cpu restriction on dedicated instances (like ec2)
* (worker) double check process termination after docker terminate
* (worker) patch log renew beat

## [v1.4.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.4.0) (2018-12-14)

### Fixes and improvements
* (worker) refactor and improve ec2 service

## [v1.3.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.3.1) (2018-12-11)

* (client) backward compatibility for `lt`

## [v1.3.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.3.0) (2018-12-10)

* (worker) refactor cpu allocation - are now allocated like gpu
* (launcher) automatic translation tasks are now by default launched with `--as_release`

## [v1.2.5](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.2.5) (2018-11-28)

### Fixes and improvements
* (worker) fix retry on ssh connection
* (worker) more robust environment checking before launching task

## [v1.2.4](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.2.4) (2018-11-26)

### Fixes and improvements
* (launcher) fix crash on `task/status` without parameters

## [v1.2.3](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.2.3) (2018-11-25)

### Fixes and improvements
* (worker) fix integrity checking at worker restart

## [v1.2.2](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.2.2) (2018-11-23)

### Fixes and improvements
* (launcher) `max_log_size` option in setting file for setting maximum limit to log file

## [v1.2.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.2.1) (2018-11-22)

### Fixes and improvements
* (launcher) fix incorrect reporting of CPU-only tasks

## [v1.2.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.2.0) (2018-11-22)

### Breaking changes
* server code is now requiring `redis-py>=3.0.0`

### Fixes and improvements
* (worker) For security, checks that config name matches json config filename
* (worker) Fix invalid log when redis not directly available at startup
* model/task name are 6 characters longer and now include postfix (:`trans`/`prepr`/`relea`/`vocab`)
* tasks are now sorted by launch date
* introduce extension function for routes
* fix too slow propagation of stopped tasks
* optimize task stopping
* (client) optimize initial handshaking to check connection and authentication
* (worker) fix major bug making number of cpus available not accurate and finally blocking tasks

## [v1.1.2](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.1.2) (2018-10-25)

### Fixes and improvements
* Fix error when worker restarts with pure cpu-tasks running
* (client) Add `--nochainprepr` to disable chained preprocessing and training
* (client) Fix `prepr` task type and display in ls
* (worker) Increasing logs in debug mode

## [v1.1.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.1.1) (2018-10-23)

### Fixes and improvements
* Fix possible error when `process_request` is called multiple times

## [v1.1.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.1.0) (2018-10-23)

### New features
* add `stop` admin command - requires `stop:config`
* finer-grain route permission checks
* split training tasks into prepr and train for more efficient GPU resource handling.

### Fixes and improvements
* Fix translation tasks not launch for iterations 2+
* Fix split of subsets for chained translation tasks and pure-CPU training
* Improve reactivity of worker that could let a resource idle for one hour
* Increase "dead-worker" detection to 20 minutes of unchanged beat
* Reduce running task checking interval to every 5min after first check
* Fix workeradmin tasks taking all bandwidth
* Improve management of CPU-tasks
* Fix parsing options

## [v1.0.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v1.0.0) (2018-10-04)

### Breaking changes
* workers are now dedicated to a single pool

### New features
* new tool `runworker` for activity monitoring/relaunching of workers
* add `workeradmin` redis message queue for interaction with workers, and corresponding new REST service in launcher
* each worker can manage and switch between multiple configurations
* possibility to pass private key directly in configurations

### Fixes and improvements
* default `settings.ini` in current directory
* introduce `--resource` as simpler alternative to `--option` 
* chained translation tasks are now dispatched on multiple gpus
* (worker) retry 5 times failed status, and correctly terminate

## [v0.4.2](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.4.2) (2018-09-21)

## Fixes and improvements
* fix ID of chained translation tasks (#12)

## [v0.4.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.4.1) (2018-09-20)

## Fixes and improvements
* Fix broken 'trans'

## [v0.4.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.4.0) (2018-09-19)

### New features
* Possibly chain training and translation tasks
* (admin) add `stop by admin` feature

## Fixes and improvements
* when running chained training - update queued time to be close to terminate time of parent task
* more robust worker beat
* log and file storing use filesystem

## [v0.3.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.3.1) (2018-09-17)

## Fixes and improvements
* Fix client compatibility with Python 3
* Fix 'change task' not putting tasks on the new service active queue

## [v0.3.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.3.0) (2018-09-17)

### Breaking changes
* rename shortcut for `--priority` to `-P`
* launcher service does not rely on json configuration files, and dynamically retrieve configuration from `admin` section in database

### New features
* Add `--quiet` mode and `-S STATUS` for `lt` command

## Fixes and improvements
* Fix incorrect usage count with `ls`
* Review Funny names to remove offending candidates
* Fix incorrect count of available CPU

## [v0.2.2](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.2.2) (2018-08-24)

### Breaking changes
* normalize database structure for support of CPU servers.

### New features
* add possibility to change ssh port for connection to training services
* enable pure-cpu server and support 0-gpu tasks

### Fixes and improvements
* worker use `logging.conf` for formatted logs
* fix ghost tasks remaining after terminated if they were already in service queue
* fix download of binary file (#9)
* extend locking period for more robustness under load

## [v0.2.1](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.2.1) (2018-08-02)

### New features
* introduce TTL on stopped tasks in database

### Fixes and improvements

* fix python 3 compatibility issue (client)

## [v0.2.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.2.0) (2018-07-31)

### Breaking changes
* redefine more consistent routes
* change `task_id` as positional argument for simpler client commands
* fix `service/check` returning inconsistent json format 
* local path parameters must be absolute path

### New features

* versioning information
* possibility to launch multi-gpu tasks
* launch error now reports error message in task log
* define `auto` registry - resolved by launcher checking at `default_for` in registry definition
* add friendly name to tasks, and differentiate task type
* add task chaining feature - it is possible to launch sequence of tasks
* `list_services` displays usage and capacity of the different services
* add priority field to prioritise tasks, and make queuing system more robust
* detect busy resources and put them in quarantine mode
* add '/status' service

### Fixes and improvements

* invalid docker image makes a run fail immediately
* check and retry if redis database is not available
* add default master storage (`"default_ms":true`)
* `docker.path` variable to set docker path on remote service
* for ssh service, if `options.server` is not set, set it to `auto`
* fix and improve task termination for ssh service
* incremental log during training
* automatically configure database at launch time
* `service/check` does not fail if resources not available
* misc bug fixes

## [v0.1.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.1.0) (2018-03-02)

Initial release.
