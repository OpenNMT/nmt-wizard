## [Unreleased]

## Fixes and improvements
* Fix client compatibility with Python 3

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
