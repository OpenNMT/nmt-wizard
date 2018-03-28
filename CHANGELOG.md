## [Unreleased]

### Breaking changes
* redefine more consistent routes

### New features

* add friendly name to tasks, and differentiate task type
* add task chaining feature - it is possible to launch sequence of tasks
* `list_services` displays usage and capacity of the different services
* add priority field to prioritise tasks, and make queuing system more robust

### Fixes and improvements

* add default master storage (`"default_ms":true`)
* `docker.path` variable to set docker path on remote service
* for ssh service, if `options.server` is not set, set it to `auto`
* fix and improve task termination for ssh service
* incremental log during training
* automatically configure database at launch time
* misc bug fixes

## [v0.1.0](https://github.com/OpenNMT/nmt-wizard/releases/tag/v0.1.0) (2018-03-02)

Initial release.
